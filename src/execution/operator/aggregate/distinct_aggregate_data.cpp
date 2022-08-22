#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

DistinctAggregateData::DistinctAggregateData(Allocator &allocator, const vector<unique_ptr<Expression>> &aggregates,
                                             vector<idx_t> indices, ClientContext &client)
    : child_executor(allocator), payload_chunk(), indices(move(indices)) {
	const idx_t aggregate_count = aggregates.size();

	idx_t table_count = CreateTableIndexMap(aggregates);

	grouped_aggregate_data.resize(table_count);
	radix_tables.resize(table_count);
	radix_states.resize(table_count);
	grouping_sets.resize(table_count);
	distinct_output_chunks.resize(table_count);

	vector<LogicalType> payload_types;
	for (idx_t i = 0; i < aggregate_count; i++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];

		// Initialize the child executor and get the payload types for every aggregate
		for (auto &child : aggregate.children) {
			payload_types.push_back(child->return_type);
			child_executor.AddExpression(*child);
		}
		if (!aggregate.distinct) {
			continue;
		}
		D_ASSERT(table_map.count(i));
		idx_t table_idx = table_map[i];
		if (radix_tables[table_idx] != nullptr) {
			//! Table is already initialized
			continue;
		}
		//! Populate the group with the children of the aggregate
		for (size_t set_idx = 0; set_idx < aggregate.children.size(); set_idx++) {
			grouping_sets[table_idx].insert(set_idx);
		}
		// Create the hashtable for the aggregate
		grouped_aggregate_data[table_idx] = make_unique<GroupedAggregateData>();
		grouped_aggregate_data[table_idx]->InitializeDistinct(aggregates[i]);
		radix_tables[table_idx] =
		    make_unique<RadixPartitionedHashTable>(grouping_sets[table_idx], *grouped_aggregate_data[table_idx]);

		auto &radix_table = *radix_tables[table_idx];
		radix_states[table_idx] = radix_table.GetGlobalSinkState(client);

		vector<LogicalType> chunk_types;
		for (auto &child_p : aggregate.children) {
			chunk_types.push_back(child_p->return_type);
		}

		// This is used in Finalize to get the data from the radix table
		distinct_output_chunks[table_idx] = make_unique<DataChunk>();
		distinct_output_chunks[table_idx]->Initialize(client, chunk_types);
	}
	if (!payload_types.empty()) {
		payload_chunk.Initialize(allocator, payload_types);
	}
}

using aggr_ref_t = std::reference_wrapper<BoundAggregateExpression>;

struct FindMatchingAggregate {
	explicit FindMatchingAggregate(const aggr_ref_t &aggr) : aggr_r(aggr) {
	}
	bool operator()(const aggr_ref_t other_r) {
		auto &other = other_r.get();
		auto &aggr = aggr_r.get();
		if (other.children.size() != aggr.children.size()) {
			return false;
		}
		if (!Expression::Equals(aggr.filter.get(), other.filter.get())) {
			return false;
		}
		for (idx_t i = 0; i < aggr.children.size(); i++) {
			auto &other_child = (BoundReferenceExpression &)*other.children[i];
			auto &aggr_child = (BoundReferenceExpression &)*aggr.children[i];
			if (other_child.index != aggr_child.index) {
				return false;
			}
		}
		return true;
	}
	const aggr_ref_t aggr_r;
};

idx_t DistinctAggregateData::CreateTableIndexMap(const vector<unique_ptr<Expression>> &aggregates) {
	vector<aggr_ref_t> table_inputs;

	D_ASSERT(table_map.empty());
	for (auto &agg_idx : indices) {
		D_ASSERT(agg_idx < aggregates.size());
		auto &aggregate = (BoundAggregateExpression &)*aggregates[agg_idx];

		auto matching_inputs =
		    std::find_if(table_inputs.begin(), table_inputs.end(), FindMatchingAggregate(std::ref(aggregate)));
		if (matching_inputs != table_inputs.end()) {
			//! Assign the existing table to the aggregate
			idx_t found_idx = std::distance(table_inputs.begin(), matching_inputs);
			table_map[agg_idx] = found_idx;
			continue;
		}
		//! Create a new table and assign its index to the aggregate
		table_map[agg_idx] = table_inputs.size();
		table_inputs.push_back(std::ref(aggregate));
	}
	//! Every distinct aggregate needs to be assigned an index
	D_ASSERT(table_map.size() == indices.size());
	//! There can not be more tables then there are distinct aggregates
	D_ASSERT(table_inputs.size() <= indices.size());

	return table_inputs.size();
}

bool DistinctAggregateData::AnyDistinct() const {
	return !radix_tables.empty();
}

const vector<idx_t> &DistinctAggregateData::Indices() const {
	return this->indices;
}

bool DistinctAggregateData::IsDistinct(idx_t index) const {
	bool is_distinct = !radix_tables.empty() && table_map.count(index);
#ifdef DEBUG
	//! Make sure that if it is distinct, it's also in the indices
	//! And if it's not distinct, that it's also not in the indices
	bool found = false;
	for (auto &idx : indices) {
		if (idx == index) {
			found = true;
			break;
		}
	}
	D_ASSERT(found == is_distinct);
#endif
	return is_distinct;
}

} // namespace duckdb
