#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

//! Shared information about a collection of distinct aggregates
DistinctAggregateCollectionInfo::DistinctAggregateCollectionInfo(const vector<unique_ptr<Expression>> &aggregates,
                                                                 vector<idx_t> indices)
    : indices(std::move(indices)), aggregates(aggregates) {
	table_count = CreateTableIndexMap();

	const idx_t aggregate_count = aggregates.size();

	total_child_count = 0;
	for (idx_t i = 0; i < aggregate_count; i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();

		if (!aggregate.IsDistinct()) {
			continue;
		}
		total_child_count += aggregate.GetChildren().size();
	}
}

//! Stateful data for the distinct aggregates

DistinctAggregateState::DistinctAggregateState(const DistinctAggregateData &data, ClientContext &client)
    : child_executor(client) {
	radix_states.resize(data.info.table_count);
	distinct_output_chunks.resize(data.info.table_count);

	idx_t aggregate_count = data.info.aggregates.size();
	for (idx_t i = 0; i < aggregate_count; i++) {
		auto &aggregate = data.info.aggregates[i]->Cast<BoundAggregateExpression>();

		// Initialize the child executor and get the payload types for every aggregate
		for (auto &child : aggregate.GetChildren()) {
			child_executor.AddExpression(*child);
		}
		if (!aggregate.IsDistinct()) {
			continue;
		}
		D_ASSERT(data.info.table_map.count(i));
		idx_t table_idx = data.info.table_map.at(i);
		if (data.radix_tables[table_idx] == nullptr) {
			//! This table is unused because the aggregate shares its data with another
			continue;
		}

		// Get the global sinkstate for the aggregate
		auto &radix_table = *data.radix_tables[table_idx];
		radix_states[table_idx] = radix_table.GetGlobalSinkState(client);

		// Fill the chunk_types (group_by + children)
		vector<LogicalType> chunk_types;
		for (auto &group_type : data.grouped_aggregate_data[table_idx]->group_types) {
			chunk_types.push_back(group_type);
		}
		for (auto &aggregate_type : data.grouped_aggregate_data[table_idx]->aggregate_return_types) {
			chunk_types.push_back(aggregate_type);
		}

		// This is used in Finalize to get the data from the radix table
		distinct_output_chunks[table_idx] = make_uniq<DataChunk>();
		distinct_output_chunks[table_idx]->Initialize(client, chunk_types);
	}
}

//! Persistent + shared (read-only) data for the distinct aggregates
DistinctAggregateData::DistinctAggregateData(ClientContext &context, const DistinctAggregateCollectionInfo &info,
                                             TupleDataValidityType distinct_validity)
    : DistinctAggregateData(context, info, {}, nullptr, distinct_validity) {
}

DistinctAggregateData::DistinctAggregateData(ClientContext &context, const DistinctAggregateCollectionInfo &info,
                                             const GroupingSet &groups,
                                             const vector<unique_ptr<Expression>> *group_expressions,
                                             TupleDataValidityType distinct_validity)
    : info(info) {
	grouped_aggregate_data.resize(info.table_count);
	radix_tables.resize(info.table_count);
	grouping_sets.resize(info.table_count);
	key_normalizers.resize(info.table_count);
	internal_aggregate_filters.resize(info.table_count);
	representative_input_indices.resize(info.table_count);

	for (auto &i : info.indices) {
		auto &aggregate = info.aggregates[i]->Cast<BoundAggregateExpression>();

		D_ASSERT(info.table_map.count(i));
		idx_t table_idx = info.table_map.at(i);
		if (radix_tables[table_idx] != nullptr) {
			//! This aggregate shares a table with another aggregate, and the table is already initialized
			continue;
		}

		auto &normalizers = key_normalizers[table_idx];
		auto &representative_indices = representative_input_indices[table_idx];
		vector<bool> requires_normalization;
		for (idx_t child_idx = 0; child_idx < aggregate.GetChildren().size(); child_idx++) {
			auto &child = aggregate.GetChildren()[child_idx];
			auto &child_ref = child->Cast<BoundReferenceExpression>();

			unique_ptr<Expression> normalizer =
			    make_uniq<BoundReferenceExpression>(child->GetReturnType(), child_ref.Index());
			const auto child_requires_normalization =
			    ExpressionBinder::PushCollation(context, normalizer, child->GetReturnType());
			requires_normalization.push_back(child_requires_normalization);
			if (child_requires_normalization) {
				representative_indices.push_back(child_ref.Index());
			}
			normalizers.push_back(std::move(normalizer));
		}
		// The grouping set contains the indices of the chunk that correspond to the data vector
		// that will be used to figure out in which bucket the payload should be put
		auto &grouping_set = grouping_sets[table_idx];
		//! Populate the group with the children of the aggregate
		for (auto &group : groups) {
			grouping_set.insert(group);
		}
		idx_t group_by_size = group_expressions ? group_expressions->size() : 0;
		for (idx_t set_idx = 0; set_idx < aggregate.GetChildren().size(); set_idx++) {
			grouping_set.insert(ProjectionIndex(set_idx + group_by_size));
		}
		// Create the hashtable for the aggregate
		grouped_aggregate_data[table_idx] = make_uniq<GroupedAggregateData>();
		grouped_aggregate_data[table_idx]->InitializeDistinct(context, info.aggregates[i], group_expressions,
		                                                      normalizers, requires_normalization);
		D_ASSERT(representative_indices.size() == grouped_aggregate_data[table_idx]->payload_types.size());
		auto &internal_filter = internal_aggregate_filters[table_idx];
		internal_filter.reserve(grouped_aggregate_data[table_idx]->aggregates.size());
		for (idx_t internal_idx = 0; internal_idx < grouped_aggregate_data[table_idx]->aggregates.size();
		     internal_idx++) {
			internal_filter.push_back(internal_idx);
		}
		if (representative_indices.empty()) {
			normalizers.clear();
		}
		radix_tables[table_idx] =
		    make_uniq<RadixPartitionedHashTable>(grouping_set, *grouped_aggregate_data[table_idx], distinct_validity);
	}
}

bool DistinctAggregateData::RequiresNormalization(idx_t table_idx) const {
	return !key_normalizers[table_idx].empty();
}

bool DistinctAggregateData::AnyRequiresNormalization() const {
	for (auto &normalizers : key_normalizers) {
		if (!normalizers.empty()) {
			return true;
		}
	}
	return false;
}

DistinctAggregateLocalState::DistinctAggregateLocalState(const DistinctAggregateData &data, ClientContext &client) {
	const auto table_count = data.info.table_count;
	input_executors.resize(table_count);
	input_chunks.resize(table_count);
	payload_chunks.resize(table_count);

	for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
		if (!data.radix_tables[table_idx] || !data.RequiresNormalization(table_idx)) {
			continue;
		}

		auto &table_data = *data.grouped_aggregate_data[table_idx];
		D_ASSERT(table_data.GroupCount() >= data.key_normalizers[table_idx].size());
		const auto sql_group_count = table_data.GroupCount() - data.key_normalizers[table_idx].size();
		auto executor = make_uniq<ExpressionExecutor>(client);
		for (idx_t group_idx = 0; group_idx < sql_group_count; group_idx++) {
			executor->AddExpression(*table_data.groups[group_idx]);
		}
		for (auto &normalizer : data.key_normalizers[table_idx]) {
			executor->AddExpression(*normalizer);
		}
		input_executors[table_idx] = std::move(executor);

		input_chunks[table_idx] = make_uniq<DataChunk>();
		input_chunks[table_idx]->Initialize(client, table_data.group_types);

		payload_chunks[table_idx] = make_uniq<DataChunk>();
		payload_chunks[table_idx]->Initialize(client, table_data.payload_types);
	}
}

void DistinctAggregateLocalState::PrepareData(const DistinctAggregateData &data, idx_t table_idx, DataChunk &input) {
	D_ASSERT(data.RequiresNormalization(table_idx));
	auto &distinct_input = *input_chunks[table_idx];
	auto &payload = *payload_chunks[table_idx];

	distinct_input.Reset();
	payload.Reset();

	auto &representative_indices = data.representative_input_indices[table_idx];
	D_ASSERT(representative_indices.size() == payload.ColumnCount());
	for (idx_t payload_idx = 0; payload_idx < representative_indices.size(); payload_idx++) {
		payload.data[payload_idx].Reference(input.data[representative_indices[payload_idx]]);
	}
	payload.SetChildCardinality(input.size());
	input_executors[table_idx]->Execute(input, distinct_input);
}

using aggr_ref_t = reference<BoundAggregateExpression>;

struct FindMatchingAggregate {
	explicit FindMatchingAggregate(const aggr_ref_t &aggr) : aggr_r(aggr) {
	}
	bool operator()(const aggr_ref_t other_r) {
		auto &other = other_r.get();
		auto &aggr = aggr_r.get();
		if (other.GetChildren().size() != aggr.GetChildren().size()) {
			return false;
		}
		if (!Expression::Equals(aggr.GetFilterMutable(), other.GetFilterMutable())) {
			return false;
		}
		for (idx_t i = 0; i < aggr.GetChildren().size(); i++) {
			auto &other_child = other.GetChildren()[i]->Cast<BoundReferenceExpression>();
			auto &aggr_child = aggr.GetChildren()[i]->Cast<BoundReferenceExpression>();
			if (other_child.Index() != aggr_child.Index()) {
				return false;
			}
			if (StringType::GetCollation(other_child.GetReturnType()) !=
			    StringType::GetCollation(aggr_child.GetReturnType())) {
				return false;
			}
		}
		return true;
	}
	const aggr_ref_t aggr_r;
};

idx_t DistinctAggregateCollectionInfo::CreateTableIndexMap() {
	vector<aggr_ref_t> table_inputs;

	D_ASSERT(table_map.empty());
	for (auto &agg_idx : indices) {
		D_ASSERT(agg_idx < aggregates.size());
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		auto matching_inputs =
		    std::find_if(table_inputs.begin(), table_inputs.end(), FindMatchingAggregate(std::ref(aggregate)));
		if (matching_inputs != table_inputs.end()) {
			//! Assign the existing table to the aggregate
			auto found_idx = NumericCast<idx_t>(std::distance(table_inputs.begin(), matching_inputs));
			table_map[agg_idx] = found_idx;
			continue;
		}
		//! Create a new table and assign its index to the aggregate
		table_map[agg_idx] = table_inputs.size();
		table_inputs.push_back(std::ref(aggregate));
	}
	//! Every distinct aggregate needs to be assigned an index
	D_ASSERT(table_map.size() == indices.size());
	//! There can not be more tables than there are distinct aggregates
	D_ASSERT(table_inputs.size() <= indices.size());

	return table_inputs.size();
}

bool DistinctAggregateCollectionInfo::AnyDistinct() const {
	return !indices.empty();
}

const unsafe_vector<idx_t> &DistinctAggregateCollectionInfo::Indices() const {
	return this->indices;
}

static vector<idx_t> GetDistinctIndices(vector<unique_ptr<Expression>> &aggregates) {
	vector<idx_t> distinct_indices;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.IsDistinct()) {
			distinct_indices.push_back(i);
		}
	}
	return distinct_indices;
}

unique_ptr<DistinctAggregateCollectionInfo>
DistinctAggregateCollectionInfo::Create(vector<unique_ptr<Expression>> &aggregates) {
	vector<idx_t> indices = GetDistinctIndices(aggregates);
	if (indices.empty()) {
		return nullptr;
	}
	return make_uniq<DistinctAggregateCollectionInfo>(aggregates, std::move(indices));
}

bool DistinctAggregateData::IsDistinct(idx_t index) const {
	bool is_distinct = !radix_tables.empty() && info.table_map.count(index);
#ifdef DEBUG
	//! Make sure that if it is distinct, it's also in the indices
	//! And if it's not distinct, that it's also not in the indices
	bool found = false;
	for (auto &idx : info.indices) {
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
