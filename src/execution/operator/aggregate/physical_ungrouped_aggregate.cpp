#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"

namespace duckdb {

void DistinctAggregateData::Initialize(vector<unique_ptr<Expression>> &aggregates, const vector<idx_t> &indices) {
	idx_t distinct_aggregates = indices.size();
	radix_tables.resize(distinct_aggregates);
	grouped_aggregate_data.resize(distinct_aggregates);

	//! For every distinct aggregate, create a hashtable
	for (idx_t i = 0; i < indices.size(); i++) {
		auto aggr_idx = indices[i];
		auto &aggr = (BoundAggregateExpression &)*(aggregates[aggr_idx]);

		GroupingSet grouping_set;
		for (size_t set_idx = 0; set_idx < aggr.children.size(); set_idx++) {
			grouping_set.insert(set_idx);
		}
		grouped_aggregate_data[aggr_idx] = make_unique<GroupedAggregateData>();
		grouped_aggregate_data[aggr_idx]->SetDistinctGroupData(aggregates[aggr_idx]->Copy());
		radix_tables[aggr_idx] =
		    make_unique<RadixPartitionedHashTable>(grouping_set, *grouped_aggregate_data[aggr_idx]);
	}
}
bool DistinctAggregateData::AnyDistinct() const {
	return !radix_tables.empty();
}

PhysicalUngroupedAggregate::PhysicalUngroupedAggregate(vector<LogicalType> types,
                                                       vector<unique_ptr<Expression>> expressions,
                                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNGROUPED_AGGREGATE, move(types), estimated_cardinality),
      aggregates(move(expressions)) {

	vector<idx_t> distinct_aggregate_indices;
	//! Determine which (if any) aggregates are distinct
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = (BoundAggregateExpression &)*(aggregates[i]);
		if (!aggr.distinct) {
			continue;
		}
		distinct_aggregate_indices.push_back(i);
	}
	//! No distinct aggregations
	if (distinct_aggregate_indices.empty()) {
		return;
	}
	distinct_aggregate_data.Initialize(aggregates, distinct_aggregate_indices);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct AggregateState {
	explicit AggregateState(const vector<unique_ptr<Expression>> &aggregate_expressions) {
		for (auto &aggregate : aggregate_expressions) {
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			auto state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(state.get());
			aggregates.push_back(move(state));
			destructors.push_back(aggr.function.destructor);
#ifdef DEBUG
			counts.push_back(0);
#endif
		}
	}
	~AggregateState() {
		D_ASSERT(destructors.size() == aggregates.size());
		for (idx_t i = 0; i < destructors.size(); i++) {
			if (!destructors[i]) {
				continue;
			}
			Vector state_vector(Value::POINTER((uintptr_t)aggregates[i].get()));
			state_vector.SetVectorType(VectorType::FLAT_VECTOR);

			destructors[i](state_vector, 1);
		}
	}

	void Move(AggregateState &other) {
		other.aggregates = move(aggregates);
		other.destructors = move(destructors);
	}

	//! The aggregate values
	vector<unique_ptr<data_t[]>> aggregates;
	//! The destructors
	vector<aggregate_destructor_t> destructors;
	//! Counts (used for verification)
	vector<idx_t> counts;
};

class SimpleAggregateGlobalState : public GlobalSinkState {
public:
	SimpleAggregateGlobalState(const vector<unique_ptr<Expression>> &aggregates,
	                           const DistinctAggregateData &distinct_data, ClientContext &client)
	    : state(aggregates), finished(false) {
		if (!distinct_data.AnyDistinct()) {
			return;
		}
		radix_states.resize(distinct_data.radix_tables.size());
		for (idx_t i = 0; i < distinct_data.radix_tables.size(); i++) {
			if (!distinct_data.radix_tables[i]) {
				// This aggregate is not distinct
				continue;
			}
			auto &radix_table = *distinct_data.radix_tables[i];
			radix_states[i] = radix_table.GetGlobalSinkState(client);
		}
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate state
	AggregateState state;
	//! Whether or not the aggregate is finished
	bool finished;
	//! The global sink states of the hash tables
	vector<unique_ptr<GlobalSinkState>> radix_states;
};

class SimpleAggregateLocalState : public LocalSinkState {
public:
	SimpleAggregateLocalState(Allocator &allocator, const vector<unique_ptr<Expression>> &aggregates,
	                          const vector<LogicalType> &child_types,
	                          const DistinctAggregateData &distinct_aggregate_data, ExecutionContext &context)
	    : state(aggregates), child_executor(allocator) {

		InitializeDistinctAggregates(distinct_aggregate_data, context);
		vector<LogicalType> payload_types;
		vector<AggregateObject> aggregate_objects;
		for (auto &aggregate : aggregates) {
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			// initialize the payload chunk
			if (!aggr.children.empty()) {
				for (auto &child : aggr.children) {
					payload_types.push_back(child->return_type);
					child_executor.AddExpression(*child);
				}
			}
			aggregate_objects.emplace_back(&aggr);
		}
		if (!payload_types.empty()) { // for select count(*) from t; there is no payload at all
			payload_chunk.Initialize(allocator, payload_types);
		}
		filter_set.Initialize(allocator, aggregate_objects, child_types);
	}

	//! The local aggregate state
	AggregateState state;
	//! The executor
	ExpressionExecutor child_executor;
	//! The payload chunk
	DataChunk payload_chunk;
	//! Aggregate filter data set
	AggregateFilterDataSet filter_set;
	//! The global sink states of the distinct aggregates hash tables
	vector<unique_ptr<LocalSinkState>> radix_states;
	//! The input chunks for each distinct aggregate
	vector<unique_ptr<DataChunk>> distinct_input_chunks;

public:
	void Reset() {
		payload_chunk.Reset();
	}
	void InitializeDistinctAggregates(const DistinctAggregateData &data, ExecutionContext &context) {
		if (!data.AnyDistinct()) {
			// No distinct aggregates
			return;
		}
		radix_states.resize(data.radix_tables.size());
		distinct_input_chunks.resize(data.radix_tables.size());
		for (idx_t i = 0; i < data.radix_tables.size(); i++) {
			if (!data.radix_tables[i]) {
				// Aggregate at this index is not distinct
				continue;
			}
			auto &radix_table = *data.radix_tables[i];
			radix_states[i] = radix_table.GetLocalSinkState(context);
			auto &payload_types = data.grouped_aggregate_data[i]->payload_types;
			if (!payload_types.empty()) {
				distinct_input_chunks[i] = make_unique<DataChunk>();
				distinct_input_chunks[i]->InitializeEmpty(payload_types);
			}
		}
	}
};

unique_ptr<GlobalSinkState> PhysicalUngroupedAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<SimpleAggregateGlobalState>(aggregates, distinct_aggregate_data, context);
}

unique_ptr<LocalSinkState> PhysicalUngroupedAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<SimpleAggregateLocalState>(Allocator::Get(context.client), aggregates, children[0]->GetTypes(),
	                                              distinct_aggregate_data, context);
}

SinkResultType PhysicalUngroupedAggregate::Sink(ExecutionContext &context, GlobalSinkState &state,
                                                LocalSinkState &lstate, DataChunk &input) const {
	auto &sink = (SimpleAggregateLocalState &)lstate;
	// perform the aggregation inside the local state
	idx_t payload_idx = 0, payload_expr_idx = 0;
	sink.Reset();

	DataChunk &payload_chunk = sink.payload_chunk;

	idx_t next_payload_idx = 0;
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];

		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		if (sink.radix_states[aggr_idx]) {
			//! aggregate is distinct, can't be calculated yet
			D_ASSERT(this->distinct_aggregate_data.radix_tables[aggr_idx]);
			auto &global_sink = (SimpleAggregateGlobalState &)state;
			auto &radix_table = *distinct_aggregate_data.radix_tables[aggr_idx];
			auto &radix_global_sink = *global_sink.radix_states[aggr_idx];
			auto &radix_local_sink = *sink.radix_states[aggr_idx];
			auto &input_chunk = *sink.distinct_input_chunks[aggr_idx];

			radix_table.Sink(context, radix_global_sink, radix_local_sink, input, input_chunk);
			continue;
		}

		idx_t payload_cnt = 0;
		// resolve the filter (if any)
		if (aggregate.filter) {
			auto &filtered_data = sink.filter_set.GetFilterData(aggr_idx);
			auto count = filtered_data.ApplyFilter(input);

			sink.child_executor.SetChunk(filtered_data.filtered_payload);
			payload_chunk.SetCardinality(count);
		} else {
			sink.child_executor.SetChunk(input);
			payload_chunk.SetCardinality(input);
		}

#ifdef DEBUG
		sink.state.counts[aggr_idx] += payload_chunk.size();
#endif

		// resolve the child expressions of the aggregate (if any)
		if (!aggregate.children.empty()) {
			for (idx_t i = 0; i < aggregate.children.size(); ++i) {
				sink.child_executor.ExecuteExpression(payload_expr_idx, payload_chunk.data[payload_idx + payload_cnt]);
				payload_expr_idx++;
				payload_cnt++;
			}
		}

		AggregateInputData aggr_input_data(aggregate.bind_info.get());
		aggregate.function.simple_update(payload_cnt == 0 ? nullptr : &payload_chunk.data[payload_idx], aggr_input_data,
		                                 payload_cnt, sink.state.aggregates[aggr_idx].get(), payload_chunk.size());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void PhysicalUngroupedAggregate::Combine(ExecutionContext &context, GlobalSinkState &state,
                                         LocalSinkState &lstate) const {
	auto &gstate = (SimpleAggregateGlobalState &)state;
	auto &source = (SimpleAggregateLocalState &)lstate;
	D_ASSERT(!gstate.finished);

	// finalize: combine the local state into the global state
	// all aggregates are combinable: we might be doing a parallel aggregate
	// use the combine method to combine the partial aggregates
	lock_guard<mutex> glock(gstate.lock);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];

		if (source.radix_states[aggr_idx]) {
			//! aggregate is distinct, can't be calculated yet
			D_ASSERT(this->distinct_aggregate_data.radix_tables[aggr_idx]);
			auto &global_sink = (SimpleAggregateGlobalState &)state;
			auto &radix_table = *distinct_aggregate_data.radix_tables[aggr_idx];
			auto &radix_global_sink = *global_sink.radix_states[aggr_idx];
			auto &radix_local_sink = *source.radix_states[aggr_idx];

			radix_table.Combine(context, radix_global_sink, radix_local_sink);
			continue;
		}

		Vector source_state(Value::POINTER((uintptr_t)source.state.aggregates[aggr_idx].get()));
		Vector dest_state(Value::POINTER((uintptr_t)gstate.state.aggregates[aggr_idx].get()));

		AggregateInputData aggr_input_data(aggregate.bind_info.get());
		aggregate.function.combine(source_state, dest_state, aggr_input_data, 1);
#ifdef DEBUG
		gstate.state.counts[aggr_idx] += source.state.counts[aggr_idx];
#endif
	}

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &source.child_executor, "child_executor", 0);
	client_profiler.Flush(context.thread.profiler);
}

SinkFinalizeType PhysicalUngroupedAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      GlobalSinkState &gstate_p) const {
	auto &gstate = (SimpleAggregateGlobalState &)gstate_p;

	if (distinct_aggregate_data.AnyDistinct()) {
		for (idx_t i = 0; i < distinct_aggregate_data.radix_tables.size(); i++) {
			auto &radix_table_p = distinct_aggregate_data.radix_tables[i];
			if (!radix_table_p) {
				continue;
			}
			auto &radix_state = *gstate.radix_states[i];
			radix_table_p->Finalize(context, radix_state);
		}
	}
	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class SimpleAggregateState : public GlobalSourceState {
public:
	SimpleAggregateState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalUngroupedAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<SimpleAggregateState>();
}

void VerifyNullHandling(DataChunk &chunk, AggregateState &state, const vector<unique_ptr<Expression>> &aggregates) {
#ifdef DEBUG
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = (BoundAggregateExpression &)*aggregates[aggr_idx];
		if (state.counts[aggr_idx] == 0 && aggr.function.null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
			// Default is when 0 values go in, NULL comes out
			UnifiedVectorFormat vdata;
			chunk.data[aggr_idx].ToUnifiedFormat(1, vdata);
			D_ASSERT(!vdata.validity.RowIsValid(vdata.sel->get_index(0)));
		}
	}
#endif
}

void PhysicalUngroupedAggregate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                         LocalSourceState &lstate) const {
	auto &gstate = (SimpleAggregateGlobalState &)*sink_state;
	auto &state = (SimpleAggregateState &)gstate_p;
	D_ASSERT(gstate.finished);
	if (state.finished) {
		return;
	}

	// initialize the result chunk with the aggregate values
	chunk.SetCardinality(1);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];

		Vector state_vector(Value::POINTER((uintptr_t)gstate.state.aggregates[aggr_idx].get()));
		AggregateInputData aggr_input_data(aggregate.bind_info.get());
		aggregate.function.finalize(state_vector, aggr_input_data, chunk.data[aggr_idx], 1, 0);
	}
	VerifyNullHandling(chunk, gstate.state, aggregates);
	state.finished = true;
}

string PhysicalUngroupedAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (i > 0) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb
