#include "duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

PhysicalSimpleAggregate::PhysicalSimpleAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions)
    : PhysicalSink(PhysicalOperatorType::SIMPLE_AGGREGATE, move(types)), aggregates(move(expressions)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct AggregateState {
	AggregateState(vector<unique_ptr<Expression>> &aggregate_expressions) {
		for(auto &aggregate : aggregate_expressions) {
			assert(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			auto state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(state.get());
			aggregates.push_back(move(state));
			destructors.push_back(aggr.function.destructor);
		}
	}
	~AggregateState() {
		assert(destructors.size() == aggregates.size());
		for (idx_t i = 0; i < destructors.size(); i++) {
			if (!destructors[i]) {
				continue;
			}
			Vector state_vector(Value::POINTER((uintptr_t)aggregates[i].get()));
			state_vector.vector_type = VectorType::FLAT_VECTOR;

			destructors[i](state_vector, 1);
		}
	}
	void Clear() {
		aggregates.clear();
		destructors.clear();
	}

	//! The aggregate values
	vector<unique_ptr<data_t[]>> aggregates;
	// The destructors
	vector<aggregate_destructor_t> destructors;
};

class SimpleAggregateGlobalState : public GlobalOperatorState {
public:
	SimpleAggregateGlobalState(vector<unique_ptr<Expression>> &aggregates) :
		state(aggregates) { }

	//! The lock for updating the global aggregate state
	std::mutex lock;
	//! The global aggregate state
	AggregateState state;
};

class SimpleAggregateLocalState : public LocalSinkState {
public:
	SimpleAggregateLocalState(vector<unique_ptr<Expression>> &aggregates) :
		state(aggregates) {
		vector<TypeId> payload_types;
		for (auto &aggregate : aggregates) {
			assert(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			// initialize the payload chunk
			if (aggr.children.size()) {
				for (idx_t i = 0; i < aggr.children.size(); ++i) {
					payload_types.push_back(aggr.children[i]->return_type);
					child_executor.AddExpression(*aggr.children[i]);
				}
			} else {
				// COUNT(*)
				payload_types.push_back(TypeId::INT64);
			}
		}
		payload_chunk.Initialize(payload_types);
	}

	//! The local aggregate state
	AggregateState state;
	//! The executor
	ExpressionExecutor child_executor;
	//! The payload chunk
	DataChunk payload_chunk;
};


unique_ptr<GlobalOperatorState> PhysicalSimpleAggregate::GetGlobalState(ClientContext &context) {
	return make_unique<SimpleAggregateGlobalState>(aggregates);
}

unique_ptr<LocalSinkState> PhysicalSimpleAggregate::GetLocalSinkState(ClientContext &context) {
	return make_unique<SimpleAggregateLocalState>(aggregates);
}

void PhysicalSimpleAggregate::Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) {
	auto &sink = (SimpleAggregateLocalState &) lstate;
	// perform the aggregation inside the local state
	idx_t payload_idx = 0, payload_expr_idx = 0;
	DataChunk &payload_chunk = sink.payload_chunk;
	payload_chunk.Reset();
	sink.child_executor.SetChunk(input);
	payload_chunk.SetCardinality(input);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];
		idx_t payload_cnt = 0;
		// resolve the child expression of the aggregate (if any)
		if (aggregate.children.size() > 0) {
			for (idx_t i = 0; i < aggregate.children.size(); ++i) {
				sink.child_executor.ExecuteExpression(payload_expr_idx,
														payload_chunk.data[payload_idx + payload_cnt]);
				payload_expr_idx++;
				payload_cnt++;
			}
		} else {
			payload_cnt++;
		}
		// perform the actual aggregation
		aggregate.function.simple_update(&payload_chunk.data[payload_idx], payload_cnt,
											sink.state.aggregates[aggr_idx].get(), payload_chunk.size());
		payload_idx += payload_cnt;
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void PhysicalSimpleAggregate::Combine(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate) {
	auto &gstate = (SimpleAggregateGlobalState &) state;
	auto &source = (SimpleAggregateLocalState &) lstate;

	// finalize: combine the local state into the global state
	lock_guard<mutex> glock(gstate.lock);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];
		Vector source_state(Value::POINTER((uintptr_t)source.state.aggregates[aggr_idx].get()));
		Vector dest_state(Value::POINTER((uintptr_t)gstate.state.aggregates[aggr_idx].get()));

		aggregate.function.combine(source_state, dest_state, 1);
	}
	source.state.Clear();
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
void PhysicalSimpleAggregate::GetChunkInternal(ClientContext &context, DataChunk &chunk,
                                               PhysicalOperatorState *state) {
	auto &gstate = (SimpleAggregateGlobalState&) *sink_state;
	if (state->finished) {
		return;
	}
	// initialize the result chunk with the aggregate values
	chunk.SetCardinality(1);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];

		Vector state_vector(Value::POINTER((uintptr_t)gstate.state.aggregates[aggr_idx].get()));
		aggregate.function.finalize(state_vector, chunk.data[aggr_idx], 1);
	}
	state->finished = true;
}
