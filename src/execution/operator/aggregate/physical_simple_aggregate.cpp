#include "duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

class PhysicalSimpleAggregateOperatorState : public PhysicalOperatorState {
public:
	PhysicalSimpleAggregateOperatorState(PhysicalSimpleAggregate *parent, PhysicalOperator *child);

	//! The aggregate values
	vector<unique_ptr<data_t[]>> aggregates;

	ExpressionExecutor child_executor;
	//! The payload chunk
	DataChunk payload_chunk;
};

PhysicalSimpleAggregate::PhysicalSimpleAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions)
    : PhysicalOperator(PhysicalOperatorType::SIMPLE_AGGREGATE, types), aggregates(move(expressions)) {
}

void PhysicalSimpleAggregate::GetChunkInternal(ClientContext &context, DataChunk &chunk,
                                               PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSimpleAggregateOperatorState *>(state_);
	while (true) {
		// iterate over the child
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			break;
		}

		// now resolve the aggregates for each of the children
		index_t payload_idx = 0, payload_expr_idx = 0;
		DataChunk &payload_chunk = state->payload_chunk;
		payload_chunk.Reset();
		state->child_executor.SetChunk(state->child_chunk);
		for (index_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
			auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];
			index_t payload_cnt = 0;
			// resolve the child expression of the aggregate (if any)
			if (aggregate.children.size() > 0) {
				for (index_t i = 0; i < aggregate.children.size(); ++i) {
					state->child_executor.ExecuteExpression(payload_expr_idx,
					                                        payload_chunk.data[payload_idx + payload_cnt]);
					payload_expr_idx++;
					payload_cnt++;
				}
			} else {
				payload_chunk.data[payload_idx + payload_cnt].count = state->child_chunk.size();
				payload_cnt++;
			}
			// perform the actual aggregation
			aggregate.function.simple_update(&payload_chunk.data[payload_idx], payload_cnt,
			                                 state->aggregates[aggr_idx].get());
			payload_idx += payload_cnt;
		}
	}
	// initialize the result chunk with the aggregate values
	for (index_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[aggr_idx];

		Vector state_vector(Value::POINTER((uintptr_t)state->aggregates[aggr_idx].get()));
		aggregate.function.finalize(state_vector, chunk.data[aggr_idx]);
	}
	state->finished = true;
}

unique_ptr<PhysicalOperatorState> PhysicalSimpleAggregate::GetOperatorState() {
	return make_unique<PhysicalSimpleAggregateOperatorState>(this, children[0].get());
}

PhysicalSimpleAggregateOperatorState::PhysicalSimpleAggregateOperatorState(PhysicalSimpleAggregate *parent,
                                                                           PhysicalOperator *child)
    : PhysicalOperatorState(child) {
	vector<TypeId> payload_types;
	for (auto &aggregate : parent->aggregates) {
		assert(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		// initialize the payload chunk
		if (aggr.children.size()) {
			for (index_t i = 0; i < aggr.children.size(); ++i) {
				payload_types.push_back(aggr.children[i]->return_type);
				child_executor.AddExpression(*aggr.children[i]);
			}
		} else {
			// COUNT(*)
			payload_types.push_back(TypeId::INT64);
		}
		// initialize the aggregate values
		auto state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size(aggr.return_type)]);
		aggr.function.initialize(state.get(), aggr.return_type);
		aggregates.push_back(move(state));
	}
	payload_chunk.Initialize(payload_types);
}
