#include "execution/operator/aggregate/physical_simple_aggregate.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "function/aggregate_function/distributive.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalSimpleAggregate::PhysicalSimpleAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions) :
	 PhysicalOperator(PhysicalOperatorType::SIMPLE_AGGREGATE, types), aggregates(move(expressions)) {
}

void PhysicalSimpleAggregate::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSimpleAggregateOperatorState *>(state_);
	while(true) {
		// iterate over the child
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			break;
		}
		ExpressionExecutor executor(state->child_chunk);
		// now resolve the aggregates for each of the children
		state->payload_chunk.Reset();
		for(index_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
			auto &aggregate = (BoundAggregateExpression&) *aggregates[aggr_idx];
			auto &payload_vector = state->payload_chunk.data[aggr_idx];
			// resolve the child expression of the aggregate (if any)
			if (aggregate.child) {
				executor.ExecuteExpression(*aggregate.child, payload_vector);
			} else {
				payload_vector.count = state->child_chunk.size();
			}
			// perform the actual aggregation
			switch (aggregate.type) {
			case ExpressionType::AGGREGATE_COUNT_STAR:
				countstar_simple_update(&payload_vector, 1, state->aggregates[aggr_idx]);
				break;
			case ExpressionType::AGGREGATE_COUNT:
				count_simple_update(&payload_vector, 1, state->aggregates[aggr_idx]);
				break;
			case ExpressionType::AGGREGATE_SUM:
				sum_simple_update(&payload_vector, 1, state->aggregates[aggr_idx]);
				break;
			case ExpressionType::AGGREGATE_MIN:
				min_simple_update(&payload_vector, 1, state->aggregates[aggr_idx]);
				break;
			case ExpressionType::AGGREGATE_MAX:
				max_simple_update(&payload_vector, 1, state->aggregates[aggr_idx]);
				break;
			default:
				throw Exception("Unsupported aggregate for simple aggregation");
			}
		}
	}
	// initialize the result chunk with the aggregate values
	for(index_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		chunk.data[aggr_idx].count = 1;
		chunk.data[aggr_idx].SetValue(0, state->aggregates[aggr_idx]);
	}
	state->finished = true;
}

unique_ptr<PhysicalOperatorState> PhysicalSimpleAggregate::GetOperatorState() {
	return make_unique<PhysicalSimpleAggregateOperatorState>(this, children[0].get());
}

PhysicalSimpleAggregateOperatorState::PhysicalSimpleAggregateOperatorState(PhysicalSimpleAggregate *parent, PhysicalOperator *child)
	: PhysicalOperatorState(child) {
	vector<TypeId> payload_types;
	for (auto &aggregate : parent->aggregates) {
		assert(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		// initialize the payload chunk
		if (aggr.child) {
			payload_types.push_back(aggr.child->return_type);
		} else {
			// COUNT(*)
			payload_types.push_back(TypeId::BIGINT);
		}
		// initialize the aggregate values
		switch (aggregate->type) {
		case ExpressionType::AGGREGATE_COUNT_STAR:
		case ExpressionType::AGGREGATE_COUNT:
		case ExpressionType::AGGREGATE_COUNT_DISTINCT:
			aggregates.push_back(bigint_simple_initialize());
			break;
		default:
			aggregates.push_back(null_simple_initialize());
			break;
		}
	}
	payload_chunk.Initialize(payload_types);
}
