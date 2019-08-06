#include "execution/operator/aggregate/physical_simple_aggregate.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "function/aggregate_function/distributive.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

PhysicalSimpleAggregate::PhysicalSimpleAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions) :
	 PhysicalOperator(PhysicalOperatorType::SIMPLE_AGGREGATE, types), aggregates(move(expressions)) {
}

void PhysicalSimpleAggregate::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalSimpleAggregateOperatorState *>(state_);
	vector<Vector*> inputs(1, nullptr); // There is always one input
	for (const auto& expr : aggregates) {
		auto &aggregate = (BoundAggregateExpression&) *expr;
		if (aggregate.children.size() > inputs.size()) {
			inputs.resize(aggregate.children.size(), nullptr);
		}
	}
	while(true) {
		// iterate over the child
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			break;
		}
		ExpressionExecutor executor(state->child_chunk);
		// now resolve the aggregates for each of the children
		index_t payload_idx = 0;
		DataChunk &payload_chunk = state->payload_chunk;
		payload_chunk.Reset();
		for(index_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
			auto &aggregate = (BoundAggregateExpression&) *aggregates[aggr_idx];
			index_t payload_cnt = 0;
			// resolve the child expression of the aggregate (if any)
			if (aggregate.children.size()) {
				for (index_t i = 0; i < aggregate.children.size(); ++i) {
					auto input = &payload_chunk.data[payload_idx+payload_cnt];
					executor.ExecuteExpression(*aggregate.children[i], *input);
					inputs[payload_cnt++] = input;
				}
			} else {
				auto input = &payload_chunk.data[payload_idx+payload_cnt];
				input->count = state->child_chunk.size();
				inputs[payload_cnt++] = input;
			}
			// perform the actual aggregation
			if (aggregate.bound_aggregate->simple_update) {
				aggregate.bound_aggregate->simple_update(inputs.data(), payload_cnt, state->aggregates[aggr_idx]);
			}
			else {
				throw Exception("Unsupported aggregate for simple aggregation");
			}
			payload_idx += payload_cnt;
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
		if (aggr.children.size()) {
			for (index_t i = 0; i < aggr.children.size(); ++i) {
				payload_types.push_back(aggr.children[i]->return_type);
			}
		} else {
			// COUNT(*)
			payload_types.push_back(TypeId::BIGINT);
		}
		// initialize the aggregate values
		aggregates.push_back(aggr.bound_aggregate->simple_initialize());
	}
	payload_chunk.Initialize(payload_types);
}
