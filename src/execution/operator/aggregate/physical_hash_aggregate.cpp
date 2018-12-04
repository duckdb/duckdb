#include "execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "parser/expression/aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalHashAggregate::PhysicalHashAggregate(LogicalOperator &op, vector<unique_ptr<Expression>> expressions)
    : PhysicalAggregate(op, move(expressions), PhysicalOperatorType::HASH_GROUP_BY) {
	Initialize();
}

PhysicalHashAggregate::PhysicalHashAggregate(LogicalOperator &op, vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups)
    : PhysicalAggregate(op, move(expressions), move(groups), PhysicalOperatorType::HASH_GROUP_BY) {
	Initialize();
}

void PhysicalHashAggregate::Initialize() {
}

void PhysicalHashAggregate::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashAggregateOperatorState *>(state_);
	do {
		if (children.size() > 0) {
			// resolve the child chunk if there is one
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				break;
			}
		}

		ExpressionExecutor executor(state, context);
		// aggregation with groups
		DataChunk &group_chunk = state->group_chunk;
		DataChunk &payload_chunk = state->payload_chunk;
		executor.Execute(groups, group_chunk);
		executor.Execute(payload_chunk,
		                 [&](size_t i) {
			                 if (!state->payload_expressions[i]) {
				                 state->payload_chunk.data[i].count = group_chunk.size();
				                 state->payload_chunk.data[i].sel_vector = group_chunk.sel_vector;
			                 }
			                 return state->payload_expressions[i];
		                 },
		                 state->payload_expressions.size());

		group_chunk.Verify();
		payload_chunk.Verify();
		assert(payload_chunk.column_count == 0 || group_chunk.size() == payload_chunk.size());

		// move the strings inside the groups to the string heap
		group_chunk.MoveStringsToHeap(state->ht->string_heap);
		payload_chunk.MoveStringsToHeap(state->ht->string_heap);

		state->ht->AddChunk(group_chunk, payload_chunk);
		state->tuples_scanned += state->child_chunk.size();
	} while (state->child_chunk.size() > 0);

	state->group_chunk.Reset();
	state->aggregate_chunk.Reset();
	size_t elements_found = state->ht->Scan(state->ht_scan_position, state->group_chunk, state->aggregate_chunk);

	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (elements_found == 0 && state->tuples_scanned == 0 && is_implicit_aggr) {
		assert(state->aggregate_chunk.column_count == aggregates.size());
		// for each column in the aggregates, seit either to NULL or 0
		for (size_t i = 0; i < state->aggregate_chunk.column_count; i++) {
			state->aggregate_chunk.data[i].count = 1;
			switch (aggregates[i]->type) {
			case ExpressionType::AGGREGATE_COUNT_STAR:
			case ExpressionType::AGGREGATE_COUNT:
			case ExpressionType::AGGREGATE_COUNT_DISTINCT:
				state->aggregate_chunk.data[i].SetValue(0, 0);
				break;
			default:
				state->aggregate_chunk.data[i].SetValue(0, Value());
				break;
			}
		}
		state->finished = true;
	}
	if (elements_found == 0 && !state->finished) {
		state->finished = true;
		return;
	}
	// we finished the child chunk
	// actually compute the final projection list now
	ExpressionExecutor executor(state, context, false);
	executor.Execute(select_list, chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState(ExpressionExecutor *parent) {
	auto state = make_unique<PhysicalHashAggregateOperatorState>(
	    this, children.size() == 0 ? nullptr : children[0].get(), parent);
	state->tuples_scanned = 0;
	vector<TypeId> group_types, payload_types;
	std::vector<ExpressionType> aggregate_kind;
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	for (auto &expr : aggregates) {
		aggregate_kind.push_back(expr->type);
		if (expr->children.size() > 0) {
			auto &child = expr->children[0];
			payload_types.push_back(child->return_type);
			state->payload_expressions.push_back(child.get());
		} else {
			// COUNT(*)
			payload_types.push_back(TypeId::BIGINT);
			state->payload_expressions.push_back(nullptr);
		}
	}
	state->payload_chunk.Initialize(payload_types);

	state->ht = make_unique<SuperLargeHashTable>(1024, group_types, payload_types, aggregate_kind);
	return move(state);
}
