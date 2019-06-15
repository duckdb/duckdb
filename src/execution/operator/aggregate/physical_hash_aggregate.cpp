#include "execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalHashAggregate::PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                             PhysicalOperatorType type)
    : PhysicalAggregate(types, move(expressions), type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups, PhysicalOperatorType type)
    : PhysicalAggregate(types, move(expressions), move(groups), type) {
}

void PhysicalHashAggregate::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashAggregateOperatorState *>(state_);
	do {
		if (children.size() > 0) {
			// resolve the child chunk if there is one
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				break;
			}
		}

		ExpressionExecutor executor(state->child_chunk);
		// aggregation with groups
		DataChunk &group_chunk = state->group_chunk;
		DataChunk &payload_chunk = state->payload_chunk;
		executor.Execute(groups, group_chunk);
		for (index_t i = 0; i < aggregates.size(); i++) {
			auto &aggr = (BoundAggregateExpression &)*aggregates[i];
			if (aggr.child) {
				executor.ExecuteExpression(*aggr.child, payload_chunk.data[i]);
				payload_chunk.heap.MergeHeap(payload_chunk.data[i].string_heap);
			} else {
				payload_chunk.data[i].count = group_chunk.size();
				payload_chunk.data[i].sel_vector = group_chunk.sel_vector;
			}
		}
		payload_chunk.sel_vector = group_chunk.sel_vector;

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
	index_t elements_found = state->ht->Scan(state->ht_scan_position, state->group_chunk, state->aggregate_chunk);

	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (elements_found == 0 && state->tuples_scanned == 0 && is_implicit_aggr) {
		assert(state->aggregate_chunk.column_count == aggregates.size());
		// for each column in the aggregates, seit either to NULL or 0
		for (index_t i = 0; i < state->aggregate_chunk.column_count; i++) {
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
	index_t chunk_index = 0;
	if (state->group_chunk.column_count + state->aggregate_chunk.column_count == chunk.column_count) {
		for (index_t col_idx = 0; col_idx < state->group_chunk.column_count; col_idx++) {
			chunk.data[chunk_index++].Reference(state->group_chunk.data[col_idx]);
		}
	} else {
		assert(state->aggregate_chunk.column_count == chunk.column_count);
	}

	for (index_t col_idx = 0; col_idx < state->aggregate_chunk.column_count; col_idx++) {
		chunk.data[chunk_index++].Reference(state->aggregate_chunk.data[col_idx]);
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	auto state =
	    make_unique<PhysicalHashAggregateOperatorState>(this, children.size() == 0 ? nullptr : children[0].get());
	state->tuples_scanned = 0;
	vector<TypeId> group_types, payload_types;
	vector<ExpressionType> aggregate_kind;
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	for (auto &expr : aggregates) {
		assert(expr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = (BoundAggregateExpression &)*expr;
		aggregate_kind.push_back(expr->type);
		if (aggr.child) {
			payload_types.push_back(aggr.child->return_type);
		} else {
			// COUNT(*)
			payload_types.push_back(TypeId::BIGINT);
		}
	}
	if (payload_types.size() > 0) {
		state->payload_chunk.Initialize(payload_types);
	}

	state->ht = make_unique<SuperLargeHashTable>(1024, group_types, payload_types, aggregate_kind);
	return move(state);
}
