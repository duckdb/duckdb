#include "execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

PhysicalHashAggregate::PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(types, move(expressions), {}, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups, PhysicalOperatorType type)
    : PhysicalOperator(type, types), groups(move(groups)) {
	// get a list of all aggregates to be computed
	// fake a single group with a constant value for aggregation without groups
	if (this->groups.size() == 0) {
		auto ce = make_unique<BoundConstantExpression>(Value::TINYINT(42));
		this->groups.push_back(move(ce));
		is_implicit_aggr = true;
	} else {
		is_implicit_aggr = false;
	}
	for (auto &expr : expressions) {
		assert(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		assert(expr->IsAggregate());
		aggregates.push_back(move(expr));
	}
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
		index_t payload_idx = 0;
		ExpressionExecutor executor(state->child_chunk);
		// aggregation with groups
		DataChunk &group_chunk = state->group_chunk;
		DataChunk &payload_chunk = state->payload_chunk;
		executor.Execute(groups, group_chunk);
		payload_chunk.Reset();
		for (index_t i = 0; i < aggregates.size(); i++) {
			auto &aggr = (BoundAggregateExpression &)*aggregates[i];
			if (aggr.children.size()) {
				for (index_t j = 0; j < aggr.children.size(); ++j) {
					executor.ExecuteExpression(*aggr.children[j], payload_chunk.data[payload_idx]);
					payload_chunk.heap.MergeHeap(payload_chunk.data[payload_idx].string_heap);
					++payload_idx;
				}
			} else {
				payload_chunk.data[payload_idx].count = group_chunk.size();
				payload_chunk.data[payload_idx].sel_vector = group_chunk.sel_vector;
				++payload_idx;
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
			assert(aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto aggr = (BoundAggregateExpression *)(&*aggregates[i]);
			state->aggregate_chunk.data[i].SetValue(
			    0, aggr->function.simple_initialize ? aggr->function.simple_initialize() : Value());
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
	vector<BoundAggregateExpression *> aggregate_kind;
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	for (auto &expr : aggregates) {
		assert(expr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = (BoundAggregateExpression &)*expr;
		aggregate_kind.push_back(&aggr);
		if (aggr.children.size()) {
			for (index_t i = 0; i < aggr.children.size(); ++i) {
				payload_types.push_back(aggr.children[i]->return_type);
			}
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

PhysicalHashAggregateOperatorState::PhysicalHashAggregateOperatorState(PhysicalHashAggregate *parent,
                                                                       PhysicalOperator *child)
    : PhysicalOperatorState(child), ht_scan_position(0), tuples_scanned(0) {
	vector<TypeId> group_types, aggregate_types;
	for (auto &expr : parent->groups) {
		group_types.push_back(expr->return_type);
	}
	group_chunk.Initialize(group_types);
	for (auto &expr : parent->aggregates) {
		aggregate_types.push_back(expr->return_type);
	}
	if (aggregate_types.size() > 0) {
		aggregate_chunk.Initialize(aggregate_types);
	}
}
