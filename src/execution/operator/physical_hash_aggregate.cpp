
#include "execution/operator/physical_hash_aggregate.hpp"
#include "execution/expression_executor.hpp"
#include "execution/vector/vector_operations.hpp"

#include "parser/expression/aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

void PhysicalHashAggregate::InitializeChunk(DataChunk &chunk) {
	PhysicalAggregate::InitializeChunk(chunk);
}

PhysicalHashAggregate::PhysicalHashAggregate(
    vector<unique_ptr<AbstractExpression>> expressions)
    : PhysicalAggregate(move(expressions), PhysicalOperatorType::HASH_GROUP_BY) {
    	Initialize();
}

PhysicalHashAggregate::PhysicalHashAggregate(
    vector<unique_ptr<AbstractExpression>> expressions,
    vector<unique_ptr<AbstractExpression>> groups)
    : PhysicalAggregate(move(expressions), move(groups), PhysicalOperatorType::HASH_GROUP_BY) {
    	Initialize();

}

void PhysicalHashAggregate::Initialize() {

}

void PhysicalHashAggregate::GetChunk(DataChunk &chunk,
                                     PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashAggregateOperatorState *>(state_);
	chunk.Reset();

	if (state->finished) {
		return;
	}

	ExpressionExecutor executor(state->child_chunk, state);

	do {
		if (children.size() > 0) {
			// resolve the child chunk if there is one
			children[0]->GetChunk(state->child_chunk, state->child_state.get());
		}

		if (groups.size() > 0) {
			// aggregation with groups
			DataChunk &group_chunk = state->group_chunk;
			DataChunk &payload_chunk = state->payload_chunk;
			for (size_t i = 0; i < groups.size(); i++) {
				auto &expr = groups[i];
				executor.Execute(expr.get(), *group_chunk.data[i]);
				group_chunk.count = group_chunk.data[i]->count;
			}
			size_t i = 0;
			for(auto &expr : aggregates) {
				if (expr->children.size() > 0) {
					auto &child = expr->children[0];
					executor.Execute(child.get(), *payload_chunk.data[i]);
					payload_chunk.count = payload_chunk.data[i]->count;
					i++;
				}
			}
			state->ht->AddChunk(group_chunk, payload_chunk);
		} else {
			// aggregation without groups
			// merge into the fixed list of aggregates
			for(size_t i = 0; i < aggregates.size(); i++) {
				executor.Merge(*aggregates[i], state->aggregates[i]);
			}
		}
	} while(state->child_chunk.count != 0);

	if (groups.size() > 0) {
		state->ht->Scan(state->ht_scan_position, state->aggregate_chunk);
		if (state->aggregate_chunk.count == 0) {
			return;
		}
	} else {
		state->finished = true;
	}
	// we finished the child chunk
	// actually compute the final projection list now
	for (size_t i = 0; i < select_list.size(); i++) {
		auto &expr = select_list[i];
		executor.Execute(expr.get(), *chunk.data[i]);
	}
	chunk.count = chunk.data[0]->count;
	for (size_t i = 0; i < chunk.column_count; i++) {
		if (chunk.count != chunk.data[i]->count) {
			throw Exception("Projection count mismatch!");
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	auto state = make_unique<PhysicalHashAggregateOperatorState>(this, children.size() == 0 ? nullptr : children[0].get());
	if (groups.size() > 0) {
		size_t group_width = 0, payload_width = 0;
		vector<TypeId> group_types, payload_types, aggregate_types;
		std::vector<ExpressionType> aggregate_kind;
		for (auto &expr : groups) {
			group_types.push_back(expr->return_type);
			group_width += GetTypeIdSize(expr->return_type);
		}
		state->group_chunk.Initialize(group_types);
		for (auto &expr : aggregates) {
			if (expr->children.size() > 0) {
				auto& child = expr->children[0];
				payload_types.push_back(child->return_type);
				payload_width += GetTypeIdSize(child->return_type);
				aggregate_kind.push_back(expr->type);
			}
		}
		state->payload_chunk.Initialize(payload_types);

		state->ht = make_unique<SuperLargeHashTable>(1024, group_width, payload_width, aggregate_kind);
	}
	return move(state);
}
