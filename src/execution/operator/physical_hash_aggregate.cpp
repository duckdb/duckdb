
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
    : PhysicalAggregate(move(expressions), PhysicalOperatorType::HASH_GROUP_BY), tuple_size(0) {
    	Initialize();
}

PhysicalHashAggregate::PhysicalHashAggregate(
    vector<unique_ptr<AbstractExpression>> expressions,
    vector<unique_ptr<AbstractExpression>> groups)
    : PhysicalAggregate(move(expressions), move(groups), PhysicalOperatorType::HASH_GROUP_BY), tuple_size(0) {
    	Initialize();

}

void PhysicalHashAggregate::Initialize() {
	for(auto &expr : groups) {
		tuple_size += GetTypeIdSize(expr->return_type);
	}
	for (auto &expr : select_list) {
		tuple_size += GetTypeIdSize(expr->return_type);
	}
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
			children[0]->GetChunk(state->child_chunk, state->child_state.get());
		}

		DataChunk &group_chunk = state->group_chunk;
		Vector hashes(TypeId::INTEGER);
		if (groups.size() > 0) {
			if (group_chunk.column_count == 0) {
				// initialize group chunk
				vector<TypeId> types;
				for (auto &expr : groups) {
					types.push_back(expr->return_type);
				}
				group_chunk.Initialize(types);
			}

			size_t i = 0;
			for (size_t i = 0; i < groups.size(); i++) {
				auto &expr = groups[i];
				executor.Execute(expr.get(), *group_chunk.data[i]);
				if (i == 0) {
					hashes.Resize(group_chunk.data[i]->count);
					VectorOperations::Hash(*group_chunk.data[i], hashes);
				} else {
					VectorOperations::CombineHash(hashes, *group_chunk.data[i], hashes);
				}
			}
			throw NotImplementedException("group by");
		} else {
			// aggregation without 
			for(size_t i = 0; i < aggregates.size(); i++) {
				executor.Merge(*aggregates[i], state->aggregates[i]);
			}
		}
	} while(state->child_chunk.count != 0);

	// we finished the child chunk
	// actually compute the final projection list now
	for (size_t i = 0; i < select_list.size(); i++) {
		auto &expr = select_list[i];
		executor.Execute(expr.get(), *chunk.data[i]);
	}
	chunk.count = chunk.data[0]->count;
	state->finished = true;
	for (size_t i = 0; i < chunk.column_count; i++) {
		if (chunk.count != chunk.data[i]->count) {
			throw Exception("Projection count mismatch!");
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	return make_unique<PhysicalHashAggregateOperatorState>(this, children.size() == 0 ? nullptr : children[0].get());
}
