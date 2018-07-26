
#include "execution/operator/physical_hash_aggregate.hpp"
#include "execution/expression_executor.hpp"
#include "execution/vector/vector_operations.hpp"

using namespace duckdb;
using namespace std;

void PhysicalHashAggregate::InitializeChunk(DataChunk &chunk) {
	// get the chunk types from the projection list
	vector<TypeId> types;
	for (auto &expr : select_list) {
		types.push_back(expr->return_type);
	}
	chunk.Initialize(types);
}

void PhysicalHashAggregate::GetChunk(DataChunk &chunk,
                                     PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashAggregateOperatorState *>(state_);
	chunk.Reset();

	children[0]->GetChunk(state->child_chunk, state->child_state.get());
	if (state->child_chunk.count == 0) {
		return;
	}

	DataChunk &group_chunk = state->group_chunk;
	if (group_chunk.column_count == 0) {
		// initialize group chunk
		vector<TypeId> types;
		for (auto &expr : groups) {
			types.push_back(expr->return_type);
		}
		group_chunk.Initialize(types);
	}

	Vector hashes(TypeId::INTEGER);

	ExpressionExecutor executor(state->child_chunk);
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

	// children[0]->GetChunk(state->child_chunk, state->child_state.get());
	// if (state->child_chunk.count == 0) {
	// 	return;
	// }

	// if (expressions.size() == 0) {
	// 	throw Exception("Attempting to execute a filter without expressions");
	// }

	// Vector result(TypeId::BOOLEAN, state->child_chunk.count);
	// ExpressionExecutor executor(state->child_chunk);
	// executor.Execute(expressions[0].get(), result);
	// // AND together the remaining filters!
	// for (size_t i = 1; i < expressions.size(); i++) {
	// 	auto &expr = expressions[i];
	// 	executor.Merge(expr.get(), result);
	// }
	// // now generate the selection vector
	// bool *matches = (bool *)result.data;
	// chunk.sel_vector = unique_ptr<sel_t[]>(new sel_t[result.count]);
	// size_t match_count = 0;
	// for (size_t i = 0; i < result.count; i++) {
	// 	if (matches[i]) {
	// 		chunk.sel_vector[match_count++] = i;
	// 	}
	// }
	// if (match_count == result.count) {
	// 	// everything matches! don't need a selection vector!
	// 	chunk.sel_vector.reset();
	// }
	// for (size_t i = 0; i < chunk.column_count; i++) {
	// 	// create a reference to the vector of the child chunk
	// 	chunk.data[i]->Reference(*state->child_chunk.data[i].get());
	// 	// and assign the selection vector
	// 	chunk.data[i]->count = match_count;
	// 	chunk.data[i]->sel_vector = chunk.sel_vector.get();
	// }
	// chunk.count = match_count;
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	return make_unique<PhysicalHashAggregateOperatorState>(children[0].get());
}
