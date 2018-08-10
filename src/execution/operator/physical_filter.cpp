
#include "execution/operator/physical_filter.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

void PhysicalFilter::InitializeChunk(DataChunk &chunk) {
	// just copy the chunk data of the child
	children[0]->InitializeChunk(chunk);
}

void PhysicalFilter::GetChunk(DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOperatorState *>(state_);
	chunk.Reset();

	do {
		children[0]->GetChunk(state->child_chunk, state->child_state.get());
		if (state->child_chunk.count == 0) {
			return;
		}

		if (expressions.size() == 0) {
			throw Exception(
			    "Attempting to execute a filter without expressions");
		}

		Vector result(TypeId::BOOLEAN, state->child_chunk.count);
		ExpressionExecutor executor(state->child_chunk);
		executor.Execute(expressions[0].get(), result);
		// AND together the remaining filters!
		for (size_t i = 1; i < expressions.size(); i++) {
			auto &expr = expressions[i];
			executor.Merge(expr.get(), result);
		}
		// now generate the selection vector
		bool *matches = (bool *)result.data;
		chunk.sel_vector = unique_ptr<sel_t[]>(new sel_t[result.count]);
		size_t match_count = 0;
		for (size_t i = 0; i < result.count; i++) {
			if (matches[i]) {
				chunk.sel_vector[match_count++] = i;
			}
		}
		if (match_count == result.count) {
			// everything matches! don't need a selection vector!
			chunk.sel_vector.reset();
		}
		for (size_t i = 0; i < chunk.column_count; i++) {
			// create a reference to the vector of the child chunk
			chunk.data[i]->Reference(*state->child_chunk.data[i].get());
			// and assign the selection vector
			chunk.data[i]->count = match_count;
			chunk.data[i]->sel_vector = chunk.sel_vector.get();
		}
		chunk.count = match_count;
	} while (chunk.count == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalFilter::GetOperatorState() {
	return make_unique<PhysicalOperatorState>(children[0].get());
}
