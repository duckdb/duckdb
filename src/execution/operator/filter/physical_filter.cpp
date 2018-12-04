#include "execution/operator/filter/physical_filter.hpp"

#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

void PhysicalFilter::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOperatorState *>(state_);
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}

		if (expressions.size() == 0) {
			throw Exception("Attempting to execute a filter without expressions");
		}

		Vector result(TypeId::BOOLEAN, true, false);
		ExpressionExecutor executor(state, context);
		executor.Merge(expressions, result);

		// now generate the selection vector
		chunk.sel_vector = state->child_chunk.sel_vector;
		for (size_t i = 0; i < chunk.column_count; i++) {
			// create a reference to the vector of the child chunk
			chunk.data[i].Reference(state->child_chunk.data[i]);
		}
		chunk.SetSelectionVector(result);
	} while (chunk.size() == 0);
}

string PhysicalFilter::ExtraRenderInformation() {
	string extra_info;
	for (auto &expr : expressions) {
		extra_info += expr->ToString() + "\n";
	}
	return extra_info;
}
