#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

void PhysicalFilter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOperatorState *>(state_);
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}

		assert(expressions.size() > 0);

		Vector result(TypeId::BOOLEAN, true, false);
		ExpressionExecutor executor(state->child_chunk);
		executor.Merge(expressions, result);

		// now generate the selection vector
		chunk.sel_vector = state->child_chunk.sel_vector;
		for (index_t i = 0; i < chunk.column_count; i++) {
			// create a reference to the vector of the child chunk
			chunk.data[i].Reference(state->child_chunk.data[i]);
		}
		chunk.SetSelectionVector(result);
	} while (chunk.size() == 0);
}

string PhysicalFilter::ExtraRenderInformation() const {
	string extra_info;
	for (auto &expr : expressions) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}
