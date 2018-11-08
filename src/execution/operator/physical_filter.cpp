
#include "execution/operator/physical_filter.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

vector<string> PhysicalFilter::GetNames() {
	return children[0]->GetNames();
}
vector<TypeId> PhysicalFilter::GetTypes() {
	return children[0]->GetTypes();
}

void PhysicalFilter::_GetChunk(ClientContext &context, DataChunk &chunk,
                               PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOperatorState *>(state_);
	chunk.Reset();

	do {
		children[0]->GetChunk(context, state->child_chunk,
		                      state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}

		if (expressions.size() == 0) {
			throw Exception(
			    "Attempting to execute a filter without expressions");
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

unique_ptr<PhysicalOperatorState>
PhysicalFilter::GetOperatorState(ExpressionExecutor *parent) {
	return make_unique<PhysicalOperatorState>(children[0].get(), parent);
}

string PhysicalFilter::ExtraRenderInformation() {
	string extra_info;
	for (auto &expr : expressions) {
		extra_info += expr->ToString() + "\n";
	}
	return extra_info;
}
