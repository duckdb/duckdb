
#include "execution/operator/helper/physical_prune_columns.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

vector<string> PhysicalPruneColumns::GetNames() {
	auto names = children[0]->GetNames();
	assert(column_limit <= names.size());
	names.erase(names.begin() + column_limit, names.end());
	return names;
}
vector<TypeId> PhysicalPruneColumns::GetTypes() {
	auto types = children[0]->GetTypes();
	assert(column_limit <= types.size());
	types.erase(types.begin() + column_limit, types.end());
	return types;
}

void PhysicalPruneColumns::_GetChunk(ClientContext &context, DataChunk &chunk,
                                     PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOperatorState *>(state_);
	chunk.Reset();

	children[0]->GetChunk(context, state->child_chunk,
	                      state->child_state.get());
	if (state->child_chunk.size() == 0) {
		return;
	}
	assert(column_limit <= state->child_chunk.column_count);
	for (size_t i = 0; i < column_limit; i++) {
		chunk.data[i].Reference(state->child_chunk.data[i]);
	}
	chunk.sel_vector = state->child_chunk.sel_vector;
}

unique_ptr<PhysicalOperatorState>
PhysicalPruneColumns::GetOperatorState(ExpressionExecutor *parent) {
	return make_unique<PhysicalOperatorState>(children[0].get(), parent);
}
