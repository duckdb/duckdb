#include "duckdb/execution/operator/projection/physical_projection.hpp"

#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

class PhysicalProjectionState : public PhysicalOperatorState {
public:
	PhysicalProjectionState(PhysicalOperator *child, vector<unique_ptr<Expression>> &expressions)
	    : PhysicalOperatorState(child), executor(expressions) {
		assert(child);
	}

	ExpressionExecutor executor;
};

void PhysicalProjection::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalProjectionState *>(state_);

	// get the next chunk from the child
	children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
	if (state->child_chunk.size() == 0) {
		return;
	}

	state->executor.Execute(state->child_chunk, chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalProjection::GetOperatorState() {
	return make_unique<PhysicalProjectionState>(children[0].get(), select_list);
}

string PhysicalProjection::ExtraRenderInformation() const {
	string extra_info;
	for (auto &expr : select_list) {
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}
