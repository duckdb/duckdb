
#include "execution/operator/set/physical_union.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

PhysicalUnion::PhysicalUnion(LogicalOperator &op, std::unique_ptr<PhysicalOperator> top,
                             std::unique_ptr<PhysicalOperator> bottom)
    : PhysicalOperator(PhysicalOperatorType::UNION, op.types) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

// first exhaust top, then exhaust bottom. state to remember which.
void PhysicalUnion::_GetChunk(ClientContext &context, DataChunk &chunk,
                              PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalUnionOperatorState *>(state_);
	if (!state->top_done) {
		children[0]->GetChunk(context, chunk, state->top_state.get());
		if (chunk.size() == 0) {
			state->top_done = true;
		}
	}
	if (state->top_done) {
		children[1]->GetChunk(context, chunk, state->bottom_state.get());
	}
	if (chunk.size() == 0) {
		state->finished = true;
	}
}

std::unique_ptr<PhysicalOperatorState>
PhysicalUnion::GetOperatorState(ExpressionExecutor *parent_executor) {
	auto state = make_unique<PhysicalUnionOperatorState>(parent_executor);
	state->top_state = children[0]->GetOperatorState(parent_executor);
	state->bottom_state = children[1]->GetOperatorState(parent_executor);
	return (move(state));
}
