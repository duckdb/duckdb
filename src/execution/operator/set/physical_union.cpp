#include "duckdb/execution/operator/set/physical_union.hpp"

using namespace std;

namespace duckdb {

class PhysicalUnionOperatorState : public PhysicalOperatorState {
public:
	PhysicalUnionOperatorState(PhysicalOperator &op) : PhysicalOperatorState(op, nullptr), top_done(false) {
	}
	unique_ptr<PhysicalOperatorState> top_state;
	unique_ptr<PhysicalOperatorState> bottom_state;
	bool top_done = false;
};

PhysicalUnion::PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                             unique_ptr<PhysicalOperator> bottom)
    : PhysicalOperator(PhysicalOperatorType::UNION, move(types)) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

// first exhaust top, then exhaust bottom. state to remember which.
void PhysicalUnion::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
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

unique_ptr<PhysicalOperatorState> PhysicalUnion::GetOperatorState() {
	auto state = make_unique<PhysicalUnionOperatorState>(*this);
	state->top_state = children[0]->GetOperatorState();
	state->bottom_state = children[1]->GetOperatorState();
	return (move(state));
}

} // namespace duckdb
