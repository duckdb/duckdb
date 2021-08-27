#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

// class PhysicalUnionOperatorState : public OperatorState {
// public:
// 	explicit PhysicalUnionOperatorState(PhysicalOperator &op) : OperatorState(op, nullptr), top_done(false) {
// 	}
// 	unique_ptr<OperatorState> top_state;
// 	unique_ptr<OperatorState> bottom_state;
// 	bool top_done = false;
// };

PhysicalUnion::PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                             unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNION, move(types), estimated_cardinality) {
	children.push_back(move(top));
	children.push_back(move(bottom));
	throw InternalException("FIXME: physical union");
}

// // first exhaust top, then exhaust bottom. state to remember which.
// void PhysicalUnion::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
//                                      OperatorState *state_p) const {
// 	auto state = reinterpret_cast<PhysicalUnionOperatorState *>(state_p);
// 	if (!state->top_done) {
// 		children[0]->GetChunk(context, chunk, state->top_state.get());
// 		if (chunk.size() == 0) {
// 			state->top_done = true;
// 		}
// 	}
// 	if (state->top_done) {
// 		children[1]->GetChunk(context, chunk, state->bottom_state.get());
// 	}
// 	if (chunk.size() == 0) {
// 		state->finished = true;
// 	}
// }

// unique_ptr<OperatorState> PhysicalUnion::GetOperatorState() {
// 	auto state = make_unique<PhysicalUnionOperatorState>(*this);
// 	state->top_state = children[0]->GetOperatorState();
// 	state->bottom_state = children[1]->GetOperatorState();
// 	return (move(state));
// }

// void PhysicalUnion::FinalizeOperatorState(OperatorState &state_p, ExecutionContext &context) {
// 	auto &state = reinterpret_cast<PhysicalUnionOperatorState &>(state_p);
// 	if (!children.empty() && state.top_state) {
// 		children[0]->FinalizeOperatorState(*state.top_state, context);
// 	}
// 	if (!children.empty() && state.bottom_state) {
// 		children[1]->FinalizeOperatorState(*state.bottom_state, context);
// 	}
// }

} // namespace duckdb
