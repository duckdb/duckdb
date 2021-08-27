#include "duckdb/execution/operator/helper/physical_execute.hpp"

namespace duckdb {

// void PhysicalExecute::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
//                                        PhysicalOperatorState *state_p) const {
// 	D_ASSERT(plan);
// 	plan->GetChunk(context, chunk, state_p);
// }

// unique_ptr<PhysicalOperatorState> PhysicalExecute::GetOperatorState() {
// 	return plan->GetOperatorState();
// }

// void PhysicalExecute::FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) {
// 	if (!children.empty() && state_p.child_state) {
// 		plan->FinalizeOperatorState(state_p, context);
// 	}
// }

} // namespace duckdb
