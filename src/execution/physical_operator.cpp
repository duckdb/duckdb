#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"

using namespace std;

namespace duckdb {

string PhysicalOperator::ToString(idx_t depth) const {
	string extra_info = StringUtil::Replace(ExtraRenderInformation(), "\n", " ");
	StringUtil::RTrim(extra_info);
	if (!extra_info.empty()) {
		extra_info = "[" + extra_info + "]";
	}
	string result = PhysicalOperatorToString(type) + extra_info;
	if (children.size() > 0) {
		for (idx_t i = 0; i < children.size(); i++) {
			result += "\n" + string(depth * 4, ' ');
			auto &child = children[i];
			result += child->ToString(depth + 1);
		}
		result += "";
	}
	return result;
}

PhysicalOperatorState::PhysicalOperatorState(PhysicalOperator &op, PhysicalOperator *child) : finished(false) {
	op.InitializeChunk(initial_chunk);
	if (child) {
		child->InitializeChunkEmpty(child_chunk);
		child_state = child->GetOperatorState();
	}
}

void PhysicalOperator::GetChunk(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	if (context.client.interrupted) {
		throw InterruptException();
	}
	// reset the chunk back to its initial state
	chunk.Reference(state->initial_chunk);

	if (state->finished) {
		return;
	}

	// execute the operator
	context.thread.profiler.StartOperator(this);
	GetChunkInternal(context, chunk, state);
	context.thread.profiler.EndOperator(&chunk);

	chunk.Verify();
}

void PhysicalOperator::Print() {
	Printer::Print(ToString());
}

} // namespace duckdb
