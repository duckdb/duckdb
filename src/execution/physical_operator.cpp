#include "execution/physical_operator.hpp"

#include "main/client_context.hpp"
#include "common/string_util.hpp"

using namespace duckdb;
using namespace std;

string PhysicalOperator::ToString(size_t depth) const {
	string extra_info = StringUtil::Replace(ExtraRenderInformation(), "\n", " ");
	StringUtil::RTrim(extra_info);
	if (!extra_info.empty()) {
		extra_info = "[" + extra_info + "]";
	}
	string result = PhysicalOperatorToString(type) + extra_info;
	if (children.size() > 0) {
		for (size_t i = 0; i < children.size(); i++) {
			result += "\n" + string(depth * 4, ' ');
			auto &child = children[i];
			result += child->ToString(depth + 1);
		}
		result += "";
	}
	return result;
}

PhysicalOperatorState::PhysicalOperatorState(PhysicalOperator *child) : finished(false) {
	if (child) {
		child->InitializeChunk(child_chunk);
		child_state = child->GetOperatorState();
	}
}

void PhysicalOperator::GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	if (context.interrupted) {
		throw InterruptException();
	}
	// finished with this operator
	chunk.Reset();
	if (state->finished) {
		return;
	}

	context.profiler.StartOperator(this);
	_GetChunk(context, chunk, state);
	context.profiler.EndOperator(chunk);

	chunk.Verify();
}

void PhysicalOperator::Print() {
	Printer::Print(ToString());
}
