#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

class CopyToFunctionGlobalState : public GlobalOperatorState {
public:
	CopyToFunctionGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), global_state(move(global_state)) {
	}

	idx_t rows_copied;
	unique_ptr<GlobalFunctionData> global_state;
};

class CopyToFunctionLocalState : public LocalSinkState {
public:
	CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state) : local_state(move(local_state)) {
	}
	unique_ptr<LocalFunctionData> local_state;
};

void PhysicalCopyToFile::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &g = (CopyToFunctionGlobalState &)*sink_state;

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.rows_copied));

	state->finished = true;
}

void PhysicalCopyToFile::Sink(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate,
                              DataChunk &input) {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	g.rows_copied += input.size();
	function.copy_to_sink(context.client, *bind_data, *g.global_state, *l.local_state, input);
}

void PhysicalCopyToFile::Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	if (function.copy_to_combine) {
		function.copy_to_combine(context.client, *bind_data, *g.global_state, *l.local_state);
	}
}
void PhysicalCopyToFile::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) {
	auto g = (CopyToFunctionGlobalState *)gstate.get();
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *g->global_state);
	}
	PhysicalSink::Finalize(pipeline, context, move(gstate));
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<CopyToFunctionLocalState>(function.copy_to_initialize_local(context.client, *bind_data));
}
unique_ptr<GlobalOperatorState> PhysicalCopyToFile::GetGlobalState(ClientContext &context) {
	return make_unique<CopyToFunctionGlobalState>(function.copy_to_initialize_global(context, *bind_data));
}

} // namespace duckdb
