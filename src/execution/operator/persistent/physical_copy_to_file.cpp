#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <fstream>

using namespace std;

namespace duckdb {

class CopyToFunctionGlobalState : public GlobalOperatorState {
public:
	CopyToFunctionGlobalState(unique_ptr<GlobalFunctionData> global_state) : global_state(move(global_state)) {
	}
	unique_ptr<GlobalFunctionData> global_state;
};

class CopyToFunctionLocalState : public LocalSinkState {
public:
	CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state) : local_state(move(local_state)) {
	}
	unique_ptr<LocalFunctionData> local_state;
};

void PhysicalCopyToFile::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	//	chunk.SetCardinality(1);
	//	chunk.SetValue(0, 0, Value::BIGINT(total));

	state->finished = true;
}

void PhysicalCopyToFile::Sink(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate,
                              DataChunk &input) {
	function.copy_to_sink(context.client, *bind_data, *((CopyToFunctionGlobalState &)gstate).global_state,
	                      *((CopyToFunctionLocalState &)lstate).local_state, input);
}

void PhysicalCopyToFile::Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	function.copy_to_combine(context.client, *bind_data, *((CopyToFunctionGlobalState &)gstate).global_state,
	                         *((CopyToFunctionLocalState &)lstate).local_state);
}
void PhysicalCopyToFile::Finalize(ClientContext &context, unique_ptr<GlobalOperatorState> gstate) {
	auto global_state = (CopyToFunctionGlobalState *)gstate.get();
	function.copy_to_finalize(context, *bind_data, *global_state->global_state);
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<CopyToFunctionLocalState>(function.copy_to_initialize_local(context.client, *bind_data));
}
unique_ptr<GlobalOperatorState> PhysicalCopyToFile::GetGlobalState(ClientContext &context) {
	return make_unique<CopyToFunctionGlobalState>(function.copy_to_initialize_global(context, *bind_data));
}

} // namespace duckdb
