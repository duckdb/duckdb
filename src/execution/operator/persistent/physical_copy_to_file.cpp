#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/file_system.hpp"

#include <algorithm>

namespace duckdb {

class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	explicit CopyToFunctionGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), global_state(move(global_state)) {
	}

	idx_t rows_copied;
	unique_ptr<GlobalFunctionData> global_state;
};

class CopyToFunctionLocalState : public LocalSinkState {
public:
	explicit CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state) : local_state(move(local_state)) {
	}
	unique_ptr<LocalFunctionData> local_state;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_path = tmp_file_path.substr(0, tmp_file_path.length() - 4);
	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}
	fs.MoveFile(tmp_file_path, file_path);
}

PhysicalCopyToFile::PhysicalCopyToFile(vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::COPY_TO_FILE, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data)) {
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                        DataChunk &input) const {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	g.rows_copied += input.size();
	function.copy_to_sink(context, *bind_data, *g.global_state, *l.local_state, input);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalCopyToFile::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	if (function.copy_to_combine) {
		function.copy_to_combine(context, *bind_data, *g.global_state, *l.local_state);
	}
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              GlobalSinkState &gstate_p) const {
	auto &gstate = (CopyToFunctionGlobalState &)gstate_p;
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<CopyToFunctionLocalState>(function.copy_to_initialize_local(context, *bind_data));
}
unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<CopyToFunctionGlobalState>(function.copy_to_initialize_global(context, *bind_data, file_path));
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CopyToFileState : public GlobalSourceState {
public:
	CopyToFileState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCopyToFile::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CopyToFileState>();
}

void PhysicalCopyToFile::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CopyToFileState &)gstate;
	auto &g = (CopyToFunctionGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.rows_copied));
	state.finished = true;
}

} // namespace duckdb
