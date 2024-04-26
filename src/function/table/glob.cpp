#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/multi_file_reader.hpp"

namespace duckdb {

struct GlobFunctionBindData : public TableFunctionData {
	//! The path to glob
	string path;
	unique_ptr<MultiFileReader> multi_file_reader;
};

static unique_ptr<FunctionData> GlobFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GlobFunctionBindData>();
	result->path = input.inputs[0].ToString();
	result->multi_file_reader = MultiFileReader::Create(context, input.table_function);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file");
	return std::move(result);
}

struct GlobFunctionState : public GlobalTableFunctionState {
	GlobFunctionState() {
	}

	unique_ptr<MultiFileList> file_list;
	MultiFileListScanData file_list_scan;
};

static unique_ptr<GlobalTableFunctionState> GlobFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<GlobFunctionBindData>();
	auto res = make_uniq<GlobFunctionState>();

	res->file_list = bind_data.multi_file_reader->GetFileList(context, bind_data.path, "Globbing", FileGlobOptions::ALLOW_EMPTY);
	res->file_list->InitializeScan(res->file_list_scan);

	return std::move(res);
}

static void GlobFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<GlobFunctionState>();

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		string file;
		if (!state.file_list->Scan(state.file_list_scan, file)) {
			break;
		}
		output.data[0].SetValue(count++, file);
	}
	output.SetCardinality(count);
}

void GlobTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction glob_function("glob", {LogicalType::VARCHAR}, GlobFunction, GlobFunctionBind, GlobFunctionInit);
	set.AddFunction(MultiFileReader::CreateFunctionSet(glob_function));
}

} // namespace duckdb
