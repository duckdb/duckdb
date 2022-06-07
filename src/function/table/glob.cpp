#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

struct GlobFunctionBindData : public TableFunctionData {
	vector<string> files;
};

static unique_ptr<FunctionData> GlobFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.enable_external_access) {
		throw PermissionException("Globbing is disabled through configuration");
	}
	auto result = make_unique<GlobFunctionBindData>();
	auto &fs = FileSystem::GetFileSystem(context);
	result->files = fs.Glob(StringValue::Get(input.inputs[0]), context);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file");
	return move(result);
}

struct GlobFunctionState : public GlobalTableFunctionState {
	GlobFunctionState() : current_idx(0) {
	}

	idx_t current_idx;
};

static unique_ptr<GlobalTableFunctionState> GlobFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<GlobFunctionState>();
}

static void GlobFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (GlobFunctionBindData &)*data_p.bind_data;
	auto &state = (GlobFunctionState &)*data_p.global_state;

	idx_t count = 0;
	idx_t next_idx = MinValue<idx_t>(state.current_idx + STANDARD_VECTOR_SIZE, bind_data.files.size());
	for (; state.current_idx < next_idx; state.current_idx++) {
		output.data[0].SetValue(count, bind_data.files[state.current_idx]);
		count++;
	}
	output.SetCardinality(count);
}

void GlobTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet glob("glob");
	glob.AddFunction(TableFunction({LogicalType::VARCHAR}, GlobFunction, GlobFunctionBind, GlobFunctionInit));
	set.AddFunction(glob);
}

} // namespace duckdb
