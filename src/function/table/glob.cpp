#include "duckdb/function/table/range.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/file_system.hpp"

using namespace std;

namespace duckdb {

struct GlobFunctionBindData : public TableFunctionData {
	vector<string> files;
};

static unique_ptr<FunctionData> glob_function_bind(ClientContext &context, vector<Value> &inputs,
                                                   unordered_map<string, Value> &named_parameters,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<GlobFunctionBindData>();
	auto &fs = FileSystem::GetFileSystem(context);
	result->files = fs.Glob(inputs[0].str_value);
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("file");
	return move(result);
}

struct GlobFunctionState : public FunctionOperatorData {
	GlobFunctionState() : current_idx(0) {
	}

	idx_t current_idx;
};

static unique_ptr<FunctionOperatorData> glob_function_init(ClientContext &context, const FunctionData *bind_data,
                                                           vector<column_t> &column_ids,
                                                           TableFilterSet *table_filters) {
	return make_unique<GlobFunctionState>();
}

static void glob_function(ClientContext &context, const FunctionData *bind_data_, FunctionOperatorData *state_,
                          DataChunk &output) {
	auto &bind_data = (GlobFunctionBindData &)*bind_data_;
	auto &state = (GlobFunctionState &)*state_;

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
	glob.AddFunction(TableFunction({LogicalType::VARCHAR}, glob_function, glob_function_bind, glob_function_init));
	set.AddFunction(glob);
}

} // namespace duckdb
