#include "duckdb/function/table/sqlite_functions.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

struct PragmaVersionData : public FunctionOperatorData {
	PragmaVersionData() : finished(false) {
	}
	bool finished;
};

static unique_ptr<FunctionData> pragma_version_bind(ClientContext &context, vector<Value> &inputs,
                                                    unordered_map<string, Value> &named_parameters,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("library_version");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("source_id");
	return_types.push_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<FunctionOperatorData> pragma_version_init(
    ClientContext &context,
    const FunctionData *bind_data,
    OperatorTaskInfo *task_info,
    vector<column_t> &column_ids,
    unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	return make_unique<PragmaVersionData>();
}

static void pragma_version(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (PragmaVersionData &) *operator_state;
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, DuckDB::LibraryVersion());
	output.SetValue(1, 0, DuckDB::SourceID());
	data.finished = true;
}

void PragmaVersion::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_version", {}, pragma_version, pragma_version_bind, pragma_version_init));
}

const char *DuckDB::SourceID() {
	return DUCKDB_SOURCE_ID;
}

const char *DuckDB::LibraryVersion() {
	return "DuckDB";
}

} // namespace duckdb
