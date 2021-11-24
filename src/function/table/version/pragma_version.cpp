#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

struct PragmaVersionData : public FunctionOperatorData {
	PragmaVersionData() : finished(false) {
	}
	bool finished;
};

static unique_ptr<FunctionData> PragmaVersionBind(ClientContext &context, vector<Value> &inputs,
                                                  unordered_map<string, Value> &named_parameters,
                                                  vector<LogicalType> &input_table_types,
                                                  vector<string> &input_table_names, vector<LogicalType> &return_types,
                                                  vector<string> &names) {
	names.emplace_back("library_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_id");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<FunctionOperatorData> PragmaVersionInit(ClientContext &context, const FunctionData *bind_data,
                                                          const vector<column_t> &column_ids,
                                                          TableFilterCollection *filters) {
	return make_unique<PragmaVersionData>();
}

static void PragmaVersionFunction(ClientContext &context, const FunctionData *bind_data,
                                  FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (PragmaVersionData &)*operator_state;
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
	set.AddFunction(TableFunction("pragma_version", {}, PragmaVersionFunction, PragmaVersionBind, PragmaVersionInit));
}

const char *DuckDB::SourceID() {
	return DUCKDB_SOURCE_ID;
}

const char *DuckDB::LibraryVersion() {
	return DUCKDB_VERSION;
}

string DuckDB::Platform() {
	string os = "linux";
	string arch = "amd64";
#ifdef _WIN32
	os = "windows";
#elif defined(__APPLE__)
	os = "osx";
#endif
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
	arch = "arm64";
#endif
	return os + "_" + arch;
}

} // namespace duckdb
