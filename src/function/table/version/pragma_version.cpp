#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/platform.hpp"

#include <cstdint>

namespace duckdb {

struct PragmaVersionData : public GlobalTableFunctionState {
	PragmaVersionData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("library_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_id");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaVersionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaVersionData>();
}

static void PragmaVersionFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaVersionData>();
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
	TableFunction pragma_version("pragma_version", {}, PragmaVersionFunction);
	pragma_version.bind = PragmaVersionBind;
	pragma_version.init_global = PragmaVersionInit;
	set.AddFunction(pragma_version);
}

idx_t DuckDB::StandardVectorSize() {
	return STANDARD_VECTOR_SIZE;
}

const char *DuckDB::SourceID() {
	return DUCKDB_SOURCE_ID;
}

const char *DuckDB::LibraryVersion() {
	return DUCKDB_VERSION;
}

string DuckDB::Platform() {
	return DuckDBPlatform();
}

struct PragmaPlatformData : public GlobalTableFunctionState {
	PragmaPlatformData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaPlatformBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("platform");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaPlatformInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaPlatformData>();
}

static void PragmaPlatformFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaPlatformData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, DuckDB::Platform());
	data.finished = true;
}

void PragmaPlatform::RegisterFunction(BuiltinFunctions &set) {
	TableFunction pragma_platform("pragma_platform", {}, PragmaPlatformFunction);
	pragma_platform.bind = PragmaPlatformBind;
	pragma_platform.init_global = PragmaPlatformInit;
	set.AddFunction(pragma_platform);
}

} // namespace duckdb
