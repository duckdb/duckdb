#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

struct DuckDBLogData : public GlobalTableFunctionState {
	DuckDBLogData(ColumnDataCollection &collection_p) : collection(collection_p), state() {
		collection.InitializeScan(state);
	}

	ColumnDataCollection &collection;
	ColumnDataScanState state;
};

static unique_ptr<FunctionData> DuckDBLogBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("context_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("log_level");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBLogInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBLogData>(*context.db->GetLogManager().log_storage->log_entries);
	return std::move(result);
}

void DuckDBLogFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBLogData>();
	data.collection.Scan(data.state, output);
}

void DuckDBLogFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_logs", {}, DuckDBLogFunction, DuckDBLogBind, DuckDBLogInit));
}

} // namespace duckdb
