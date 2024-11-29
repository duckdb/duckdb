#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

struct DuckDBLogContextData : public GlobalTableFunctionState {
	DuckDBLogContextData(ColumnDataCollection &collection_p) : collection(collection_p), state() {
		collection.InitializeScan(state);
	}

	ColumnDataCollection &collection;
	ColumnDataScanState state;
};

static unique_ptr<FunctionData> DuckDBLogContextBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("context_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("client_context");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("transaction_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("thread");
	return_types.emplace_back(LogicalType::UBIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBLogContextInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBLogContextData>(*context.db->GetLogManager().log_storage->log_contexts);
	return std::move(result);
}

void DuckDBLogContextFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBLogContextData>();
	data.collection.Scan(data.state, output);
}

void DuckDBLogContextFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_log_contexts", {}, DuckDBLogContextFunction, DuckDBLogContextBind, DuckDBLogContextInit));
}

} // namespace duckdb
