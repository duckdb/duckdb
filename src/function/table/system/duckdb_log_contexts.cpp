#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_storage.hpp"

namespace duckdb {

struct DuckDBLogContextData : public GlobalTableFunctionState {
	explicit DuckDBLogContextData(shared_ptr<LogStorage> log_storage_p) : log_storage(std::move(log_storage_p)) {
		scan_state = log_storage->CreateScanContextsState();
		log_storage->InitializeScanContexts(*scan_state);
	}
	DuckDBLogContextData() : log_storage(nullptr) {
	}

	//! The log storage we are scanning
	shared_ptr<LogStorage> log_storage;
	unique_ptr<LogStorageScanState> scan_state;
};

static unique_ptr<FunctionData> DuckDBLogContextBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("context_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("client_context");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("transaction_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("thread");
	return_types.emplace_back(LogicalType::UBIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBLogContextInit(ClientContext &context, TableFunctionInitInput &input) {
	if (LogManager::Get(context).CanScan()) {
		return make_uniq<DuckDBLogContextData>(LogManager::Get(context).GetLogStorage());
	}
	return make_uniq<DuckDBLogContextData>();
}

void DuckDBLogContextFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBLogContextData>();
	if (data.log_storage) {
		data.log_storage->ScanContexts(*data.scan_state, output);
	}
}

void DuckDBLogContextFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_log_contexts", {}, DuckDBLogContextFunction, DuckDBLogContextBind, DuckDBLogContextInit));
}

} // namespace duckdb
