#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_options.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_storage.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"


namespace duckdb {

struct DuckDBLogData : public GlobalTableFunctionState {
	explicit DuckDBLogData(shared_ptr<LogStorage> log_storage_p) : log_storage(std::move(log_storage_p)) {
		scan_state = log_storage->CreateScanEntriesState();
		log_storage->InitializeScanEntries(*scan_state);
	}
	DuckDBLogData() : log_storage(nullptr) {
	}

	//! The log storage we are scanning
	shared_ptr<LogStorage> log_storage;
	unique_ptr<LogStorageScanState> scan_state;
};

static unique_ptr<FunctionData> DuckDBLogBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	bool context_id_only = false;
	auto context_id_only_settings = input.named_parameters.find("context_id_only");
	if (context_id_only_settings != input.named_parameters.end()) {
		context_id_only = context_id_only_settings->second.GetValue<bool>();
	}

	if (context_id_only) {
		names.emplace_back("context_id");
		return_types.emplace_back(LogicalType::UBIGINT);
	}

	names.emplace_back("timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("log_level");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);

	if (!context_id_only) {
		names.emplace_back("scope");
		return_types.emplace_back(LogicalType::VARCHAR);

		names.emplace_back("client_context");
		return_types.emplace_back(LogicalType::UBIGINT);

		names.emplace_back("transaction_id");
		return_types.emplace_back(LogicalType::UBIGINT);

		names.emplace_back("thread");
		return_types.emplace_back(LogicalType::UBIGINT);
	}

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBLogInit(ClientContext &context, TableFunctionInitInput &input) {
	if (LogManager::Get(context).CanScan()) {
		return make_uniq<DuckDBLogData>(LogManager::Get(context).GetLogStorage());
	}
	return make_uniq<DuckDBLogData>();
}

void DuckDBLogFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBLogData>();
	if (data.log_storage) {
		data.log_storage->ScanEntries(*data.scan_state, output);
	}
}

static unique_ptr<SubqueryRef> ParseSubquery(const string &query, const ParserOptions &options, const string &err_msg) {
	Parser parser(options);
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException(err_msg);
	}
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	return duckdb::make_uniq<SubqueryRef>(std::move(select_stmt));
}

// duckdb_logs is slightly wonky in that it bind_replace's itself with a join between the duckdb_logs and
// duckdb_log_contexts table functions.
static unique_ptr<TableRef> DuckDBLogBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	bool context_id_only = false;
	auto context_id_only_settings = input.named_parameters.find("context_id_only");
	if (context_id_only_settings != input.named_parameters.end()) {
		context_id_only = context_id_only_settings->second.GetValue<bool>();
	}

	if (!context_id_only) {
		if (LogManager::Get(context).CanScan()) {
			return std::move(ParseSubquery(
			    "SELECT * exclude (l.context_id, c.context_id) FROM duckdb_logs(context_id_only=true) as l JOIN "
			    "duckdb_log_contexts() as c ON l.context_id=c.context_id order by timestamp;",
			    context.GetParserOptions(), "Expected a single SELECT statement"));
		}
	}

	return nullptr;
}

void DuckDBLogFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction logs_fun("duckdb_logs", {}, DuckDBLogFunction, DuckDBLogBind, DuckDBLogInit);
	logs_fun.bind_replace = DuckDBLogBindReplace;
	logs_fun.named_parameters["context_id_only"] = LogicalType::BOOLEAN;
	set.AddFunction(logs_fun);
}

} // namespace duckdb
