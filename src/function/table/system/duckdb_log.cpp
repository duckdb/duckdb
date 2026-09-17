#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_options.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_sink.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

struct DuckDBLogData : public GlobalTableFunctionState {
	explicit DuckDBLogData(shared_ptr<LogSink> log_sink_p) : log_sink(std::move(log_sink_p)) {
		scan_state = log_sink->CreateScanState(LoggingTargetTable::LOG_ENTRIES);
		log_sink->InitializeScan(*scan_state);
	}
	DuckDBLogData() : log_sink(nullptr) {
	}

	//! The log sink we are scanning
	shared_ptr<LogSink> log_sink;
	unique_ptr<LogSinkScanState> scan_state;
};

static unique_ptr<FunctionData> DuckDBLogBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("context_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("log_level");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBLogInit(ClientContext &context, TableFunctionInitInput &input) {
	if (LogManager::Get(context).CanScan(LoggingTargetTable::LOG_ENTRIES)) {
		return make_uniq<DuckDBLogData>(LogManager::Get(context).GetLogSink());
	}
	return make_uniq<DuckDBLogData>();
}

void DuckDBLogFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBLogData>();
	if (data.log_sink) {
		data.log_sink->Scan(*data.scan_state, output);
	}
}

unique_ptr<TableRef> DuckDBLogBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto log_sink = LogManager::Get(context).GetLogSink();

	bool denormalized_table = false;
	auto denormalized_table_setting = input.named_parameters.find("denormalized_table");
	if (denormalized_table_setting != input.named_parameters.end()) {
		if (denormalized_table_setting->second.IsNull()) {
			throw InvalidInputException("denormalized_table cannot be NULL");
		}
		denormalized_table = denormalized_table_setting->second.GetValue<bool>();
	}

	// Without join contexts we simply scan the LOG_ENTRIES tables
	if (!denormalized_table) {
		auto res = log_sink->BindReplace(context, input, LoggingTargetTable::LOG_ENTRIES);
		return res;
	}

	// If the sink can bind replace for LoggingTargetTable::ALL_LOGS, we use that since that will be most efficient
	auto all_log_scan = log_sink->BindReplace(context, input, LoggingTargetTable::ALL_LOGS);
	if (all_log_scan) {
		return all_log_scan;
	}

	// We cannot scan ALL_LOGS but denormalized_table was requested: we need to inject the join between LOG_ENTRIES and
	// LOG_CONTEXTS
	string sub_query_string = "SELECT l.context_id, scope, connection_id, transaction_id, query_id, thread_id, "
	                          "timestamp, type, log_level, message"
	                          " FROM (SELECT row_number() OVER () AS rowid, * FROM duckdb_logs()) as l JOIN "
	                          "duckdb_log_contexts() as c ON l.context_id=c.context_id order by timestamp, l.rowid;";
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sub_query_string);
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));

	return duckdb::make_uniq<SubqueryRef>(std::move(select_stmt));
}

void DuckDBLogFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction logs_fun("duckdb_logs", {}, DuckDBLogFunction, DuckDBLogBind, DuckDBLogInit);
	logs_fun.bind_replace = DuckDBLogBindReplace;
	logs_fun.named_parameters["denormalized_table"] = LogicalType::BOOLEAN;
	set.AddFunction(logs_fun);
}

} // namespace duckdb
