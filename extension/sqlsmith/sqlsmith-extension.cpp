#define DUCKDB_EXTENSION_MAIN

#include "sqlsmith-extension.hpp"
#include "sqlsmith.hh"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
//#include "duckdb/parser/parsed_data/create_view_info.hpp"
//#include "duckdb/parser/parser.hpp"
//#include "duckdb/parser/statement/select_statement.hpp"
//#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
//#include "duckdb/main/client_context.hpp"
#endif

namespace duckdb {

struct SQLSmithFunctionData : public TableFunctionData {
	SQLSmithFunctionData() {
	}

	int32_t seed = -1;
	idx_t max_queries = 0;
	bool exclude_catalog = false;
	bool dump_all_queries = false;
	bool dump_all_graphs = false;
	bool verbose_output = false;
	string complete_log;
	string log;
	bool finished = false;
};

static unique_ptr<FunctionData> SQLSmithBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<SQLSmithFunctionData>();
	for (auto &kv : input.named_parameters) {
		if (kv.first == "seed") {
			result->seed = IntegerValue::Get(kv.second);
		} else if (kv.first == "max_queries") {
			result->max_queries = UBigIntValue::Get(kv.second);
		} else if (kv.first == "exclude_catalog") {
			result->exclude_catalog = BooleanValue::Get(kv.second);
		} else if (kv.first == "dump_all_queries") {
			result->dump_all_queries = BooleanValue::Get(kv.second);
		} else if (kv.first == "dump_all_graphs") {
			result->dump_all_graphs = BooleanValue::Get(kv.second);
		} else if (kv.first == "verbose_output") {
			result->verbose_output = BooleanValue::Get(kv.second);
		} else if (kv.first == "complete_log") {
			result->complete_log = StringValue::Get(kv.second);
		} else if (kv.first == "log") {
			result->log = StringValue::Get(kv.second);
		}
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return move(result);
}

static void SQLSmithFunction(ClientContext &context, const FunctionData *bind_data,
                             FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (SQLSmithFunctionData &)*bind_data;
	if (data.finished) {
		return;
	}

	duckdb_sqlsmith::SQLSmithOptions options;
	options.seed = data.seed;
	options.max_queries = data.max_queries;
	options.exclude_catalog = data.exclude_catalog;
	options.dump_all_queries = data.dump_all_queries;
	options.dump_all_graphs = data.dump_all_graphs;
	options.verbose_output = data.verbose_output;
	options.complete_log = data.complete_log;
	options.log = data.log;
	duckdb_sqlsmith::run_sqlsmith(DatabaseInstance::GetDatabase(context), options);

	data.finished = true;
}

void SQLSmithExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	TableFunction sqlsmith_func("sqlsmith", {}, SQLSmithFunction, SQLSmithBind);
	sqlsmith_func.named_parameters["seed"] = LogicalType::INTEGER;
	sqlsmith_func.named_parameters["max_queries"] = LogicalType::UBIGINT;
	sqlsmith_func.named_parameters["exclude_catalog"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["dump_all_queries"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["dump_all_graphs"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["verbose_output"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["complete_log"] = LogicalType::VARCHAR;
	sqlsmith_func.named_parameters["log"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo sqlsmith_info(sqlsmith_func);

	// create the sqlsmith function
	catalog.CreateTableFunction(*con.context, &sqlsmith_info);

	con.Commit();
}

std::string SQLSmithExtension::Name() {
	return "sqlsmith";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void sqlsmith_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SQLSmithExtension>();
}

DUCKDB_EXTENSION_API const char *sqlsmith_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
