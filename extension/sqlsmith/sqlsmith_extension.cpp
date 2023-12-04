#define DUCKDB_EXTENSION_MAIN

#include "sqlsmith_extension.hpp"
#include "sqlsmith.hh"
#include "statement_simplifier.hpp"
#include "fuzzyduck.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/extension_util.hpp"
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

static duckdb::unique_ptr<FunctionData> SQLSmithBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<SQLSmithFunctionData>();
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
	return std::move(result);
}

static void SQLSmithFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<SQLSmithFunctionData>();
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

struct ReduceSQLFunctionData : public TableFunctionData {
	ReduceSQLFunctionData() {
	}

	vector<string> statements;
	idx_t offset = 0;
};

static duckdb::unique_ptr<FunctionData> ReduceSQLBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sql");

	auto result = make_uniq<ReduceSQLFunctionData>();
	auto sql = input.inputs[0].ToString();
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		throw InvalidInputException("reduce_sql_statement requires a single statement as parameter");
	}
	auto &statement = *parser.statements[0];
	StatementSimplifier simplifier(statement, result->statements);
	simplifier.Simplify(statement);
	return std::move(result);
}

static void ReduceSQLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ReduceSQLFunctionData>();
	if (data.offset >= data.statements.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.statements.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.statements[data.offset++];
		output.data[0].SetValue(count, Value(entry));
		count++;
	}
	output.SetCardinality(count);
}

struct FuzzyDuckFunctionData : public TableFunctionData {
	FuzzyDuckFunctionData(ClientContext &context) : fuzzer(context) {
	}

	FuzzyDuck fuzzer;
	bool finished = false;
};

static duckdb::unique_ptr<FunctionData> FuzzyDuckBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FuzzyDuckFunctionData>(context);
	for (auto &kv : input.named_parameters) {
		if (kv.first == "seed") {
			result->fuzzer.seed = IntegerValue::Get(kv.second);
		} else if (kv.first == "max_queries") {
			result->fuzzer.max_queries = UBigIntValue::Get(kv.second);
		} else if (kv.first == "complete_log") {
			result->fuzzer.complete_log = StringValue::Get(kv.second);
		} else if (kv.first == "log") {
			result->fuzzer.log = StringValue::Get(kv.second);
		} else if (kv.first == "verbose_output") {
			result->fuzzer.verbose_output = BooleanValue::Get(kv.second);
		}
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void FuzzyDuckFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<FuzzyDuckFunctionData>();
	if (data.finished) {
		return;
	}

	data.fuzzer.Fuzz();
	data.finished = true;
}

static void FuzzAllFunctions(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<FuzzyDuckFunctionData>();
	if (data.finished) {
		return;
	}

	data.fuzzer.FuzzAllFunctions();
	data.finished = true;
}

void SqlsmithExtension::Load(DuckDB &db) {
	auto &db_instance = *db.instance;

	TableFunction sqlsmith_func("sqlsmith", {}, SQLSmithFunction, SQLSmithBind);
	sqlsmith_func.named_parameters["seed"] = LogicalType::INTEGER;
	sqlsmith_func.named_parameters["max_queries"] = LogicalType::UBIGINT;
	sqlsmith_func.named_parameters["exclude_catalog"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["dump_all_queries"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["dump_all_graphs"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["verbose_output"] = LogicalType::BOOLEAN;
	sqlsmith_func.named_parameters["complete_log"] = LogicalType::VARCHAR;
	sqlsmith_func.named_parameters["log"] = LogicalType::VARCHAR;
	ExtensionUtil::RegisterFunction(db_instance, sqlsmith_func);

	TableFunction fuzzy_duck_fun("fuzzyduck", {}, FuzzyDuckFunction, FuzzyDuckBind);
	fuzzy_duck_fun.named_parameters["seed"] = LogicalType::INTEGER;
	fuzzy_duck_fun.named_parameters["max_queries"] = LogicalType::UBIGINT;
	fuzzy_duck_fun.named_parameters["log"] = LogicalType::VARCHAR;
	fuzzy_duck_fun.named_parameters["complete_log"] = LogicalType::VARCHAR;
	fuzzy_duck_fun.named_parameters["verbose_output"] = LogicalType::BOOLEAN;
	ExtensionUtil::RegisterFunction(db_instance, fuzzy_duck_fun);

	TableFunction fuzz_all_functions("fuzz_all_functions", {}, FuzzAllFunctions, FuzzyDuckBind);
	fuzz_all_functions.named_parameters["seed"] = LogicalType::INTEGER;
	fuzz_all_functions.named_parameters["log"] = LogicalType::VARCHAR;
	fuzz_all_functions.named_parameters["complete_log"] = LogicalType::VARCHAR;
	fuzz_all_functions.named_parameters["verbose_output"] = LogicalType::BOOLEAN;
	ExtensionUtil::RegisterFunction(db_instance, fuzz_all_functions);

	TableFunction reduce_sql_function("reduce_sql_statement", {LogicalType::VARCHAR}, ReduceSQLFunction, ReduceSQLBind);
	ExtensionUtil::RegisterFunction(db_instance, reduce_sql_function);
}

std::string SqlsmithExtension::Name() {
	return "sqlsmith";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void sqlsmith_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SqlsmithExtension>();
}

DUCKDB_EXTENSION_API const char *sqlsmith_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
