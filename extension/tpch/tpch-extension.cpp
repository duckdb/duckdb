#include "tpch-extension.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"

#include "dbgen.hpp"

namespace duckdb {

struct DBGenFunctionData : public TableFunctionData {
	DBGenFunctionData() {
	}

	bool finished = false;
	double sf = 0;
	string schema = DEFAULT_SCHEMA;
	string suffix;
	bool overwrite = false;
};

static unique_ptr<FunctionData> dbgen_bind(ClientContext &context, vector<Value> &inputs,
                                           unordered_map<string, Value> &named_parameters,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<DBGenFunctionData>();
	for (auto &kv : named_parameters) {
		if (kv.first == "sf") {
			result->sf = kv.second.value_.double_;
		} else if (kv.first == "schema") {
			result->schema = kv.second.str_value;
		} else if (kv.first == "suffix") {
			result->suffix = kv.second.str_value;
		} else if (kv.first == "overwrite") {
			result->overwrite = kv.second.value_.boolean;
		}
	}
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	return move(result);
}

static void dbgen_function(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                           DataChunk &output) {
	auto &data = (DBGenFunctionData &)*bind_data;
	if (data.finished) {
		return;
	}
	tpch::DBGenWrapper::CreateTPCHSchema(context, data.schema, data.suffix);
	tpch::DBGenWrapper::LoadTPCHData(context, data.sf, data.schema, data.suffix);

	data.finished = true;
}
static string pragma_tpch_query(ClientContext &context, vector<Value> parameters) {
	auto index = parameters[0].GetValue<int32_t>();
	return tpch::DBGenWrapper::GetQuery(index);
}

void TPCHExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	TableFunction dbgen_func("dbgen", {}, dbgen_function, dbgen_bind);
	dbgen_func.named_parameters["sf"] = LogicalType::DOUBLE;
	dbgen_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	dbgen_func.named_parameters["schema"] = LogicalType::VARCHAR;
	dbgen_func.named_parameters["suffix"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo dbgen_info(dbgen_func);

	// create the dbgen function
	db.catalog->CreateTableFunction(*con.context, &dbgen_info);

	// create the TPCH pragma that allows us to run the query
	auto tpch_func = PragmaFunction::PragmaCall("tpch", pragma_tpch_query, {LogicalType::BIGINT});

	CreatePragmaFunctionInfo info(tpch_func);
	db.catalog->CreatePragmaFunction(*con.context, &info);

	con.Commit();
}

std::string TPCHExtension::GetQuery(int query) {
	return tpch::DBGenWrapper::GetQuery(query);
}

std::string TPCHExtension::GetAnswer(double sf, int query) {
	return tpch::DBGenWrapper::GetAnswer(sf, query);
}

} // namespace duckdb
