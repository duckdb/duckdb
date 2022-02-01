
#include "substrait-extension.hpp"
#include "to_substrait.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#endif

namespace duckdb {

struct GetSubstraitFunctionData : public TableFunctionData {
	GetSubstraitFunctionData() {
	}
	string query;
	bool finished = false;
};

static unique_ptr<FunctionData> GetSubstraitBind(ClientContext &context, vector<Value> &inputs,
                                                 named_parameter_map_t &named_parameters,
                                                 vector<LogicalType> &input_table_types,
                                                 vector<string> &input_table_names, vector<LogicalType> &return_types,
                                                 vector<string> &names) {
	auto result = make_unique<GetSubstraitFunctionData>();
	result->query = inputs[0].ToString();
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Success");
	return move(result);
}

static void SubFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                        DataChunk *input, DataChunk &output) {
	auto &data = (GetSubstraitFunctionData &)*bind_data;
	if (data.finished) {
		return;
	}
	output.SetCardinality(1);
	DuckDBToSubstrait transformer_d2s;
	auto new_conn = Connection(*context.db);
	auto query_plan = new_conn.context->ExtractPlan(data.query);

	transformer_d2s.TransformPlan(*query_plan);
	string serialized;
	transformer_d2s.SerializeToString(serialized);

	output.SetValue(0, 0, Value::BLOB_RAW(serialized));
	data.finished = true;
}

struct TPCHData : public FunctionOperatorData {
	TPCHData() : offset(0) {
	}
	idx_t offset;
};

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait binary from a valid SQL Query
	TableFunction sub_func("get_substrait", {LogicalType::VARCHAR}, SubFunction, GetSubstraitBind);
	//	sub_func.named_parameters["query"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo sub_info(sub_func);
	catalog.CreateTableFunction(*con.context, &sub_info);
	con.Commit();
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}