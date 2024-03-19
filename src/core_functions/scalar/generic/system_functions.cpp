#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

// current_query
static void CurrentQueryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(state.GetContext().GetCurrentQuery());
	result.Reference(val);
}

// current_schema
static void CurrentSchemaFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(ClientData::Get(state.GetContext()).catalog_search_path->GetDefault().schema);
	result.Reference(val);
}

// current_database
static void CurrentDatabaseFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(DatabaseManager::GetDefaultDatabase(state.GetContext()));
	result.Reference(val);
}

// current_schemas
static void CurrentSchemasFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	if (!input.AllConstant()) {
		throw NotImplementedException("current_schemas requires a constant input");
	}
	if (ConstantVector::IsNull(input.data[0])) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}
	auto implicit_schemas = *ConstantVector::GetData<bool>(input.data[0]);
	vector<Value> schema_list;
	auto &catalog_search_path = ClientData::Get(state.GetContext()).catalog_search_path;
	auto &search_path = implicit_schemas ? catalog_search_path->Get() : catalog_search_path->GetSetPaths();
	std::transform(search_path.begin(), search_path.end(), std::back_inserter(schema_list),
	               [](const CatalogSearchEntry &s) -> Value { return Value(s.schema); });

	auto val = Value::LIST(LogicalType::VARCHAR, schema_list);
	result.Reference(val);
}

// in_search_path
static void InSearchPathFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &search_path = ClientData::Get(context).catalog_search_path;
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    input.data[0], input.data[1], result, input.size(), [&](string_t db_name, string_t schema_name) {
		    return search_path->SchemaInSearchPath(context, db_name.GetString(), schema_name.GetString());
	    });
}

// txid_current
static void TransactionIdCurrent(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction = DuckTransaction::Get(context, catalog);
	auto val = Value::BIGINT(transaction.start_time);
	result.Reference(val);
}

// version
static void VersionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto val = Value(DuckDB::LibraryVersion());
	result.Reference(val);
}

ScalarFunction CurrentQueryFun::GetFunction() {
	ScalarFunction current_query({}, LogicalType::VARCHAR, CurrentQueryFunction);
	current_query.stability = FunctionStability::VOLATILE;
	return current_query;
}

ScalarFunction CurrentSchemaFun::GetFunction() {
	ScalarFunction current_schema({}, LogicalType::VARCHAR, CurrentSchemaFunction);
	current_schema.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return current_schema;
}

ScalarFunction CurrentDatabaseFun::GetFunction() {
	ScalarFunction current_database({}, LogicalType::VARCHAR, CurrentDatabaseFunction);
	current_database.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return current_database;
}

ScalarFunction CurrentSchemasFun::GetFunction() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunction current_schemas({LogicalType::BOOLEAN}, varchar_list_type, CurrentSchemasFunction);
	current_schemas.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return current_schemas;
}

ScalarFunction InSearchPathFun::GetFunction() {
	ScalarFunction in_search_path({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                              InSearchPathFunction);
	in_search_path.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return in_search_path;
}

ScalarFunction CurrentTransactionIdFun::GetFunction() {
	ScalarFunction txid_current({}, LogicalType::BIGINT, TransactionIdCurrent);
	txid_current.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return txid_current;
}

ScalarFunction VersionFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, VersionFunction);
}

} // namespace duckdb
