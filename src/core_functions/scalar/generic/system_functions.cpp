#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/execution/expression_executor.hpp"

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

struct CurrentSchemasBindData : public FunctionData {
	explicit CurrentSchemasBindData(Value result_value) : result(std::move(result_value)) {
	}

	Value result;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentSchemasBindData>(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CurrentSchemasBindData>();
		return Value::NotDistinctFrom(result, other.result);
	}
};

static unique_ptr<FunctionData> CurrentSchemasBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::BOOLEAN) {
		throw BinderException("current_schemas requires a boolean input");
	}
	if (!arguments[0]->IsFoldable()) {
		throw NotImplementedException("current_schemas requires a constant input");
	}
	Value schema_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	Value result_val;
	if (schema_value.IsNull()) {
		// null
		result_val = Value(LogicalType::LIST(LogicalType::VARCHAR));
	} else {
		auto implicit_schemas = BooleanValue::Get(schema_value);
		vector<Value> schema_list;
		auto &catalog_search_path = ClientData::Get(context).catalog_search_path;
		auto &search_path = implicit_schemas ? catalog_search_path->Get() : catalog_search_path->GetSetPaths();
		std::transform(search_path.begin(), search_path.end(), std::back_inserter(schema_list),
		               [](const CatalogSearchEntry &s) -> Value { return Value(s.schema); });
		result_val = Value::LIST(LogicalType::VARCHAR, schema_list);
	}
	return make_uniq<CurrentSchemasBindData>(std::move(result_val));
}

// current_schemas
static void CurrentSchemasFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<CurrentSchemasBindData>();
	result.Reference(info.result);
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
	auto val = Value::UBIGINT(transaction.start_time);
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
	ScalarFunction current_schemas({LogicalType::BOOLEAN}, varchar_list_type, CurrentSchemasFunction,
	                               CurrentSchemasBind);
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
	ScalarFunction txid_current({}, LogicalType::UBIGINT, TransactionIdCurrent);
	txid_current.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return txid_current;
}

ScalarFunction VersionFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, VersionFunction);
}

} // namespace duckdb
