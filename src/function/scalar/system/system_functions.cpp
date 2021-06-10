#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

// current_query
struct SystemBindData : public FunctionData {
	ClientContext &context;

	explicit SystemBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<SystemBindData>(context);
	}
};

unique_ptr<FunctionData> BindSystemFunction(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	return make_unique<SystemBindData>(context);
}

static void CurrentQueryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (SystemBindData &)*func_expr.bind_info;

	Value val(info.context.query);
	result.Reference(val);
}

// current_schema
static void CurrentSchemaFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(DEFAULT_SCHEMA);
	result.Reference(val);
}

// current_schemas
static void CurrentSchemasFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	vector<Value> schema_list = {Value(DEFAULT_SCHEMA)};
	auto val = Value::LIST(schema_list);
	result.Reference(val);
}

// txid_current
static void TransactionIdCurrent(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (SystemBindData &)*func_expr.bind_info;

	auto &transaction = Transaction::GetTransaction(info.context);
	auto val = Value::BIGINT(transaction.start_time);
	result.Reference(val);
}

// version
static void VersionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto val = Value(DuckDB::LibraryVersion());
	result.Reference(val);
}

void SystemFun::RegisterFunction(BuiltinFunctions &set) {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);

	set.AddFunction(
	    ScalarFunction("current_query", {}, LogicalType::VARCHAR, CurrentQueryFunction, false, BindSystemFunction));
	set.AddFunction(ScalarFunction("current_schema", {}, LogicalType::VARCHAR, CurrentSchemaFunction));
	set.AddFunction(
	    ScalarFunction("current_schemas", {LogicalType::BOOLEAN}, varchar_list_type, CurrentSchemasFunction));
	set.AddFunction(
	    ScalarFunction("txid_current", {}, LogicalType::BIGINT, TransactionIdCurrent, false, BindSystemFunction));
	set.AddFunction(ScalarFunction("version", {}, LogicalType::VARCHAR, VersionFunction));
}

} // namespace duckdb
