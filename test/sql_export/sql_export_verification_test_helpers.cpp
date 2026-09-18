#include "sql_export_verification_test_helpers.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace sql_export_verification_test {

SQLExportVerificationRecord TakeSQLExportRecord(SQLExportVerificationState &observer) {
	auto records = observer.TakeRecords();
	REQUIRE(records.size() == 1);
	return std::move(records[0]);
}

void SetSQLExportMode(Connection &con, SQLExportVerificationState &observer, const string &mode) {
	REQUIRE_NO_FAIL(con.Query("SET debug_verify_sql_export='" + mode + "'"));
	observer.TakeRecords();
}

void RegisterSQLExportOpaqueSource(DuckDB &db, Connection &con) {
	ExtensionLoader loader(*db.instance, "sql_export_opaque_source");
	auto &range = loader.GetTableFunction("range");
	auto function = *range.functions.GetFunctionByArguments(*con.context, {LogicalType::BIGINT});
	function.name = Identifier("sql_export_opaque_source");
	function.to_sql = [](ClientContext &, const LogicalGet &, TableFunctionToSQLInput) -> TableFunctionToSQLResult {
		return {nullptr, "test_opaque"};
	};
	loader.RegisterFunction(std::move(function));
}

void SQLExportChangeGeneratedSchema(PlannerExtensionInput &input, BoundStatement &statement) {
	auto &info = static_cast<SQLExportBindHook &>(*input.info);
	if (++info.calls != 2) {
		return;
	}
	if (info.throw_on_second) {
		throw BinderException("SQL export bind hook exception");
	}
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(99)));
	auto projection = make_uniq<LogicalProjection>(input.binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(make_uniq<LogicalDummyScan>(input.binder.GenerateTableIndex()));
	statement.plan = std::move(projection);
	statement.names = {"generated"};
	statement.types = {LogicalType::BIGINT};
}

} // namespace sql_export_verification_test
