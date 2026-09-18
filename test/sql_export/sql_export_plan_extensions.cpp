#include "sql_export_test_helpers.hpp"
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include <stdexcept>
#include <type_traits>
#include "logical_plan_sql_export_test_helpers.hpp"

using namespace duckdb;

namespace logical_plan_sql_export_test {

static unique_ptr<ParsedExpression> PlanParsedInteger(int32_t value, const Identifier &alias) {
	auto result = ConstantExpression::FromValue(Value::INTEGER(value));
	result->SetAlias(alias);
	return result;
}

static unique_ptr<QueryNode> PlanConstantRelation(const LogicalPlanSQLExportExtensionInput &input, int32_t value) {
	auto select = make_uniq<SelectNode>();
	for (idx_t i = 0; i < input.op.types.size(); i++) {
		select->select_list.push_back(PlanParsedInteger(value, Identifier()));
	}
	return std::move(select);
}

class SQLExportOperatorExtension : public OperatorExtension {
public:
	SQLExportOperatorExtension(string name_p, logical_plan_sql_export_t callback_p)
	    : name(std::move(name_p)), callback(std::move(callback_p)) {
		Bind = nullptr;
	}

	std::string GetName() override {
		return name;
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		return nullptr;
	}

	LogicalPlanSQLExportExtensionResult ExportLogicalPlanSQL(const LogicalPlanSQLExportExtensionInput &input) override {
		return callback(input);
	}

private:
	string name;
	logical_plan_sql_export_t callback;
};

class LegacyOperatorExtension : public OperatorExtension {
public:
	LegacyOperatorExtension() {
		Bind = nullptr;
	}

	std::string GetName() override {
		return "legacy_sql_export_test";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		return nullptr;
	}
};

static unique_ptr<SQLExportExtensionOperator> LeafExtension(const string &name, TableIndex table_index) {
	auto binding = ColumnBinding(table_index, ProjectionIndex(0));
	return make_uniq<SQLExportExtensionOperator>(name, vector<ColumnBinding> {binding},
	                                             vector<LogicalType> {LogicalType::INTEGER},
	                                             vector<TableIndex> {table_index});
}

static unique_ptr<SQLExportExtensionOperator> ChildExtension(const string &name, TableIndex child_index) {
	auto binding = ColumnBinding(child_index, ProjectionIndex(0));
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(
	    make_uniq<BoundColumnRefExpression>(Identifier("display_name"), LogicalType::INTEGER, binding));
	auto result = make_uniq<SQLExportExtensionOperator>(name, vector<ColumnBinding> {binding},
	                                                    vector<LogicalType> {LogicalType::INTEGER},
	                                                    vector<TableIndex> {}, std::move(expressions));
	result->children.push_back(IntegerValues(child_index, {{42}}));
	return result;
}

static unique_ptr<QueryNode> PlanChildRelation(const LogicalPlanSQLExportExtensionInput &input) {
	if (input.children.size() != 1 || input.expression_count != 1) {
		throw InternalException("Unexpected synthetic extension export input");
	}
	auto expression = input.export_expression(0);
	if (expression.HasError()) {
		throw InternalException("Synthetic extension expression export failed");
	}
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(std::move(expression.GetValue()));
	select->from_table = std::move(input.children[0].table);
	return std::move(select);
}

TEST_CASE("Logical plan SQL export supports local and registered extension callbacks",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);

	SECTION("local decline then registered child export") {
		vector<string> calls;
		optional_ptr<TableRef> child_table;
		auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
			child_table = input.children[0].table.get();
			calls.push_back("local");
			return LogicalPlanSQLExportExtensionResult::NotHandled();
		});
		OperatorExtension::Register(DBConfig::GetConfig(*db.instance),
		                            make_shared_ptr<SQLExportOperatorExtension>(
		                                "child_extension", [&](const LogicalPlanSQLExportExtensionInput &input) {
			                                calls.push_back("registered");
			                                REQUIRE(input.children[0].table.get() == child_table.get());
			                                return LogicalPlanSQLExportExtensionResult::Exported(
			                                    PlanChildRelation(input));
		                                }));
		auto plan = ChildExtension("child_extension", TableIndex(80));
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		REQUIRE(result.IsSuccess());
		REQUIRE(result.GetValue().query->Cast<SelectNode>().from_table.get() == child_table.get());
		REQUIRE(calls == vector<string> {"local", "registered"});
		REQUIRE(connection.Query(result.GetValue().query->ToString())->GetValue(0, 0) == Value::INTEGER(42));
	}
	SECTION("local terminal rejection") {
		idx_t calls = 0;
		OperatorExtension::Register(DBConfig::GetConfig(*db.instance),
		                            make_shared_ptr<SQLExportOperatorExtension>(
		                                "terminal_extension", [&](const LogicalPlanSQLExportExtensionInput &) {
			                                calls++;
			                                return LogicalPlanSQLExportExtensionResult::NotHandled();
		                                }));
		auto options = PlanResolverOptions([](const LogicalPlanSQLExportExtensionInput &) {
			return LogicalPlanSQLExportExtensionResult::Unsupported("Unavailable");
		});
		auto plan = LeafExtension("terminal_extension", TableIndex(81));
		RequirePlanExportIssue(LogicalPlanSQLExporter::Export(*connection.context, *plan, options),
		                       LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION);
		REQUIRE(calls == 0);
	}
	SECTION("no handler") {
		auto plan = LeafExtension("unhandled_extension", TableIndex(82));
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan);
		RequirePlanExportIssue(result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION);
	}

	SECTION("child failure bypasses resolver") {
		idx_t calls = 0;
		auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &) {
			calls++;
			return LogicalPlanSQLExportExtensionResult::NotHandled();
		});
		auto plan = ChildExtension("child_failure", TableIndex(83));
		plan->children[0]->Cast<LogicalExpressionGet>().expressions[0][0] =
		    make_uniq<BoundDefaultExpression>(LogicalType::INTEGER);
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		RequireLogicalPlanExportIssue(
		    result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION,
		    LogicalPlanVerificationPhase::EXPRESSION_EXPORT,
		    LogicalPlanVerificationPath {LogicalPlanVerificationPathRoot::LOGICAL_PLAN,
		                                 {{LogicalPlanVerificationPathComponentType::OPERATOR_CHILD, 0},
		                                  {LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION, 0}}});
		REQUIRE(calls == 0);
	}
}

TEST_CASE("Logical plan SQL export transfers positional children into owned extension SQL",
          "[sql_export][logical_plan_sql_export]") {
	optional<LogicalPlanSQLExportRelation> owned_relation;
	string exported_sql;
	{
		DuckDB db;
		Connection connection(db);
		vector<ColumnBinding> bindings;
		vector<unique_ptr<Expression>> expressions;
		for (idx_t i = 0; i < 2; i++) {
			bindings.emplace_back(TableIndex(85), ProjectionIndex(i));
			expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(Identifier("same"), LogicalType::INTEGER, bindings.back()));
		}
		auto plan = make_uniq<SQLExportExtensionOperator>("positional_child", bindings,
		                                                  vector<LogicalType>(2, LogicalType::INTEGER),
		                                                  vector<TableIndex> {}, std::move(expressions));
		plan->children.push_back(IntegerValues(TableIndex(85), {{10, 20}, {10, 20}}));
		optional_ptr<TableRef> child_table;
		auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
			auto select = make_uniq<SelectNode>();
			child_table = input.children[0].table.get();
			select->from_table = std::move(input.children[0].table);
			// Expression bindings remain available after taking the child.
			for (idx_t ordinal : {idx_t(1), idx_t(0)}) {
				auto expression = input.export_expression(ordinal);
				REQUIRE(expression.IsSuccess());
				select->select_list.push_back(std::move(expression.GetValue()));
			}
			REQUIRE_FALSE(input.children[0].table);
			return LogicalPlanSQLExportExtensionResult::Exported(std::move(select));
		});
		auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		REQUIRE(result.IsSuccess());
		REQUIRE(result.GetValue().query->Cast<SelectNode>().from_table.get() == child_table.get());
		owned_relation = std::move(result.GetValue());
		exported_sql = owned_relation->query->ToString();
	}
	REQUIRE(owned_relation);
	REQUIRE(owned_relation->query->ToString() == exported_sql);
	DuckDB target_db;
	Connection target_connection(target_db);
	auto rows = target_connection.Query(exported_sql);
	REQUIRE_NO_FAIL(*rows);
	REQUIRE(rows->RowCount() == 2);
	for (idx_t row = 0; row < 2; row++) {
		REQUIRE(rows->GetValue(0, row) == Value::INTEGER(20));
		REQUIRE(rows->GetValue(1, row) == Value::INTEGER(10));
	}
}

TEST_CASE("Logical plan SQL export gives invocation resolvers precedence over registration",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	idx_t registered_calls = 0;
	auto registered = make_shared_ptr<SQLExportOperatorExtension>(
	    "precedence_extension", [&](const LogicalPlanSQLExportExtensionInput &input) {
		    registered_calls++;
		    return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 99));
	    });
	OperatorExtension::Register(DBConfig::GetConfig(*db.instance), registered);
	Connection connection(db);
	auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
		return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 42));
	});
	auto plan = LeafExtension("precedence_extension", TableIndex(90));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
	REQUIRE(result.IsSuccess());
	REQUIRE(registered_calls == 0);
	REQUIRE(connection.Query(result.GetValue().query->ToString())->GetValue(0, 0) == Value::INTEGER(42));
}

TEST_CASE("Logical plan SQL export uses the matching registered extension callback",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	auto registered = make_shared_ptr<SQLExportOperatorExtension>(
	    "registered_extension", [&](const LogicalPlanSQLExportExtensionInput &input) {
		    return LogicalPlanSQLExportExtensionResult::Exported(PlanConstantRelation(input, 84));
	    });
	OperatorExtension::Register(DBConfig::GetConfig(*db.instance), registered);
	Connection connection(db);
	auto plan = LeafExtension("registered_extension", TableIndex(91));

	auto result = LogicalPlanSQLExporter::Export(*connection.context, *plan);
	REQUIRE(result.IsSuccess());
	REQUIRE(connection.Query(result.GetValue().query->ToString())->GetValue(0, 0) == Value::INTEGER(84));

	OperatorExtension::Register(DBConfig::GetConfig(*db.instance), make_shared_ptr<LegacyOperatorExtension>());
	auto legacy_plan = LeafExtension("legacy_sql_export_test", TableIndex(92));
	auto legacy_result = LogicalPlanSQLExporter::Export(*connection.context, *legacy_plan);
	RequirePlanExportIssue(legacy_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXTENSION);
}

TEST_CASE("Logical plan SQL export propagates callback exceptions unchanged", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);

	SECTION("runtime exception") {
		auto plan = LeafExtension("throwing_extension", TableIndex(120));
		auto options =
		    PlanResolverOptions([](const LogicalPlanSQLExportExtensionInput &) -> LogicalPlanSQLExportExtensionResult {
			    throw std::runtime_error("synthetic callback failure");
		    });
		REQUIRE_THROWS_AS(LogicalPlanSQLExporter::Export(*connection.context, *plan, options), std::runtime_error);
	}
}

TEST_CASE("Logical plan SQL export isolates extension column names and scope modifiers",
          "[sql_export][logical_plan_sql_export]") {
	DuckDB db;
	Connection connection(db);
	for (const auto &sql :
	     {"SELECT 42, 43", "SELECT 42 AS same, 43 AS same", "SELECT 42 AS \"odd.name\", 43 AS \"a b\"",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(1,2),(3,4)) v(a,b)",
	      "SELECT DISTINCT v.a, v.b FROM (VALUES (1,2),(1,2),(3,4)) v(a,b)",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(3,4)) v(a,b) ORDER BY v.a DESC LIMIT 1",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(3,4)) v(a,b) LIMIT 0",
	      "SELECT v.a, v.b FROM (VALUES (1,2),(3,4)) v(a,b) QUALIFY row_number() OVER ()=1",
	      "WITH v(a,b) AS (VALUES (11,12)) SELECT v.a,v.b FROM v"}) {
		INFO(sql);
		auto leaf = make_uniq<SQLExportExtensionOperator>(
		    "positional",
		    vector<ColumnBinding> {{TableIndex(800), ProjectionIndex(0)}, {TableIndex(800), ProjectionIndex(1)}},
		    vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER}, vector<TableIndex> {TableIndex(800)});
		vector<unique_ptr<Expression>> expressions;
		for (auto index : {1, 0, 1}) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    LogicalType::INTEGER, ColumnBinding(TableIndex(800), ProjectionIndex(index))));
		}
		auto plan = PlanProjection(TableIndex(801), std::move(leaf), std::move(expressions));
		auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &) {
			Parser parser;
			parser.ParseQuery(sql);
			return LogicalPlanSQLExportExtensionResult::Exported(
			    std::move(parser.statements[0]->Cast<SelectStatement>().node));
		});
		auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
		REQUIRE(exported.IsSuccess());
		REQUIRE(exported.GetValue().fields.size() == 3);
		auto direct = connection.Query("SELECT q.b,q.a,q.b FROM (" + string(sql) + ") q(a,b) ORDER BY 1,2,3");
		auto result = connection.Query(exported.GetValue().query->ToString() + " ORDER BY 1,2,3");
		REQUIRE_FALSE(direct->HasError());
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->GetTypes() == direct->GetTypes());
		REQUIRE(result->Equals(*direct, false));
		if (string(sql).find("WITH") != string::npos) {
			REQUIRE(exported.GetValue().query->Cast<SelectNode>().from_table->type == TableReferenceType::SUBQUERY);
		}
	}
}

} // namespace logical_plan_sql_export_test
