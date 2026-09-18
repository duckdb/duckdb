#include "sql_export_test_helpers.hpp"
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_plan_sql_exporter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include <stdexcept>
#include <type_traits>
#include "logical_plan_sql_export_test_helpers.hpp"

using namespace duckdb;

namespace logical_plan_sql_export_test {

static void RequireMarkConditionRejection(const LogicalPlanVerificationResult<LogicalPlanSQLExportRelation> &result) {
	REQUIRE(result.HasError());
	REQUIRE(result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(result.GetIssues()[0].construct ==
	        LogicalPlanVerificationConstructIdentity::ExportFeature("mark_condition_semantics"));
}

TEST_CASE("SQL export inlines plain join sources without alias capture", "[sql_export][logical_plan_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (auto aliases : {pair<string, string> {"v", "w"}, {"v", "v"}, {"r1", "v"}, {"v", "r0"}, {"r1", "r0"}}) {
		for (bool filtered : {false, true}) {
			if (filtered && (aliases.first != "v" || aliases.second == "v")) {
				continue;
			}
			CAPTURE(aliases.first, aliases.second, filtered);
			auto leaf = [](const string &name, idx_t index) {
				return make_uniq<SQLExportExtensionOperator>(
				    name, vector<ColumnBinding> {{TableIndex(index), ProjectionIndex(0)}},
				    vector<LogicalType> {LogicalType::INTEGER}, vector<TableIndex> {TableIndex(index)});
			};
			auto plan = make_uniq<LogicalCrossProduct>(leaf("left", 800), leaf("right", 801));
			plan->ResolveOperatorTypes();
			auto options = PlanResolverOptions([&](const LogicalPlanSQLExportExtensionInput &input) {
				auto is_left = input.op.GetExtensionName() == "left";
				auto alias = is_left ? aliases.first : aliases.second;
				auto values = is_left ? "(VALUES(1),(2))" : "(VALUES(10),(20))";
				auto sql = "SELECT " + alias + ".x FROM " + values + " " + alias + "(x)";
				if (is_left && filtered) {
					sql += " WHERE " + alias + ".x=1";
				}
				Parser parser;
				parser.ParseQuery(sql);
				return LogicalPlanSQLExportExtensionResult::Exported(
				    std::move(parser.statements[0]->Cast<SelectStatement>().node));
			});
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *plan, options);
			INFO((exported.HasError() ? exported.GetIssues()[0].message : string()));
			REQUIRE(exported.IsSuccess());
			if (aliases.first == "v" && aliases.second == "w" && !filtered) {
				auto &join = exported.GetValue().query->Cast<SelectNode>().from_table->Cast<JoinRef>();
				REQUIRE(join.left->alias == Identifier("v"));
				REQUIRE(join.right->alias == Identifier("w"));
			}
			auto sql = exported.GetValue().query->ToString() + " ORDER BY 1,2";
			CAPTURE(sql);
			auto generated = connection.Query(sql);
			REQUIRE_NO_FAIL(*generated);
			if (filtered) {
				REQUIRE(CHECK_COLUMN(generated, 0, {1, 1}));
				REQUIRE(CHECK_COLUMN(generated, 1, {10, 20}));
			} else {
				REQUIRE(CHECK_COLUMN(generated, 0, {1, 1, 2, 2}));
				REQUIRE(CHECK_COLUMN(generated, 1, {10, 20, 10, 20}));
			}
		}
	}
	connection.Rollback();
}

TEST_CASE("Logical plan SQL export preserves join output maps and empty sides",
          "[sql_export][logical_plan_sql_export][join_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	for (auto type : {JoinType::INNER, JoinType::LEFT, JoinType::RIGHT, JoinType::OUTER, JoinType::SEMI, JoinType::ANTI,
	                  JoinType::RIGHT_SEMI, JoinType::RIGHT_ANTI, JoinType::MARK, JoinType::SINGLE}) {
		for (idx_t empty_mask : {0, 1, 2, 3}) {
			CAPTURE(type, empty_mask);
			unique_ptr<LogicalOperator> left = IntegerValues(TableIndex(1010), {{1, 10}, {2, 20}, {2, 21}});
			unique_ptr<LogicalOperator> right = IntegerValues(TableIndex(1011), {{2, 100}, {3, 200}});
			if (empty_mask & 1) {
				left = make_uniq<LogicalEmptyResult>(std::move(left));
			}
			if (empty_mask & 2) {
				right = make_uniq<LogicalEmptyResult>(std::move(right));
			}
			auto join = make_uniq<LogicalComparisonJoin>(type);
			join->mark_index = TableIndex(1012);
			join->left_projection_map = {ProjectionIndex(1)};
			join->right_projection_map = {ProjectionIndex(1), ProjectionIndex(0)};
			join->conditions.emplace_back(
			    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
			                                        ColumnBinding(TableIndex(1010), ProjectionIndex(0))),
			    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
			                                        ColumnBinding(TableIndex(1011), ProjectionIndex(0))),
			    ExpressionType::COMPARE_EQUAL);
			join->children.push_back(std::move(left));
			join->children.push_back(std::move(right));
			join->ResolveOperatorTypes();
			auto exported = LogicalPlanSQLExporter::Export(*connection.context, *join);
			REQUIRE(exported.IsSuccess());
			REQUIRE_NO_FAIL(connection.Query("PRAGMA disable_optimizer"));
			auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(join)));
			REQUIRE_NO_FAIL(connection.Query("PRAGMA enable_optimizer"));
			auto generated = connection.Query(exported.GetValue().query->ToString());
			REQUIRE_NO_FAIL(*direct);
			REQUIRE_NO_FAIL(*generated);
			REQUIRE(generated->GetTypes() == direct->GetTypes());
			REQUIRE(SQLExportRows(*generated, false) == SQLExportRows(*direct, false));
		}
	}
	connection.Rollback();
}

TEST_CASE("Grouped MARK SQL export rejects unsupported conjunction semantics",
          "[sql_export][logical_plan_sql_export][join_sql_export]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET max_execution_time=5000"));
	connection.BeginTransaction();
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE list_l(g INTEGER,x INTEGER[]); "
	                                 "CREATE TABLE list_r(g INTEGER,y INTEGER[]); "
	                                 "INSERT INTO list_l VALUES (1,[10,NULL]); INSERT INTO list_r VALUES (1,NULL); "
	                                 "CREATE SEQUENCE mark_copy_guard"));
	auto plan = OptimizeLogicalPlanExportQuery(connection, "SELECT nextval('mark_copy_guard'),"
	                                                       "x=ANY(SELECT y FROM list_r r WHERE r.g=l.g) FROM list_l l");
	auto copied = plan->Copy(*connection.context);
	copied->ResolveOperatorTypes();
	RequireMarkConditionRejection(LogicalPlanSQLExporter::Export(*connection.context, *copied));
	auto sequence = connection.Query("SELECT last_value FROM duckdb_sequences() WHERE sequence_name='mark_copy_guard'");
	REQUIRE_NO_FAIL(*sequence);
	REQUIRE(sequence->GetValue(0, 0).IsNull());
	connection.Rollback();
}

} // namespace logical_plan_sql_export_test
