#include "catch.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "test_helpers.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_expanded_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_between_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_cast_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_comparison_expression.hpp"
#include <cmath>
#include <cstring>
#include "bound_expression_sql_export_test_helpers.hpp"

using namespace duckdb;

namespace bound_expression_sql_export_test {

template <int32_t OFFSET>
struct OffsetCastOperation {
	template <class SOURCE_TYPE, class TARGET_TYPE>
	static TARGET_TYPE Operation(SOURCE_TYPE input) {
		return static_cast<TARGET_TYPE>(input + OFFSET);
	}
};

static bool IntegerToBigintPlusOne(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	return VectorCastHelpers::TemplatedCastLoop<int32_t, int64_t, OffsetCastOperation<1>>(source, result, count,
	                                                                                      parameters);
}

TEST_CASE("Bound expression SQL export resolves columns only by binding", "[sql_export][bound_expression_sql_export]") {
	auto left_binding = ColumnBinding(TableIndex(10), ProjectionIndex(0));
	auto right_binding = ColumnBinding(TableIndex(11), ProjectionIndex(0));
	auto left = make_uniq<BoundColumnRefExpression>(Identifier("duplicate_alias"), LogicalType::INTEGER, left_binding);
	auto right =
	    make_uniq<BoundColumnRefExpression>(Identifier("duplicate_alias"), LogicalType::INTEGER, right_binding);
	auto comparison =
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHAN, std::move(left), std::move(right));
	BoundExpressionSQLExportContext context;
	context.resolve_binding = [=](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		if (binding == left_binding) {
			return ResolvedSQLColumnReference {{Identifier("src"), Identifier("select")}, LogicalType::INTEGER};
		}
		if (binding == right_binding) {
			return ResolvedSQLColumnReference {{Identifier("src"), Identifier("Mixed Case")}, LogicalType::INTEGER};
		}
		return {};
	};
	auto result = BoundExpressionSQLExporter::Export(*comparison, context);
	REQUIRE(result.IsSuccess());
	REQUIRE(result.GetValue()->GetExpressionClass() == ExpressionClass::COMPARISON);
	auto &parsed = result.GetValue()->Cast<ComparisonExpression>();
	REQUIRE(parsed.Left().Cast<ColumnRefExpression>().ColumnNames() ==
	        vector<Identifier> {Identifier("src"), Identifier("select")});
	REQUIRE(parsed.Right().Cast<ColumnRefExpression>().ColumnNames() ==
	        vector<Identifier> {Identifier("src"), Identifier("Mixed Case")});

	DuckDB db;
	Connection connection(db);
	RequireRoundTrip(connection, *comparison, context, " FROM (SELECT 11 AS \"select\", 22 AS \"Mixed Case\") AS src",
	                 "src.\"select\" < src.\"Mixed Case\"");

	BoundColumnRefExpression missing(Identifier("usable_alias"), LogicalType::INTEGER, left_binding);
	BoundExpressionSQLExportContext missing_context;
	auto missing_result = BoundExpressionSQLExporter::Export(missing, missing_context);
	LogicalPlanVerificationPath standalone_path;
	standalone_path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	RequireIssue(missing_result, LogicalPlanVerificationIssueCode::INVALID_BINDING, standalone_path);
	REQUIRE(missing_result.GetIssues()[0].facts.size() == 2);

	auto wrong_type = ResolveBinding(left_binding, {Identifier("src"), Identifier("select")}, LogicalType::BIGINT);
	auto mismatch = BoundExpressionSQLExporter::Export(missing, wrong_type);
	RequireIssue(mismatch, LogicalPlanVerificationIssueCode::TYPE_MISMATCH, standalone_path);
	auto &type_mismatch = *mismatch.GetIssues()[0].construct->type_mismatch;
	REQUIRE(type_mismatch.expected_type == LogicalType::BIGINT);
	REQUIRE(type_mismatch.actual_type == LogicalType::INTEGER);

	BoundColumnRefExpression correlated(LogicalType::INTEGER, left_binding, 1);
	auto correlated_result = BoundExpressionSQLExporter::Export(correlated, context);
	RequireIssue(correlated_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, standalone_path);
	REQUIRE(*correlated_result.GetIssues()[0].construct->identifier == "correlated_column_reference");
}

TEST_CASE("Bound expression SQL export composes deterministic expression paths",
          "[sql_export][bound_expression_sql_export]") {
	auto binding = ColumnBinding(TableIndex(20), ProjectionIndex(3));
	auto expression = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, binding));
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath root;
	root.root = LogicalPlanVerificationPathRoot::LOGICAL_PLAN;
	root.components.push_back(
	    LogicalPlanVerificationPathComponent {LogicalPlanVerificationPathComponentType::OPERATOR_CHILD, 2});
	root.components.push_back(
	    LogicalPlanVerificationPathComponent {LogicalPlanVerificationPathComponentType::OPERATOR_EXPRESSION, 4});
	auto result = BoundExpressionSQLExporter::ExportAtPath(*expression, context, root);
	REQUIRE(result.IsValid());
	REQUIRE(result.HasError());
	REQUIRE(result.GetIssues().size() == 2);
	for (idx_t child_index = 0; child_index < 2; child_index++) {
		auto expected = root;
		expected.components.push_back(LogicalPlanVerificationPathComponent {
		    LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD, child_index});
		REQUIRE(result.GetIssues()[child_index].path == optional<LogicalPlanVerificationPath>(expected));
	}
}

TEST_CASE("Bound expression SQL export handles default casts and exclusive BETWEEN",
          "[sql_export][bound_expression_sql_export]") {
	DBConfig config;
	config.options.load_extensions = false;
	DuckDB db(nullptr, &config);
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;

	auto default_cast = BoundCastExpression::AddDefaultCastToType(Constant(Value::INTEGER(42)), LogicalType::BIGINT);
	auto default_result = BoundExpressionSQLExporter::Export(*default_cast, context);
	REQUIRE(default_result.HasError());
	REQUIRE(default_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(*default_result.GetIssues()[0].construct->identifier == "default_cast_binding");
	BoundExpressionSQLExportContext default_context;
	default_context.client_context = connection.context.get();
	auto supported_default = BoundExpressionSQLExporter::Export(*default_cast, default_context);
	REQUIRE(supported_default.IsSuccess());
	REQUIRE(supported_default.GetValue()->Cast<CastExpression>().GetTargetType()->Equals(
	    *TypeExpression::FromLogicalType(LogicalType::BIGINT)));
	RequireRoundTrip(connection, *default_cast, default_context, string(), "CAST(42 AS BIGINT)");
	auto serialized_default = BinaryRoundTrip(*connection.context, *default_cast);
	REQUIRE(BoundCastExpression::IsDefaultCast(serialized_default->Cast<BoundFunctionExpression>()));
	RequireRoundTrip(connection, *serialized_default, default_context, string(), "CAST(42 AS BIGINT)");
	auto try_default =
	    BoundCastExpression::AddDefaultCastToType(Constant(Value("not an integer")), LogicalType::INTEGER, true);
	RequireRoundTrip(connection, *try_default, default_context, string(), "TRY_CAST('not an integer' AS INTEGER)");

	auto exclusive = BoundBetweenExpression::Create(Constant(Value::INTEGER(2)), Constant(Value::INTEGER(1)),
	                                                Constant(Value::INTEGER(3)), false, true);
	auto exclusive_result = BoundExpressionSQLExporter::Export(*exclusive, context);
	REQUIRE(exclusive_result.IsSuccess());
	REQUIRE(exclusive_result.GetValue()->GetExpressionClass() == ExpressionClass::CONJUNCTION);
	auto &exclusive_conjunction = exclusive_result.GetValue()->Cast<ConjunctionExpression>();
	REQUIRE(exclusive_conjunction.GetChildren().size() == 2);
	REQUIRE(exclusive_conjunction.GetChildren()[0]->GetExpressionType() == ExpressionType::COMPARE_GREATERTHAN);
	REQUIRE(exclusive_conjunction.GetChildren()[1]->GetExpressionType() == ExpressionType::COMPARE_LESSTHANOREQUALTO);

	connection.Rollback();
}

TEST_CASE("Bound expression SQL export reconstructs registered casts", "[sql_export][bound_expression_sql_export]") {
	BoundExpressionSQLExportContext context;
	DuckDB registered_db;
	Connection registered_connection(registered_db);
	ExtensionLoader loader(*registered_db.instance, "synthetic_cast_extension");
	loader.RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT, BoundCastInfo(IntegerToBigintPlusOne), 0);
	auto registered = BoundCastExpression::AddCastToType(*registered_connection.context, Constant(Value::INTEGER(7)),
	                                                     LogicalType::BIGINT);
	REQUIRE(ExpressionExecutor::EvaluateScalar(*registered_connection.context, *registered) == Value::BIGINT(8));
	auto exported = BoundExpressionSQLExporter::Export(*registered, context);
	REQUIRE(exported.IsSuccess());
	auto rebound = registered_connection.Query("SELECT " + exported.GetValue()->ToString());
	REQUIRE_FALSE(rebound->HasError());
	REQUIRE(rebound->GetTypes() == vector<LogicalType> {LogicalType::BIGINT});
	REQUIRE(rebound->GetValue(0, 0) == Value::BIGINT(8));

	context.client_context = registered_connection.context.get();
	auto default_direct = BoundCastExpression::AddDefaultCastToType(Constant(Value::INTEGER(7)), LogicalType::BIGINT);
	auto default_direct_result = BoundExpressionSQLExporter::Export(*default_direct, context);
	REQUIRE(default_direct_result.HasError());
	REQUIRE(*default_direct_result.GetIssues()[0].construct->identifier == "default_cast_binding");

	DuckDB unrelated_db;
	Connection unrelated_connection(unrelated_db);
	ExtensionLoader unrelated_loader(*unrelated_db.instance, "synthetic_unrelated_cast_extension");
	unrelated_loader.RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT,
	                                      BoundCastInfo(IntegerToBigintPlusOne), 0);
	BoundExpressionSQLExportContext unrelated_context;
	unrelated_context.client_context = unrelated_connection.context.get();
	auto default_unrelated = BoundCastExpression::AddDefaultCastToType(Constant(Value("7")), LogicalType::INTEGER);
	auto default_unrelated_result = BoundExpressionSQLExporter::Export(*default_unrelated, unrelated_context);
	REQUIRE(default_unrelated_result.HasError());
	REQUIRE(*default_unrelated_result.GetIssues()[0].construct->identifier == "default_cast_binding");
}

TEST_CASE("Bound expression SQL export rejects TRY around volatile children",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;

	auto random_plan = BindExportQuery(connection, "SELECT random()");
	auto random = FindExpression(*random_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION && expression.IsVolatile() &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "random";
	});
	REQUIRE(random);
	BoundOperatorExpression volatile_try(ExpressionType::OPERATOR_TRY, LogicalType::DOUBLE);
	volatile_try.GetChildrenMutable().push_back(random->Copy());
	auto volatile_result = BoundExpressionSQLExporter::Export(volatile_try, context);
	RequireIssue(volatile_result, LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE, path);
	REQUIRE(*volatile_result.GetIssues()[0].construct->identifier == "try_volatile_child");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export drops plan-local table filter hints",
          "[sql_export][bound_expression_sql_export]") {
	auto filter = CreateOptionalFilterExpression(
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHAN,
	                                      make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0),
	                                      make_uniq<BoundConstantExpression>(Value::INTEGER(10))),
	    LogicalType::INTEGER);
	BoundExpressionSQLExportContext standalone;
	REQUIRE(BoundExpressionSQLExporter::Export(*filter, standalone).HasError());
	BoundExpressionSQLExportContext plan_context;
	plan_context.discard_optimizer_metadata = true;
	auto exported = BoundExpressionSQLExporter::Export(*filter, plan_context);
	REQUIRE(exported.IsSuccess());
	REQUIRE(exported.GetValue()->GetExpressionClass() == ExpressionClass::CONSTANT);
	REQUIRE(exported.GetValue()->Equals(*ConstantExpression::Boolean(true)));
}

TEST_CASE("Bound expression SQL export rejects repeated volatile exclusive BETWEEN input",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();

	auto random_plan = OptimizeExportQuery(connection, "SELECT random()");
	auto random_expression = FindExpression(*random_plan, [](const Expression &expression) {
		return expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
		       expression.Cast<BoundFunctionExpression>().Function().GetName() == "random";
	});
	REQUIRE(random_expression);
	auto volatile_between = BoundBetweenExpression::Create(random_expression->Copy(), Constant(Value::DOUBLE(0)),
	                                                       Constant(Value::DOUBLE(1)), false, false);
	BoundExpressionSQLExportContext empty_context;
	auto volatile_result = BoundExpressionSQLExporter::Export(*volatile_between, empty_context);
	REQUIRE(volatile_result.HasError());
	REQUIRE(volatile_result.GetIssues()[0].code == LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPORT_FEATURE);
	REQUIRE(*volatile_result.GetIssues()[0].construct->identifier == "exclusive_between_input_evaluation");
	connection.Rollback();
}

TEST_CASE("Bound expression SQL export preserves structural operators through copies",
          "[sql_export][bound_expression_sql_export]") {
	DuckDB db;
	Connection connection(db);
	connection.BeginTransaction();
	BoundExpressionSQLExportContext context;
	vector<unique_ptr<Expression>> structural;
	structural.push_back(
	    BoundCastExpression::AddCastToType(*connection.context, Constant(Value::INTEGER(7)), LogicalType::BIGINT));
	structural.push_back(BoundBetweenExpression::Create(Constant(Value::INTEGER(7)), Constant(Value::INTEGER(2)),
	                                                    Constant(Value::INTEGER(9)), true, true));
	for (auto &expression : structural) {
		REQUIRE(BoundExpressionSQLExporter::Export(*expression->Copy(), context).IsSuccess());
		auto restored = BinaryRoundTrip(*connection.context, *expression);
		REQUIRE(BoundExpressionSQLExporter::Export(*restored, context).IsSuccess());
	}

	connection.Rollback();
}

TEST_CASE("Bound expression SQL export rejects deferred expression kinds",
          "[sql_export][bound_expression_sql_export]") {
	BoundExpressionSQLExportContext context;
	LogicalPlanVerificationPath path;
	path.root = LogicalPlanVerificationPathRoot::STANDALONE_EXPRESSION;
	vector<unique_ptr<Expression>> deferred;
	deferred.push_back(make_uniq<BoundDefaultExpression>(LogicalType::INTEGER));
	deferred.push_back(make_uniq<BoundParameterExpression>(Identifier("1")));
	deferred.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, storage_t(0)));
	deferred.push_back(make_uniq<BoundSubqueryExpression>(LogicalType::INTEGER));
	deferred.push_back(make_uniq<BoundWindowExpression>(LogicalType::INTEGER, nullptr, nullptr, nullptr));
	deferred.push_back(make_uniq<BoundUnnestExpression>(LogicalType::INTEGER));
	deferred.push_back(
	    make_uniq<BoundLambdaExpression>(ExpressionType::LAMBDA, LogicalType::INTEGER, Constant(Value::INTEGER(1)), 1));
	deferred.push_back(
	    make_uniq<BoundLambdaRefExpression>(LogicalType::INTEGER, ColumnBinding(TableIndex(1), ProjectionIndex(0)), 0));
	deferred.push_back(make_uniq<LegacyBoundCastExpression>(Constant(Value::INTEGER(1)), LogicalType::BIGINT, false));
	deferred.push_back(make_uniq<LegacyBoundComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, Constant(Value::INTEGER(1)), Constant(Value::INTEGER(1))));
	deferred.push_back(make_uniq<LegacyBoundBetweenExpression>(Constant(Value::INTEGER(1)), Constant(Value::INTEGER(0)),
	                                                           Constant(Value::INTEGER(2)), true, true));
	for (auto &expression : deferred) {
		RequireIssue(BoundExpressionSQLExporter::Export(*expression, context),
		             LogicalPlanVerificationIssueCode::UNSUPPORTED_EXPRESSION, path);
	}

	vector<unique_ptr<Expression>> expanded_children;
	expanded_children.push_back(Constant(Value::INTEGER(1)));
	BoundExpandedExpression expanded(std::move(expanded_children));
	RequireIssue(BoundExpressionSQLExporter::Export(expanded, context),
	             LogicalPlanVerificationIssueCode::INTERNAL_INVARIANT, path);
}

TEST_CASE("Bound expression SQL export owns outputs and propagates resolver exceptions",
          "[sql_export][bound_expression_sql_export]") {
	auto success = []() {
		BoundConstantExpression expression(Value::SMALLINT(9));
		BoundExpressionSQLExportContext context;
		return BoundExpressionSQLExporter::Export(expression, context);
	}();
	REQUIRE(success.IsSuccess());
	REQUIRE(success.GetValue()->Cast<CastExpression>().GetTargetType()->Equals(
	    *TypeExpression::FromLogicalType(LogicalType::SMALLINT)));

	auto failure = []() {
		BoundColumnRefExpression expression(LogicalType::INTEGER, ColumnBinding(TableIndex(88), ProjectionIndex(7)));
		BoundExpressionSQLExportContext context;
		return BoundExpressionSQLExporter::Export(expression, context);
	}();
	REQUIRE(failure.HasError());
	REQUIRE(failure.GetIssues()[0].facts.size() == 2);

	BoundColumnRefExpression expression(LogicalType::INTEGER, ColumnBinding(TableIndex(1), ProjectionIndex(0)));
	BoundExpressionSQLExportContext throwing_context;
	throwing_context.resolve_binding = [](const ColumnBinding &) -> optional<ResolvedSQLColumnReference> {
		throw InvalidInputException("synthetic resolver failure");
	};
	REQUIRE_THROWS_AS(BoundExpressionSQLExporter::Export(expression, throwing_context), InvalidInputException);
}

} // namespace bound_expression_sql_export_test
