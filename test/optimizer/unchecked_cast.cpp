#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/planner.hpp"

using namespace duckdb;

namespace {

struct CastCounts {
	idx_t checked = 0;
	idx_t unchecked = 0;
};

static BoundCastInfo GetDefaultCast(const LogicalType &source, const LogicalType &target) {
	CastFunctionSet function_set;
	GetCastFunctionInput input;
	return function_set.GetCastFunction(source, target, input);
}

static unique_ptr<LogicalOperator> OptimizeQuery(Connection &connection, const string &query) {
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(query);
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	Optimizer optimizer(*planner.binder, *connection.context);
	return optimizer.Optimize(std::move(planner.plan));
}

static void CountExpressionCasts(const Expression &expr, CastCounts &counts) {
	if (BoundCastExpression::IsCast(expr)) {
		auto &cast = expr.Cast<BoundFunctionExpression>();
		if (BoundCastExpression::GetBoundCast(cast).IsUnchecked()) {
			counts.unchecked++;
		} else {
			counts.checked++;
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { CountExpressionCasts(child, counts); });
}

static CastCounts CountPlanCasts(const LogicalOperator &op) {
	CastCounts result;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](const unique_ptr<Expression> *expr) { CountExpressionCasts(**expr, result); });
	for (auto &child : op.children) {
		auto child_counts = CountPlanCasts(*child);
		result.checked += child_counts.checked;
		result.unchecked += child_counts.unchecked;
	}
	return result;
}

static optional_ptr<const BoundFunctionExpression> FindCast(const LogicalOperator &op) {
	optional_ptr<const BoundFunctionExpression> result;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expr) {
		ExpressionIterator::VisitExpression<BoundFunctionExpression>(
		    **expr, [&](const BoundFunctionExpression &function) {
			    if (!result && BoundCastExpression::IsCast(function)) {
				    result = function;
			    }
		    });
	});
	if (result) {
		return result;
	}
	for (auto &child : op.children) {
		result = FindCast(*child);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

} // namespace

TEST_CASE("Unchecked casts are only available for native integers", "[optimizer][cast]") {
	auto native_cast = GetDefaultCast(LogicalType::BIGINT, LogicalType::TINYINT);
	REQUIRE(!native_cast.IsUnchecked());
	REQUIRE(native_cast.TrySetUnchecked());
	REQUIRE(native_cast.IsUnchecked());

	auto native_copy = native_cast.Copy();
	REQUIRE(native_copy.IsUnchecked());
	REQUIRE(native_copy.TrySetUnchecked());
	native_copy.SetFunction(DefaultCasts::NopCast);
	REQUIRE(!native_copy.IsUnchecked());
	REQUIRE(!native_copy.TrySetUnchecked());

	auto signed_unsigned_cast = GetDefaultCast(LogicalType::INTEGER, LogicalType::UINTEGER);
	REQUIRE(signed_unsigned_cast.TrySetUnchecked());

	auto boolean_cast = GetDefaultCast(LogicalType::INTEGER, LogicalType::BOOLEAN);
	REQUIRE(boolean_cast.TrySetUnchecked());

	auto hugeint_cast = GetDefaultCast(LogicalType::HUGEINT, LogicalType::TINYINT);
	REQUIRE(!hugeint_cast.TrySetUnchecked());
	auto to_hugeint_cast = GetDefaultCast(LogicalType::TINYINT, LogicalType::HUGEINT);
	REQUIRE(!to_hugeint_cast.TrySetUnchecked());
	auto floating_cast = GetDefaultCast(LogicalType::DOUBLE, LogicalType::TINYINT);
	REQUIRE(!floating_cast.TrySetUnchecked());
	auto to_floating_cast = GetDefaultCast(LogicalType::INTEGER, LogicalType::DOUBLE);
	REQUIRE(!to_floating_cast.TrySetUnchecked());
}

TEST_CASE("Unchecked casts preserve vector encodings and validity", "[optimizer][cast]") {
	auto cast = GetDefaultCast(LogicalType::BIGINT, LogicalType::TINYINT);
	REQUIRE(cast.TrySetUnchecked());

	Vector flat(LogicalType::BIGINT, 4);
	auto flat_data = FlatVector::GetDataMutable<int64_t>(flat);
	flat_data[0] = -128;
	flat_data[1] = 0;
	flat_data[2] = 42;
	flat_data[3] = 127;
	FlatVector::ValidityMutable(flat).SetInvalid(2);
	Vector flat_result(LogicalType::TINYINT, 4);
	CastParameters parameters;
	REQUIRE(cast.Cast(flat, flat_result, 4, parameters));
	REQUIRE(flat_result.GetValue(0) == Value::TINYINT(-128));
	REQUIRE(flat_result.GetValue(1) == Value::TINYINT(0));
	REQUIRE(flat_result.GetValue(2).IsNull());
	REQUIRE(flat_result.GetValue(3) == Value::TINYINT(127));

	SelectionVector selection(4);
	selection.set_index(0, 3);
	selection.set_index(1, 2);
	selection.set_index(2, 1);
	selection.set_index(3, 0);
	Vector dictionary(LogicalType::BIGINT, 4);
	dictionary.Slice(flat, selection, 4);
	Vector dictionary_result(LogicalType::TINYINT, 4);
	REQUIRE(cast.Cast(dictionary, dictionary_result, 4, parameters));
	REQUIRE(dictionary_result.GetValue(0) == Value::TINYINT(127));
	REQUIRE(dictionary_result.GetValue(1).IsNull());
	REQUIRE(dictionary_result.GetValue(2) == Value::TINYINT(0));
	REQUIRE(dictionary_result.GetValue(3) == Value::TINYINT(-128));

	Vector constant(Value::BIGINT(127), count_t(4));
	Vector constant_result(LogicalType::TINYINT, 4);
	REQUIRE(cast.Cast(constant, constant_result, 4, parameters));
	REQUIRE(constant_result.GetVectorType() == VectorType::CONSTANT_VECTOR);
	REQUIRE(constant_result.GetValue(0) == Value::TINYINT(127));
}

TEST_CASE("Statistics select unchecked casts only for proven integer ranges", "[optimizer][cast]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE safe_values AS SELECT i::BIGINT i FROM range(128) t(i)"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE unsafe_values AS SELECT i::BIGINT i FROM range(256) t(i)"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE huge_values AS SELECT i::HUGEINT i FROM range(128) t(i)"));

	connection.BeginTransaction();
	auto safe_plan = OptimizeQuery(connection, "SELECT i::TINYINT FROM safe_values");
	auto safe_counts = CountPlanCasts(*safe_plan);
	REQUIRE(safe_counts.unchecked == 1);
	REQUIRE(safe_counts.checked == 0);

	auto safe_try_plan = OptimizeQuery(connection, "SELECT TRY_CAST(i AS TINYINT) FROM safe_values");
	auto safe_try_counts = CountPlanCasts(*safe_try_plan);
	REQUIRE(safe_try_counts.unchecked == 1);
	REQUIRE(safe_try_counts.checked == 0);

	auto unsafe_plan = OptimizeQuery(connection, "SELECT i::TINYINT FROM unsafe_values");
	auto unsafe_counts = CountPlanCasts(*unsafe_plan);
	REQUIRE(unsafe_counts.unchecked == 0);
	REQUIRE(unsafe_counts.checked == 1);

	auto huge_plan = OptimizeQuery(connection, "SELECT i::TINYINT FROM huge_values");
	auto huge_counts = CountPlanCasts(*huge_plan);
	REQUIRE(huge_counts.unchecked == 0);
	REQUIRE(huge_counts.checked == 1);

	auto checked_cast = FindCast(*unsafe_plan);
	REQUIRE(checked_cast);
	auto unchecked_copy = checked_cast->Copy();
	auto &unchecked_function = unchecked_copy->Cast<BoundFunctionExpression>();
	REQUIRE(BoundCastExpression::GetBoundCastMutable(unchecked_function).TrySetUnchecked());
	REQUIRE(!checked_cast->Equals(*unchecked_copy));
	auto second_copy = unchecked_copy->Copy();
	REQUIRE(BoundCastExpression::GetBoundCast(second_copy->Cast<BoundFunctionExpression>()).IsUnchecked());
	REQUIRE(unchecked_copy->Equals(*second_copy));
	connection.Rollback();

	auto safe_result = connection.Query("SELECT min(i::TINYINT), max(i::TINYINT) FROM safe_values");
	REQUIRE_NO_FAIL(*safe_result);
	CHECK_COLUMN(safe_result, 0, {0});
	CHECK_COLUMN(safe_result, 1, {127});
	auto unsafe_result = connection.Query("SELECT count(*) FILTER (WHERE TRY_CAST(i AS TINYINT) IS NULL) "
	                                      "FROM unsafe_values");
	REQUIRE_NO_FAIL(*unsafe_result);
	CHECK_COLUMN(unsafe_result, 0, {128});
	REQUIRE_FAIL(connection.Query("SELECT sum(i::TINYINT) FROM unsafe_values"));
}

TEST_CASE("Compressed materialization creates unchecked native integer casts", "[optimizer][cast]") {
	DuckDB db;
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE safe_values AS SELECT i::BIGINT i FROM range(128) t(i)"));
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE huge_values AS SELECT i::HUGEINT i FROM range(128) t(i)"));

	connection.BeginTransaction();
	auto order_plan = OptimizeQuery(connection, "SELECT i FROM safe_values ORDER BY i");
	auto order_counts = CountPlanCasts(*order_plan);
	REQUIRE(order_counts.unchecked == 2);
	REQUIRE(order_counts.checked == 0);

	auto cte_plan =
	    OptimizeQuery(connection, "WITH cte AS MATERIALIZED (SELECT i FROM safe_values) SELECT i FROM cte ORDER BY i");
	auto cte_counts = CountPlanCasts(*cte_plan);
	REQUIRE(cte_counts.unchecked == 2);
	REQUIRE(cte_counts.checked == 0);

	auto huge_plan = OptimizeQuery(connection, "SELECT i FROM huge_values ORDER BY i");
	auto huge_counts = CountPlanCasts(*huge_plan);
	REQUIRE(huge_counts.unchecked == 0);
	REQUIRE(huge_counts.checked == 2);
	connection.Rollback();

	auto result = connection.Query("SELECT sum(i) FROM (SELECT i FROM safe_values ORDER BY i)");
	REQUIRE_NO_FAIL(*result);
	CHECK_COLUMN(result, 0, {8128});
}
