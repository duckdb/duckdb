#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

using namespace duckdb;

namespace {

// like_escape stores its LikeMatcher in bind_info, which is the field the executor branches on to pick the fast path
bool HasBindInfo(const Expression &expr, const string &function_name) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == function_name) {
			return func.BindInfo() != nullptr;
		}
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found) {
			found = HasBindInfo(child, function_name);
		}
	});
	return found;
}

bool PlanHasBindInfo(const LogicalOperator &op, const string &function_name) {
	for (auto &expr : op.expressions) {
		if (HasBindInfo(*expr, function_name)) {
			return true;
		}
	}
	for (auto &child : op.children) {
		if (PlanHasBindInfo(*child, function_name)) {
			return true;
		}
	}
	return false;
}

bool MatcherBuilt(Connection &con, const string &query) {
	auto plan = con.ExtractPlan(query);
	REQUIRE(plan);
	return PlanHasBindInfo(*plan, "like_escape");
}

} // namespace

TEST_CASE("LikeMatcher is built for constant patterns that contain the escape character", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(s VARCHAR)"));

	// Baseline: a constant pattern whose escape character is unused takes the fast path.
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE '%abc%' ESCAPE '#' FROM t"));

	// Escaped wildcards are literals and can be included in constant segments.
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a#_b' ESCAPE '#' FROM t"));
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a#%b' ESCAPE '#' FROM t"));
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE '%a#_b%' ESCAPE '#' FROM t"));
	// An escape can also quote an ordinary character.
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a#b' ESCAPE '#' FROM t"));
	// The escape character escaping itself.
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a##b' ESCAPE '#' FROM t"));
	// Escape characters can also be LIKE wildcard characters.
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a%_b' ESCAPE '%' FROM t"));
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a__b' ESCAPE '_' FROM t"));

	// An empty ESCAPE clause disables escaping, and the pattern is constant without it.
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a#b' ESCAPE '' FROM t"));

	// NOT LIKE ... ESCAPE is planned as NOT like_escape(...), so it takes the same fast path.
	REQUIRE(MatcherBuilt(con, "SELECT s NOT LIKE 'a#_b' ESCAPE '#' FROM t"));

	// not_like_escape is only reachable by calling it by name, and is bound the same way.
	auto plan = con.ExtractPlan("SELECT not_like_escape(s, 'a#_b', '#') FROM t");
	REQUIRE(plan);
	REQUIRE(PlanHasBindInfo(*plan, "not_like_escape"));
}

TEST_CASE("LikeMatcher is declined where the segment model cannot express the pattern", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(s VARCHAR)"));

	// The segment model cannot represent the one-character gap introduced by a real '_'.
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE 'a_b' ESCAPE '#' FROM t"));
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE '%a_b%' ESCAPE '#' FROM t"));

	// A dangling escape is an invalid pattern, reported by the generic matcher per row.
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE 'abc#' ESCAPE '#' FROM t"));

	// GetEscapeChar() remains responsible for rejecting multi-character escape strings.
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE 'abc' ESCAPE 'xy' FROM t"));

	// A NULL pattern or escape has no pattern to prepare.
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE NULL ESCAPE '#' FROM t"));
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE 'abc' ESCAPE NULL FROM t"));

	// A pattern that is not foldable cannot be prepared ahead of time at all.
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE s ESCAPE '#' FROM t"));

	// A pattern that is entirely wildcards has no segment to anchor on.
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE '%%' ESCAPE '#' FROM t"));
}

TEST_CASE("LikeMatcher and collations", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// the matcher compares bytes, so like LikeBindFunction it has to decline any collation
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE no_collation(s VARCHAR)"));
	REQUIRE(MatcherBuilt(con, "SELECT s LIKE 'a#_b' ESCAPE '#' FROM no_collation"));

	// POSIX does not transform the string, but it is still recorded on the type.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE posix_collation(s VARCHAR COLLATE POSIX)"));
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE 'a#_b' ESCAPE '#' FROM posix_collation"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE nocase_collation(s VARCHAR COLLATE NOCASE)"));
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE 'a#_b' ESCAPE '#' FROM nocase_collation"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE ai_ci_collation(s VARCHAR COLLATE NOCASE.NOACCENT)"));
	REQUIRE(!MatcherBuilt(con, "SELECT s LIKE 'a#_b' ESCAPE '#' FROM ai_ci_collation"));
}
