#include "catch.hpp"
#include "expression_helper.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test projection bindings for ORDER BY", "[projection-binding-order-by]") {
	ExpressionHelper helper;
	using Op = LogicalOperatorType;

	auto projection_matches = [&](string query, vector<LogicalOperatorType> path, size_t count) -> bool {
		auto plan = helper.ParseLogicalTree(query);
		for (auto type : path) {
			if (plan->type != type)
				return false;
			if (plan->children.size() == 0)
				return false;
			plan = move(plan->children[0]);
		}
		return (plan->type == Op::PROJECTION && plan->expressions.size() == count);
	};

	auto &con = helper.con;
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a (i INTEGER, j INTEGER)"));

	REQUIRE(projection_matches("SELECT i FROM a ORDER BY i", {Op::ORDER_BY}, 1));
	REQUIRE(projection_matches("SELECT a.i FROM a ORDER BY i", {Op::ORDER_BY}, 1));
	REQUIRE(projection_matches("SELECT i FROM a ORDER BY a.i", {Op::ORDER_BY}, 1));
	REQUIRE(projection_matches("SELECT i AS k FROM a ORDER BY i", {Op::ORDER_BY}, 1));

	con.Query("DROP TABLE a");
}
