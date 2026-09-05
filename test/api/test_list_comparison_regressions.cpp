#include "catch.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <vector>

using namespace duckdb;

namespace {

// Evaluates a "SELECT c, <predicate>" query and returns the predicate column, row by row.
static std::vector<Value> EvalPredicate(Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	auto &mat = result->Cast<MaterializedQueryResult>();
	std::vector<Value> out;
	for (idx_t r = 0; r < mat.RowCount(); r++) {
		out.push_back(mat.GetValue(1, r));
	}
	return out;
}

static std::vector<Value> Bools(std::initializer_list<bool> values) {
	std::vector<Value> out;
	for (auto v : values) {
		out.push_back(Value::BOOLEAN(v));
	}
	return out;
}

} // namespace

// Reproduction of https://github.com/duckdb/duckdb/issues/25169 - identical LIST values
// compared against the same constant return different, row-dependent answers.
// The trigger is a vectorized batch where a list is exhausted at some element position
// while other, still-equal rows continue to the next position.
TEST_CASE("LIST comparisons: identical rows must produce identical verdicts", "[comparison][list]") {
	DuckDB db(nullptr);
	Connection con(db);

	// === 1. canonical repro: an empty list exhausts before still-equal duplicates ===
	// [] < [0] and [-2, -1] < [0], so every verdict must be false - and the two identical
	// [-2, -1] rows must in any case agree with each other.
	{
		auto got = EvalPredicate(con, "SELECT c, c > [0] FROM (VALUES ([]::INTEGER[]), ([-2, -1]), ([-2, -1])) v(c)");
		REQUIRE(got == Bools({false, false, false}));
	}

	// === 2. exhaustion from the other side: right-hand list is longer ===
	{
		auto got = EvalPredicate(con, "SELECT c, c > [1, 2] FROM (VALUES ([1]::INTEGER[]), ([1, 2]), ([1, 2])) v(c)");
		REQUIRE(got == Bools({false, false, false}));
	}

	// === 3. negated predicate over the canonical repro ===
	{
		auto got =
		    EvalPredicate(con, "SELECT c, NOT (c > [0]) FROM (VALUES ([]::INTEGER[]), ([-2, -1]), ([-2, -1])) v(c)");
		REQUIRE(got == Bools({true, true, true}));
	}

	// === 4. equality with an empty list before duplicates ===
	{
		auto got =
		    EvalPredicate(con, "SELECT c, c = [-2, -1] FROM (VALUES ([]::INTEGER[]), ([-2, -1]), ([-2, -1])) v(c)");
		REQUIRE(got == Bools({false, true, true}));
	}

	// === 5. ordering flipped: constant on the left ===
	{
		auto got = EvalPredicate(con, "SELECT c, [0] < c FROM (VALUES ([]::INTEGER[]), ([-2, -1]), ([-2, -1])) v(c)");
		REQUIRE(got == Bools({false, false, false}));
	}

	// === 6. VARCHAR member lists ===
	{
		auto got = EvalPredicate(con, "SELECT c, c > ['a'] FROM (VALUES ([]::VARCHAR[]), (['b']), (['b'])) v(c)");
		REQUIRE(got == Bools({false, true, true}));
	}

	// === 7. NULL members: identical lists (including NULL positions) compare equal ===
	{
		auto got = EvalPredicate(
		    con, "SELECT c, c = [NULL, 2] FROM (VALUES ([NULL, 2]::INTEGER[]), ([NULL, 2]), ([NULL, 2])) v(c)");
		REQUIRE(got == Bools({true, true, true}));
	}

	// === 8. double-nested lists ===
	{
		auto got = EvalPredicate(con, "SELECT c, c > [[0]] FROM (VALUES ([[]]::INTEGER[][]), ([[1]]), ([[1]])) v(c)");
		REQUIRE(got == Bools({false, true, true}));
	}
	{
		auto got =
		    EvalPredicate(con, "SELECT c, c = [[1, 2]] FROM (VALUES ([[1, 2]]::INTEGER[][]), ([[1, 2]]), ([[1, 2]])) v(c)");
		REQUIRE(got == Bools({true, true, true}));
	}

	// === 9. triple-nested lists ===
	{
		auto got = EvalPredicate(
		    con, "SELECT c, c = [[[1]]] FROM (VALUES ([[[1]]]::INTEGER[][][]), ([[[1]]]), ([[[1]]])) v(c)");
		REQUIRE(got == Bools({true, true, true}));
	}

	// === 10. a filter must not split identical rows ===
	con.Query("CREATE TABLE t0(c0 INTEGER[], c1 INT)");
	con.Query("INSERT INTO t0 VALUES ([], 1), ([-2, -1], 2), ([-2, -1], 3)");
	{
		auto result = con.Query("SELECT c1 FROM t0 WHERE (c0 > [0]) ORDER BY c1");
		REQUIRE(result->Cast<MaterializedQueryResult>().RowCount() == 0);
	}
	{
		auto result = con.Query("SELECT c1 FROM t0 WHERE NOT (c0 > [0]) ORDER BY c1");
		auto &mat = result->Cast<MaterializedQueryResult>();
		REQUIRE(mat.RowCount() == 3);
		REQUIRE(mat.GetValue(0, 0) == 1);
		REQUIRE(mat.GetValue(0, 1) == 2);
		REQUIRE(mat.GetValue(0, 2) == 3);
	}

	// === 11. struct guard: structs use a comparator path without the position partition ===
	{
		auto got =
		    EvalPredicate(con, "SELECT s, s > {'a': 0} FROM (VALUES ({'a': -1}), ({'a': -1}), ({'a': -1})) v(s)");
		REQUIRE(got == Bools({false, false, false}));
	}
}
