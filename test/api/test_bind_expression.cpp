#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"

using namespace duckdb;

static ExpressionBindResult BindOne(Connection &con, const string &expr, const vector<Identifier> &names,
                                    const vector<LogicalType> &types) {
	auto parsed = Parser::ParseExpressionList(expr, con.context->GetParserOptions());
	REQUIRE(parsed.size() == 1);
	return con.context->BindExpression(names, types, std::move(parsed[0]));
}

TEST_CASE("BindExpression reports type and aggregate/window traits", "[api][bind_expression]") {
	DuckDB db(nullptr);
	Connection con(db);

	vector<Identifier> names = {"a", "b"};
	vector<LogicalType> types = {LogicalType::INTEGER, LogicalType::VARCHAR};

	// A scalar expression over the input schema: type follows, no aggregate/window.
	auto add = BindOne(con, "a + 1", names, types);
	REQUIRE(add.type == LogicalType::INTEGER);
	REQUIRE_FALSE(add.contains_aggregate);
	REQUIRE_FALSE(add.contains_window);

	// A bare column reference resolves to that column's type.
	REQUIRE(BindOne(con, "b", names, types).type == LogicalType::VARCHAR);
	// A comparison is boolean.
	REQUIRE(BindOne(con, "a > 5", names, types).type == LogicalType::BOOLEAN);

	// A top-level aggregate is flagged (sum over INTEGER widens to HUGEINT); not a window.
	auto agg = BindOne(con, "sum(a)", names, types);
	REQUIRE(agg.type == LogicalType::HUGEINT);
	REQUIRE(agg.contains_aggregate);
	REQUIRE_FALSE(agg.contains_window);

	// A window function is flagged as a window; a windowed aggregate is a window, not an aggregate.
	auto rn = BindOne(con, "row_number() OVER ()", names, types);
	REQUIRE(rn.type == LogicalType::BIGINT);
	REQUIRE(rn.contains_window);
	auto win_agg = BindOne(con, "avg(a) OVER ()", names, types);
	REQUIRE(win_agg.type == LogicalType::DOUBLE);
	REQUIRE(win_agg.contains_window);
	REQUIRE_FALSE(win_agg.contains_aggregate);

	// Trait detection is top-level only: an aggregate inside a subquery is not flagged.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE nums(v INTEGER)"));
	auto sub = BindOne(con, "a > (SELECT max(v) FROM nums)", names, types);
	REQUIRE(sub.type == LogicalType::BOOLEAN);
	REQUIRE_FALSE(sub.contains_aggregate);

	// An empty input schema still binds a constant expression.
	REQUIRE(BindOne(con, "1 + 1", {}, {}).type == LogicalType::INTEGER);

	// An unknown column is a bind error.
	auto bad = Parser::ParseExpressionList("c + 1", con.context->GetParserOptions());
	REQUIRE(bad.size() == 1);
	REQUIRE_THROWS(con.context->BindExpression(names, types, std::move(bad[0])));
}

TEST_CASE("BindExpression does not disturb a live streaming result", "[api][bind_expression]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Open a stream and consume one chunk, leaving it live mid-flight.
	auto stream = con.SendQuery("SELECT i FROM range(5000) t(i)");
	REQUIRE(stream->type == QueryResultType::STREAM_RESULT);
	auto first = stream->Fetch();
	REQUIRE(first);
	idx_t seen = first->size();

	// Binding an expression must not cancel the stream.
	auto res = BindOne(con, "x * 2", {"x"}, {LogicalType::INTEGER});
	REQUIRE(res.type == LogicalType::INTEGER);

	// Every remaining row still arrives.
	while (auto chunk = stream->Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		seen += chunk->size();
	}
	REQUIRE(seen == 5000);
}
