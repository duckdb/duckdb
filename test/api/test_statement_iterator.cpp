#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/statement_iterator.hpp"
#include "duckdb/parser/sql_statement.hpp"

using namespace duckdb;

// Drives the StatementIterator API end to end. The iterator is constructed via
// ClientContext::ExtractStatements(sql) and walked with Peek(context) + GetStatement(); these
// tests assert the basic state machine before we layer chunk-level / Layer-1 awareness on top.

TEST_CASE("StatementIterator: single statement", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1;");
	REQUIRE(it.Peek(ctx));
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("StatementIterator: empty input yields nothing", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("");
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("StatementIterator: whitespace-only input yields nothing", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("   \n\t  ");
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("StatementIterator: multiple statements yielded in order", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1; SELECT 2; SELECT 3;");
	int seen = 0;
	while (it.Peek(ctx)) {
		auto stmt = it.GetStatement();
		REQUIRE(stmt);
		REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
		seen++;
	}
	REQUIRE(seen == 3);
}

TEST_CASE("StatementIterator: Peek is idempotent until consumed", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1; SELECT 2;");
	// Multiple Peeks before GetStatement should keep returning true and not advance.
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.Peek(ctx));
	auto first = it.GetStatement();
	REQUIRE(first);
	REQUIRE(it.Peek(ctx));
	auto second = it.GetStatement();
	REQUIRE(second);
	REQUIRE_FALSE(it.Peek(ctx));
}

TEST_CASE("StatementIterator: GetStatement without prior Peek", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1;");
	// First GetStatement without Peek returns nullptr — Peek is the trigger that fills the
	// buffer. The user is expected to call Peek(ctx) before GetStatement().
	auto stmt = it.GetStatement();
	REQUIRE_FALSE(stmt);
	// After a Peek, the statement becomes available.
	REQUIRE(it.Peek(ctx));
	stmt = it.GetStatement();
	REQUIRE(stmt);
}

TEST_CASE("StatementIterator: mixed statement types", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("CREATE TABLE t (a INT); INSERT INTO t VALUES (1); SELECT * FROM t;");
	REQUIRE(it.Peek(ctx));
	auto s0 = it.GetStatement();
	REQUIRE(s0);
	REQUIRE(s0->type == StatementType::CREATE_STATEMENT);

	REQUIRE(it.Peek(ctx));
	auto s1 = it.GetStatement();
	REQUIRE(s1);
	REQUIRE(s1->type == StatementType::INSERT_STATEMENT);

	REQUIRE(it.Peek(ctx));
	auto s2 = it.GetStatement();
	REQUIRE(s2);
	REQUIRE(s2->type == StatementType::SELECT_STATEMENT);

	REQUIRE_FALSE(it.Peek(ctx));
}

TEST_CASE("StatementIterator: parser errors surface through Peek", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT FROM;");
	REQUIRE_THROWS(it.Peek(ctx));
}

TEST_CASE("StatementIterator: movable", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	auto s0 = it.GetStatement();
	REQUIRE(s0);

	// Move-construct a new iterator from `it`. The remaining statements should be reachable
	// through the moved-to iterator.
	StatementIterator moved(std::move(it));
	REQUIRE(moved.Peek(ctx));
	auto s1 = moved.GetStatement();
	REQUIRE(s1);
	REQUIRE_FALSE(moved.Peek(ctx));
}
