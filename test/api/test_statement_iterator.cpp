#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/statement_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

using namespace duckdb;

// Drains an iterator into a flat vector of statements for compact assertions.
static vector<unique_ptr<SQLStatement>> DrainIterator(StatementIterator &it, ClientContext &context) {
	vector<unique_ptr<SQLStatement>> result;
	while (it.Peek(context)) {
		auto stmt = it.GetStatement();
		REQUIRE(stmt);
		result.push_back(std::move(stmt));
	}
	return result;
}

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

TEST_CASE("StatementIterator: move-assignment", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());

	// Move-assign over a separately constructed iterator. The target's own state is dropped and
	// the remaining statements from `it` become reachable through it.
	auto target = ctx.ExtractStatements("SELECT 99;");
	target = std::move(it);
	REQUIRE(target.Peek(ctx));
	auto s1 = target.GetStatement();
	REQUIRE(s1);
	REQUIRE(s1->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(target.Peek(ctx));
}

TEST_CASE("StatementIterator: no trailing separator", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// A final statement without a trailing ';' is still yielded.
	auto it = ctx.ExtractStatements("SELECT 1; SELECT 2");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 2);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("StatementIterator: separator-only input yields nothing", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// A run of nothing but separators is equivalent to empty input — no statements, clean
	// exhaustion (no throw).
	auto it = ctx.ExtractStatements(";;;;;;;;;;;");
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("StatementIterator: heavy separators around statements are skipped", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// Leading, interior and trailing separator runs collapse — exactly the two real statements
	// come through, in order.
	auto it = ctx.ExtractStatements(";;;;;;;;;; SELECT 42;;;;;; SELECT 1000;;;;;;;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 2);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(stmts[0]->query, "42"));
	REQUIRE(StringUtil::Contains(stmts[1]->query, "1000"));
}

TEST_CASE("StatementIterator: leading separators before a single statement", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements(";;; SELECT 7;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 1);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(stmts[0]->query, "7"));
}

TEST_CASE("StatementIterator: statement query text is populated and normalized", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	auto s0 = it.GetStatement();
	REQUIRE(s0);
	// Peek slices stmt->query out of the source and normalizes location/length to span exactly
	// that slice (location reset to 0, length == query size).
	REQUIRE_FALSE(s0->query.empty());
	REQUIRE(StringUtil::Contains(s0->query, "SELECT 1"));
	REQUIRE_FALSE(StringUtil::Contains(s0->query, "SELECT 2"));
	REQUIRE(s0->stmt_location == 0);
	REQUIRE(s0->stmt_length == s0->query.size());

	// The sliced query text round-trips through the parser as a standalone single statement.
	Parser reparser(ctx.GetParserOptions());
	reparser.ParseQuery(s0->query);
	REQUIRE(reparser.statements.size() == 1);
	REQUIRE(reparser.statements[0]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("StatementIterator: CREATE propagates query into CreateInfo", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("CREATE TABLE t (a INT);");
	REQUIRE(it.Peek(ctx));
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::CREATE_STATEMENT);
	// Peek mirrors Parser::ParseQuery's post-processing: the normalized query is copied into the
	// CreateInfo so downstream consumers (e.g. catalog `sql` column) see it.
	auto &create = stmt->Cast<CreateStatement>();
	REQUIRE(create.info->sql == stmt->query);
	REQUIRE(StringUtil::Contains(create.info->sql, "CREATE TABLE"));
}

TEST_CASE("StatementIterator: semicolon inside string literal is not a separator", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// The ';' lives inside a single-quoted string, so it must not split the statement.
	auto it = ctx.ExtractStatements("SELECT 'a;b;c' AS x;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 1);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("StatementIterator: comments between statements are skipped", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("/* block */ SELECT 1; -- line comment\nSELECT 2;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 2);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("StatementIterator: exhaustion is sticky across repeated Peek", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = ctx.ExtractStatements("SELECT 1;");
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());
	// Once drained, every subsequent Peek stays false — no re-parsing, no resurrection.
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}
