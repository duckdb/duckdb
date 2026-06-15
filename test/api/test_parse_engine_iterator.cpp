#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/parse_iterator.hpp"
#include "duckdb/main/engine_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

using namespace duckdb;

// ParseIterator and EngineIterator share the Peek/GetStatement protocol by convention (no common
// base). Every parse-level behavior below is identical for both: preprocessing is a no-op on plain
// statements, so an EngineIterator wrapping a ParseIterator yields the same statements. The shared
// suite is therefore written once via TEMPLATE_TEST_CASE and run over both types; `MakeIterator`
// is the only construction difference (EngineIterator wraps a ParseIterator).

template <class IT>
static IT MakeIterator(const string &sql);

template <>
ParseIterator MakeIterator<ParseIterator>(const string &sql) {
	return ParseIterator(sql);
}

template <>
EngineIterator MakeIterator<EngineIterator>(const string &sql) {
	return EngineIterator(ParseIterator(sql));
}

// Drains an iterator into a flat vector of statements for compact assertions.
template <class IT>
static vector<unique_ptr<SQLStatement>> DrainIterator(IT &it, ClientContext &context) {
	vector<unique_ptr<SQLStatement>> result;
	while (it.Peek(context)) {
		auto stmt = it.GetStatement();
		REQUIRE(stmt);
		result.push_back(std::move(stmt));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Shared parse-level behavior — run over both ParseIterator and EngineIterator
//===--------------------------------------------------------------------===//

TEMPLATE_TEST_CASE("Iterator: single statement", "[api][statement_iterators]", ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1;");
	REQUIRE(it.Peek(ctx));
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEMPLATE_TEST_CASE("Iterator: empty input yields nothing", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("");
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEMPLATE_TEST_CASE("Iterator: whitespace-only input yields nothing", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("   \n\t  ");
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEMPLATE_TEST_CASE("Iterator: multiple statements yielded in order", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1; SELECT 2; SELECT 3;");
	int seen = 0;
	while (it.Peek(ctx)) {
		auto stmt = it.GetStatement();
		REQUIRE(stmt);
		REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
		seen++;
	}
	REQUIRE(seen == 3);
}

TEMPLATE_TEST_CASE("Iterator: Peek is idempotent until consumed", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1; SELECT 2;");
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

TEMPLATE_TEST_CASE("Iterator: GetStatement without prior Peek", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1;");
	auto stmt = it.GetStatement();
	REQUIRE_FALSE(stmt);
	REQUIRE(it.Peek(ctx));
	stmt = it.GetStatement();
	REQUIRE(stmt);
}

TEMPLATE_TEST_CASE("Iterator: mixed statement types", "[api][statement_iterators]", ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("CREATE TABLE t (a INT); INSERT INTO t VALUES (1); SELECT * FROM t;");
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

TEMPLATE_TEST_CASE("Iterator: parser errors surface through Peek", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT FROM;");
	REQUIRE_THROWS(it.Peek(ctx));
}

TEMPLATE_TEST_CASE("Iterator: movable", "[api][statement_iterators]", ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	auto s0 = it.GetStatement();
	REQUIRE(s0);

	TestType moved(std::move(it));
	REQUIRE(moved.Peek(ctx));
	auto s1 = moved.GetStatement();
	REQUIRE(s1);
	REQUIRE_FALSE(moved.Peek(ctx));
}

TEMPLATE_TEST_CASE("Iterator: move-assignment", "[api][statement_iterators]", ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());

	auto target = MakeIterator<TestType>("SELECT 99;");
	target = std::move(it);
	REQUIRE(target.Peek(ctx));
	auto s1 = target.GetStatement();
	REQUIRE(s1);
	REQUIRE(s1->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(target.Peek(ctx));
}

TEMPLATE_TEST_CASE("Iterator: no trailing separator", "[api][statement_iterators]", ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1; SELECT 2");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 2);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::SELECT_STATEMENT);
}

TEMPLATE_TEST_CASE("Iterator: separator-only input yields nothing", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>(";;;;;;;;;;;");
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEMPLATE_TEST_CASE("Iterator: heavy separators around statements are skipped", "[api][statement_iterators]",
                   ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>(";;;;;;;;;; SELECT 42;;;;;; SELECT 1000;;;;;;;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 2);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(stmts[0]->query, "42"));
	REQUIRE(StringUtil::Contains(stmts[1]->query, "1000"));
}

TEMPLATE_TEST_CASE("Iterator: leading separators before a single statement", "[api][statement_iterators]",
                   ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>(";;; SELECT 7;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 1);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(stmts[0]->query, "7"));
}

TEMPLATE_TEST_CASE("Iterator: statement query text is populated and normalized", "[api][statement_iterators]",
                   ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	auto s0 = it.GetStatement();
	REQUIRE(s0);
	REQUIRE_FALSE(s0->query.empty());
	REQUIRE(StringUtil::Contains(s0->query, "SELECT 1"));
	REQUIRE_FALSE(StringUtil::Contains(s0->query, "SELECT 2"));
	REQUIRE(s0->stmt_location == 0);
	REQUIRE(s0->stmt_length == s0->query.size());

	Parser reparser(ctx.GetParserOptions());
	reparser.ParseQuery(s0->query);
	REQUIRE(reparser.statements.size() == 1);
	REQUIRE(reparser.statements[0]->type == StatementType::SELECT_STATEMENT);
}

TEMPLATE_TEST_CASE("Iterator: CREATE propagates query into CreateInfo", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("CREATE TABLE t (a INT);");
	REQUIRE(it.Peek(ctx));
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::CREATE_STATEMENT);
	auto &create = stmt->template Cast<CreateStatement>();
	REQUIRE(create.info->sql == stmt->query);
	REQUIRE(StringUtil::Contains(create.info->sql, "CREATE TABLE"));
}

TEMPLATE_TEST_CASE("Iterator: semicolon inside string literal is not a separator", "[api][statement_iterators]",
                   ParseIterator, EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 'a;b;c' AS x;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 1);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
}

TEMPLATE_TEST_CASE("Iterator: comments between statements are skipped", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("/* block */ SELECT 1; -- line comment\nSELECT 2;");
	auto stmts = DrainIterator(it, ctx);
	REQUIRE(stmts.size() == 2);
	REQUIRE(stmts[0]->type == StatementType::SELECT_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::SELECT_STATEMENT);
}

TEMPLATE_TEST_CASE("Iterator: exhaustion is sticky across repeated Peek", "[api][statement_iterators]", ParseIterator,
                   EngineIterator) {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto it = MakeIterator<TestType>("SELECT 1;");
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

//===--------------------------------------------------------------------===//
// EngineIterator-specific — the single already-parsed statement source
//===--------------------------------------------------------------------===//

// Parses a single statement out of `sql` via a ParseIterator, for feeding the single-statement
// EngineIterator constructor.
static unique_ptr<SQLStatement> ParseOne(const string &sql, ClientContext &context) {
	ParseIterator it(sql);
	REQUIRE(it.Peek(context));
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	return stmt;
}

TEST_CASE("ParseIterator: single already-parsed statement", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto stmt = ParseOne("SELECT 42;", ctx);
	ParseIterator it(std::move(stmt));
	REQUIRE(it.Peek(ctx));
	auto out = it.GetStatement();
	REQUIRE(out);
	REQUIRE(out->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(out->query, "42"));
	// Yielded once, then exhausted.
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("EngineIterator: single already-parsed statement", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto stmt = ParseOne("SELECT 42;", ctx);
	EngineIterator it(std::move(stmt));
	REQUIRE(it.Peek(ctx));
	auto out = it.GetStatement();
	REQUIRE(out);
	REQUIRE(out->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(out->query, "42"));
	// A single-statement source is exhausted after its one statement.
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("EngineIterator: single statement, GetStatement needs prior Peek", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	auto stmt = ParseOne("SELECT 1;", ctx);
	EngineIterator it(std::move(stmt));
	REQUIRE_FALSE(it.GetStatement());
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());
	REQUIRE_FALSE(it.Peek(ctx));
}
