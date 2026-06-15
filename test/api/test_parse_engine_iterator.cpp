#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/parse_iterator.hpp"
#include "duckdb/main/engine_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

using namespace duckdb;

// ParseIterator and EngineIterator no longer share a contract:
//   ParseIterator : Peek(ctx) parses + buffers one statement; GetStatement() returns it (no ctx,
//                   no preprocessing). GetStatement() before Peek returns nullptr.
//   EngineIterator: Peek(ctx) answers "is there more input?" (parses ahead, NO preprocessing);
//                   GetStatement(ctx) parses + preprocesses the next peel and may return nullptr
//                   when that peel preprocesses to nothing. GetStatement is self-sufficient (works
//                   without a prior Peek).
// So the two are tested separately below.

//===--------------------------------------------------------------------===//
// ParseIterator
//===--------------------------------------------------------------------===//

static vector<unique_ptr<SQLStatement>> DrainParse(ParseIterator &it, ClientContext &context) {
	vector<unique_ptr<SQLStatement>> result;
	while (it.Peek(context)) {
		auto stmt = it.GetStatement();
		REQUIRE(stmt);
		result.push_back(std::move(stmt));
	}
	return result;
}

TEST_CASE("ParseIterator: single statement", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("SELECT 1;");
	REQUIRE(it.Peek(ctx));
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("ParseIterator: empty and whitespace input yields nothing", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator empty("");
	REQUIRE_FALSE(empty.Peek(ctx));
	ParseIterator ws("   \n\t  ");
	REQUIRE_FALSE(ws.Peek(ctx));
}

TEST_CASE("ParseIterator: multiple statements in order", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("SELECT 1; SELECT 2; SELECT 3;");
	auto stmts = DrainParse(it, ctx);
	REQUIRE(stmts.size() == 3);
	for (auto &stmt : stmts) {
		REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	}
}

TEST_CASE("ParseIterator: Peek is idempotent until consumed", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());
	REQUIRE_FALSE(it.Peek(ctx));
}

TEST_CASE("ParseIterator: GetStatement without prior Peek returns nullptr", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("SELECT 1;");
	REQUIRE_FALSE(it.GetStatement());
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());
}

TEST_CASE("ParseIterator: mixed statement types", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("CREATE TABLE t (a INT); INSERT INTO t VALUES (1); SELECT * FROM t;");
	auto stmts = DrainParse(it, ctx);
	REQUIRE(stmts.size() == 3);
	REQUIRE(stmts[0]->type == StatementType::CREATE_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::INSERT_STATEMENT);
	REQUIRE(stmts[2]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("ParseIterator: parser errors surface through Peek", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("SELECT FROM;");
	REQUIRE_THROWS(it.Peek(ctx));
}

TEST_CASE("ParseIterator: movable and move-assignable", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement());

	ParseIterator moved(std::move(it));
	REQUIRE(moved.Peek(ctx));
	REQUIRE(moved.GetStatement());
	REQUIRE_FALSE(moved.Peek(ctx));

	ParseIterator target("SELECT 99;");
	ParseIterator src("SELECT 1; SELECT 2;");
	REQUIRE(src.Peek(ctx));
	REQUIRE(src.GetStatement());
	target = std::move(src);
	REQUIRE(target.Peek(ctx));
	REQUIRE(target.GetStatement());
	REQUIRE_FALSE(target.Peek(ctx));
}

TEST_CASE("ParseIterator: separators are skipped, no trailing separator needed", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator only_seps(";;;;;;;;;;;");
	REQUIRE_FALSE(only_seps.Peek(ctx));

	ParseIterator heavy(";;;;;;;;;; SELECT 42;;;;;; SELECT 1000");
	auto stmts = DrainParse(heavy, ctx);
	REQUIRE(stmts.size() == 2);
	REQUIRE(StringUtil::Contains(stmts[0]->query, "42"));
	REQUIRE(StringUtil::Contains(stmts[1]->query, "1000"));
}

TEST_CASE("ParseIterator: statement query text is populated and normalized", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("SELECT 1; SELECT 2;");
	REQUIRE(it.Peek(ctx));
	auto s0 = it.GetStatement();
	REQUIRE(s0);
	REQUIRE(StringUtil::Contains(s0->query, "SELECT 1"));
	REQUIRE_FALSE(StringUtil::Contains(s0->query, "SELECT 2"));
	REQUIRE(s0->stmt_location == 0);
	REQUIRE(s0->stmt_length == s0->query.size());

	Parser reparser(ctx.GetParserOptions());
	reparser.ParseQuery(s0->query);
	REQUIRE(reparser.statements.size() == 1);
}

TEST_CASE("ParseIterator: CREATE propagates query into CreateInfo", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it("CREATE TABLE t (a INT);");
	REQUIRE(it.Peek(ctx));
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	auto &create = stmt->Cast<CreateStatement>();
	REQUIRE(create.info->sql == stmt->query);
	REQUIRE(StringUtil::Contains(create.info->sql, "CREATE TABLE"));
}

TEST_CASE("ParseIterator: semicolons in strings and comments handled", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator in_string("SELECT 'a;b;c' AS x;");
	REQUIRE(DrainParse(in_string, ctx).size() == 1);

	ParseIterator with_comments("/* block */ SELECT 1; -- line comment\nSELECT 2;");
	REQUIRE(DrainParse(with_comments, ctx).size() == 2);
}

TEST_CASE("ParseIterator: single already-parsed statement", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator parse("SELECT 42;");
	REQUIRE(parse.Peek(ctx));
	auto parsed = parse.GetStatement();
	REQUIRE(parsed);

	ParseIterator it(std::move(parsed));
	REQUIRE(it.Peek(ctx));
	auto out = it.GetStatement();
	REQUIRE(out);
	REQUIRE(out->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(out->query, "42"));
	REQUIRE_FALSE(it.Peek(ctx));
}

//===--------------------------------------------------------------------===//
// EngineIterator (new contract: Peek = "more input?", Get = work, may be null)
//===--------------------------------------------------------------------===//

static vector<unique_ptr<SQLStatement>> DrainEngine(EngineIterator &it, ClientContext &context) {
	vector<unique_ptr<SQLStatement>> result;
	while (it.Peek(context)) {
		auto stmt = it.GetStatement(context);
		if (!stmt) {
			continue; // a peel that preprocessing swallowed
		}
		result.push_back(std::move(stmt));
	}
	return result;
}

TEST_CASE("EngineIterator: single statement", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator("SELECT 1;")};
	REQUIRE(it.Peek(ctx));
	auto stmt = it.GetStatement(ctx);
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement(ctx));
}

TEST_CASE("EngineIterator: empty input yields nothing", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator("")};
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement(ctx));
}

TEST_CASE("EngineIterator: multiple statements in order", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator("SELECT 1; SELECT 2; SELECT 3;")};
	auto stmts = DrainEngine(it, ctx);
	REQUIRE(stmts.size() == 3);
	for (auto &stmt : stmts) {
		REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	}
}

TEST_CASE("EngineIterator: mixed statement types", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator("CREATE TABLE t (a INT); INSERT INTO t VALUES (1); SELECT * FROM t;")};
	auto stmts = DrainEngine(it, ctx);
	REQUIRE(stmts.size() == 3);
	REQUIRE(stmts[0]->type == StatementType::CREATE_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::INSERT_STATEMENT);
	REQUIRE(stmts[2]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("EngineIterator: separator-only input yields nothing", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator(";;;;;;")};
	REQUIRE_FALSE(it.Peek(ctx));
}

TEST_CASE("EngineIterator: Peek is a pure predicate, GetStatement does the work", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator("SELECT 1; SELECT 2;")};
	// Repeated Peek does not consume.
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.Peek(ctx));
	auto a = it.GetStatement(ctx);
	REQUIRE(a);
	REQUIRE(it.Peek(ctx));
	auto b = it.GetStatement(ctx);
	REQUIRE(b);
	REQUIRE_FALSE(it.Peek(ctx));
}

TEST_CASE("EngineIterator: GetStatement works without a prior Peek", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// Unlike ParseIterator, EngineIterator's GetStatement is self-sufficient: it pulls + preprocesses
	// on demand, no priming Peek required.
	EngineIterator it {ParseIterator("SELECT 7;")};
	auto stmt = it.GetStatement(ctx);
	REQUIRE(stmt);
	REQUIRE(StringUtil::Contains(stmt->query, "7"));
	REQUIRE_FALSE(it.Peek(ctx));
}

TEST_CASE("EngineIterator: parser errors surface through Peek", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator("SELECT FROM;")};
	REQUIRE_THROWS(it.Peek(ctx));
}

TEST_CASE("EngineIterator: movable", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	EngineIterator it {ParseIterator("SELECT 1; SELECT 2;")};
	REQUIRE(it.Peek(ctx));
	REQUIRE(it.GetStatement(ctx));

	EngineIterator moved(std::move(it));
	REQUIRE(moved.Peek(ctx));
	REQUIRE(moved.GetStatement(ctx));
	REQUIRE_FALSE(moved.Peek(ctx));
}

TEST_CASE("EngineIterator: single already-parsed statement source", "[api][engine_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator parse("SELECT 42;");
	REQUIRE(parse.Peek(ctx));
	auto parsed = parse.GetStatement();
	REQUIRE(parsed);

	EngineIterator it(std::move(parsed));
	REQUIRE(it.Peek(ctx));
	auto out = it.GetStatement(ctx);
	REQUIRE(out);
	REQUIRE(out->type == StatementType::SELECT_STATEMENT);
	REQUIRE(StringUtil::Contains(out->query, "42"));
	REQUIRE_FALSE(it.Peek(ctx));
	REQUIRE_FALSE(it.GetStatement(ctx));
}
