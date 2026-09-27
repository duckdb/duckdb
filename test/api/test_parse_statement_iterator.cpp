#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/parse_iterator.hpp"
#include "duckdb/main/statement_iterator.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"
#include "duckdb/parser/token_iterator.hpp"

using namespace duckdb;

struct CountingParserExtensionInfo : ParserExtensionInfo {
	idx_t calls = 0;
};

static ParserExtensionParseResult CountingParserExtension(ParserExtensionInfo *info, const vector<SimpleToken> &) {
	auto &counting_info = static_cast<CountingParserExtensionInfo &>(*info);
	counting_info.calls++;
	throw ParserException("counting parser extension invoked");
}

static shared_ptr<CountingParserExtensionInfo> RegisterCountingParserExtension(Connection &con) {
	auto info = make_shared_ptr<CountingParserExtensionInfo>();
	ParserExtension extension;
	extension.parse_function = CountingParserExtension;
	extension.parser_info = info;
	ExtensionCallbackManager::Get(*con.context).Register(std::move(extension));
	return info;
}

static unique_ptr<TokenIterator> TokenizeForParser(ClientContext &context, const string &query) {
	auto tokens = make_uniq<vector<MatcherToken>>();
	ParserTokenizerBehavior behavior(query, *tokens);
	auto grammar = CompiledGrammar::Get(context);
	grammar->GetTokenizer().TokenizeInput(behavior);
	return make_uniq<TokenIterator>(std::move(tokens));
}

template <class FUNC>
static void RequireTransformError(FUNC &&parse) {
	try {
		parse();
		FAIL("Expected parser transform error");
	} catch (ParserException &ex) {
		REQUIRE(StringUtil::Contains(ex.what(), "Wrong number of arguments provided to TRY expression"));
	}
}

template <class FUNC>
static void RequireExtensionError(FUNC &&parse) {
	try {
		parse();
		FAIL("Expected parser extension error");
	} catch (ParserException &ex) {
		REQUIRE(StringUtil::Contains(ex.what(), "counting parser extension invoked"));
	}
}

// ParseIterator and StatementIterator no longer share a contract. Both bind their ClientContext at
// construction, so Peek()/GetStatement() take no context argument:
//   ParseIterator : Peek() parses + buffers one statement; GetStatement() returns it (no
//                   preprocessing). GetStatement() before Peek returns nullptr.
//   StatementIterator: Peek() answers "is there more input?" (parses ahead, NO preprocessing);
//                   GetStatement() parses + preprocesses the next peel and may return nullptr
//                   when that peel preprocesses to nothing. GetStatement is self-sufficient (works
//                   without a prior Peek).
// So the two are tested separately below.

//===--------------------------------------------------------------------===//
// ParseIterator
//===--------------------------------------------------------------------===//

static vector<unique_ptr<SQLStatement>> DrainParse(ParseIterator &it) {
	vector<unique_ptr<SQLStatement>> result;
	while (it.Peek()) {
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

	ParseIterator it(ctx, "SELECT 1;");
	REQUIRE(it.Peek());
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(it.Peek());
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("ParseIterator: empty and whitespace input yields nothing", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator empty(ctx, "");
	REQUIRE_FALSE(empty.Peek());
	ParseIterator ws(ctx, "   \n\t  ");
	REQUIRE_FALSE(ws.Peek());
}

TEST_CASE("ParseIterator: multiple statements in order", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it(ctx, "SELECT 1; SELECT 2; SELECT 3;");
	auto stmts = DrainParse(it);
	REQUIRE(stmts.size() == 3);
	for (auto &stmt : stmts) {
		REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	}
}

TEST_CASE("ParseIterator: Peek is idempotent until consumed", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it(ctx, "SELECT 1; SELECT 2;");
	REQUIRE(it.Peek());
	REQUIRE(it.Peek());
	REQUIRE(it.Peek());
	REQUIRE(it.GetStatement());
	REQUIRE(it.Peek());
	REQUIRE(it.GetStatement());
	REQUIRE_FALSE(it.Peek());
}

TEST_CASE("ParseIterator: GetStatement without prior Peek returns nullptr", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it(ctx, "SELECT 1;");
	REQUIRE_FALSE(it.GetStatement());
	REQUIRE(it.Peek());
	REQUIRE(it.GetStatement());
}

TEST_CASE("ParseIterator: mixed statement types", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it(ctx, "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); SELECT * FROM t;");
	auto stmts = DrainParse(it);
	REQUIRE(stmts.size() == 3);
	REQUIRE(stmts[0]->type == StatementType::CREATE_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::INSERT_STATEMENT);
	REQUIRE(stmts[2]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("ParseIterator: parser errors surface through Peek", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it(ctx, "SELECT FROM;");
	REQUIRE_THROWS(it.Peek());
}

TEST_CASE("ParseIterator: movable", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// Move-constructible (it binds a ClientContext reference, so it is not move-assignable).
	ParseIterator it(ctx, "SELECT 1; SELECT 2;");
	REQUIRE(it.Peek());
	REQUIRE(it.GetStatement());

	ParseIterator moved(std::move(it));
	REQUIRE(moved.Peek());
	REQUIRE(moved.GetStatement());
	REQUIRE_FALSE(moved.Peek());
}

TEST_CASE("ParseIterator: separators are skipped, no trailing separator needed", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator only_seps(ctx, ";;;;;;;;;;;");
	REQUIRE_FALSE(only_seps.Peek());

	ParseIterator heavy(ctx, ";;;;;;;;;; SELECT 42;;;;;; SELECT 1000");
	auto stmts = DrainParse(heavy);
	REQUIRE(stmts.size() == 2);
	REQUIRE(StringUtil::Contains(stmts[0]->query, "42"));
	REQUIRE(StringUtil::Contains(stmts[1]->query, "1000"));
}

TEST_CASE("ParseIterator: statement query text is populated and normalized", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it(ctx, "SELECT 1; SELECT 2;");
	REQUIRE(it.Peek());
	auto s0 = it.GetStatement();
	REQUIRE(s0);
	REQUIRE(StringUtil::Contains(s0->query, "SELECT 1"));
	REQUIRE_FALSE(StringUtil::Contains(s0->query, "SELECT 2"));
	REQUIRE(s0->stmt_location.offset == 0);
	REQUIRE(s0->stmt_location.length == s0->query.size());

	Parser reparser(ctx.GetParserOptions());
	reparser.ParseQuery(s0->query);
	REQUIRE(reparser.statements.size() == 1);
}

TEST_CASE("ParseIterator: CREATE propagates query into CreateInfo", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	ParseIterator it(ctx, "CREATE TABLE t (a INT);");
	REQUIRE(it.Peek());
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

	ParseIterator in_string(ctx, "SELECT 'a;b;c' AS x;");
	REQUIRE(DrainParse(in_string).size() == 1);

	ParseIterator with_comments(ctx, "/* block */ SELECT 1; -- line comment\nSELECT 2;");
	REQUIRE(DrainParse(with_comments).size() == 2);
}

TEST_CASE("Parser extensions only handle grammar match failures", "[api][parse_iterator][parser_extension]") {
	for (auto heap_based_parser : {false, true}) {
		DuckDB db(nullptr);
		Connection con(db);
		auto setting = StringUtil::Format("SET heap_based_parser = %s", heap_based_parser ? "true" : "false");
		REQUIRE_NO_FAIL(*con.Query(setting));
		auto info = RegisterCountingParserExtension(con);

		Parser eager_parser(con.context->GetParserOptions());
		RequireTransformError([&]() { eager_parser.ParseQuery("SELECT TRY(1,2); quack quack quack;"); });
		REQUIRE(info->calls == 0);

		ParseIterator lazy_parser(*con.context, "SELECT TRY(1,2); quack quack quack;");
		RequireTransformError([&]() { lazy_parser.Peek(); });
		REQUIRE(info->calls == 0);

		Parser eager_grammar_failure(con.context->GetParserOptions());
		RequireExtensionError([&]() { eager_grammar_failure.ParseQuery("quack quack quack;"); });
		REQUIRE(info->calls == 1);

		ParseIterator lazy_grammar_failure(*con.context, "quack quack quack;");
		RequireExtensionError([&]() { lazy_grammar_failure.Peek(); });
		REQUIRE(info->calls == 2);
	}
}

TEST_CASE("ParseTopLevelStatement commits its token cursor only on success", "[api][parse_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);

	for (auto heap_based_parser : {false, true}) {
		auto options = con.context->GetParserOptions();
		options.heap_based_parser = heap_based_parser;
		Parser parser(options);

		auto failed_tokens = TokenizeForParser(*con.context, "SELECT TRY(1,2); SELECT 42;");
		auto initial_position = failed_tokens->Position();
		RequireTransformError([&]() { parser.ParseTopLevelStatement(*failed_tokens); });
		REQUIRE(failed_tokens->Position() == initial_position);

		auto successful_tokens = TokenizeForParser(*con.context, ";;; SELECT 42;");
		auto separator_position = successful_tokens->Position();
		auto separators = parser.ParseTopLevelStatement(*successful_tokens);
		REQUIRE_FALSE(separators);
		REQUIRE(successful_tokens->Position() > separator_position);

		auto statement_position = successful_tokens->Position();
		auto statement = parser.ParseTopLevelStatement(*successful_tokens);
		REQUIRE(statement);
		REQUIRE(statement->type == StatementType::SELECT_STATEMENT);
		REQUIRE(successful_tokens->Position() > statement_position);
	}
}

//===--------------------------------------------------------------------===//
// StatementIterator (new contract: Peek = "more input?", Get = work, may be null)
//===--------------------------------------------------------------------===//

static vector<unique_ptr<SQLStatement>> DrainStatements(StatementIterator &it) {
	vector<unique_ptr<SQLStatement>> result;
	while (it.Peek()) {
		auto stmt = it.GetStatement();
		if (!stmt) {
			continue; // a peel that preprocessing swallowed
		}
		result.push_back(std::move(stmt));
	}
	return result;
}

TEST_CASE("StatementIterator: single statement", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, "SELECT 1;")};
	REQUIRE(it.Peek());
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	REQUIRE_FALSE(it.Peek());
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("StatementIterator: empty input yields nothing", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, "")};
	REQUIRE_FALSE(it.Peek());
	REQUIRE_FALSE(it.GetStatement());
}

TEST_CASE("StatementIterator: multiple statements in order", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, "SELECT 1; SELECT 2; SELECT 3;")};
	auto stmts = DrainStatements(it);
	REQUIRE(stmts.size() == 3);
	for (auto &stmt : stmts) {
		REQUIRE(stmt->type == StatementType::SELECT_STATEMENT);
	}
}

TEST_CASE("StatementIterator: mixed statement types", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, "CREATE TABLE t (a INT); INSERT INTO t VALUES (1); SELECT * FROM t;")};
	auto stmts = DrainStatements(it);
	REQUIRE(stmts.size() == 3);
	REQUIRE(stmts[0]->type == StatementType::CREATE_STATEMENT);
	REQUIRE(stmts[1]->type == StatementType::INSERT_STATEMENT);
	REQUIRE(stmts[2]->type == StatementType::SELECT_STATEMENT);
}

TEST_CASE("StatementIterator: separator-only input yields nothing", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, ";;;;;;")};
	REQUIRE_FALSE(it.Peek());
}

TEST_CASE("StatementIterator: Peek is a pure predicate, GetStatement does the work", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, "SELECT 1; SELECT 2;")};
	// Repeated Peek does not consume.
	REQUIRE(it.Peek());
	REQUIRE(it.Peek());
	auto a = it.GetStatement();
	REQUIRE(a);
	REQUIRE(it.Peek());
	auto b = it.GetStatement();
	REQUIRE(b);
	REQUIRE_FALSE(it.Peek());
}

TEST_CASE("StatementIterator: GetStatement works without a prior Peek", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	// Unlike ParseIterator, StatementIterator's GetStatement is self-sufficient: it pulls + preprocesses
	// on demand, no priming Peek required.
	StatementIterator it {ParseIterator(ctx, "SELECT 7;")};
	auto stmt = it.GetStatement();
	REQUIRE(stmt);
	REQUIRE(StringUtil::Contains(stmt->query, "7"));
	REQUIRE_FALSE(it.Peek());
}

TEST_CASE("StatementIterator: parser errors surface through Peek", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, "SELECT FROM;")};
	REQUIRE_THROWS(it.Peek());
}

TEST_CASE("StatementIterator: movable", "[api][statement_iterator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &ctx = *con.context;

	StatementIterator it {ParseIterator(ctx, "SELECT 1; SELECT 2;")};
	REQUIRE(it.Peek());
	REQUIRE(it.GetStatement());

	StatementIterator moved(std::move(it));
	REQUIRE(moved.Peek());
	REQUIRE(moved.GetStatement());
	REQUIRE_FALSE(moved.Peek());
}
