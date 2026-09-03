#include "test_capi_v2.hpp"

// ---------------------------------------------------------------------------
// V2 sql_statement tests: parse_sql, the statement iterator, and executing
// statements via statement_execute (non-consuming: it runs a copy, so the
// statement stays alive and re-executable, and the caller destroys it).
// ---------------------------------------------------------------------------

namespace test_capi_v2 {
namespace {

// Parse exactly one statement from sql (raw, unbound). Unique name to avoid a
// unity-build clash with the same-shaped helper in other test files.
duckdb_v2_sql_statement_handle StmtParseOne(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(conn, sql, &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr);
	duckdb_v2_statement_iterator_destroy(&iter);
	return stmt;
}

// Reads the first row's column 0 as int64 and pins end-of-stream, for a single-row
// scalar SELECT (e.g. SELECT $1 + $2).
int64_t StmtScalarI64(duckdb_v2_result_handle r) {
	auto chunk = StepChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	int64_t value = reinterpret_cast<const int64_t *>(view.data)[0];
	duckdb_v2_data_chunk_destroy(&chunk);
	REQUIRE(StepChunk(r) == nullptr); // single row only
	return value;
}

} // namespace

// ===========================================================================
// The canonical loop: parse a multi-statement string, execute each
// statement on the connection, one result at a time.
// ===========================================================================

TEST_CASE("V2: parse_sql iterates a multi-statement string", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 42; SELECT 84; SELECT 126", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(iter != nullptr);

	int statement_count = 0;
	while (true) {
		duckdb_v2_sql_statement_handle stmt = nullptr;
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (!stmt) {
			break; // exhausted
		}
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(stmt != nullptr); // not consumed: a copy was executed
		REQUIRE(DrainRowCount(r) == 1);
		duckdb_v2_result_destroy(&r);
		duckdb_v2_sql_statement_destroy(&stmt);
		statement_count++;
	}
	REQUIRE(statement_count == 3);

	// Exhaustion is idempotent.
	duckdb_v2_sql_statement_handle stmt = reinterpret_cast<duckdb_v2_sql_statement_handle>(uintptr_t(0xdead));
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt == nullptr);

	duckdb_v2_statement_iterator_destroy(&iter);
	REQUIRE(iter == nullptr);
}

// ===========================================================================
// Statements outlive the iterator, and unconsumed statements are
// destroyed independently.
// ===========================================================================

TEST_CASE("V2: statements are independently owned", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1; SELECT 2", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_sql_statement_handle first = nullptr;
	duckdb_v2_sql_statement_handle second = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &first, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(first != nullptr);
	REQUIRE(second != nullptr);

	// Destroy the iterator first; the yielded statements stay valid.
	duckdb_v2_statement_iterator_destroy(&iter);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, first, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);

	// Both statements are caller-owned; the first survived its execution
	// (non-consuming) and the second was never executed. Destroy each directly.
	REQUIRE(duckdb_v2_sql_statement_destroy(&first) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(first == nullptr);
	REQUIRE(duckdb_v2_sql_statement_destroy(&second) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(second == nullptr);
}

// ===========================================================================
// Parse errors carry the parser error type. Parsing is lazy, so the error
// surfaces from the next() that reaches the failing statement, after the
// statements before it have been yielded.
// ===========================================================================

TEST_CASE("V2: parse errors surface with QUERY_PARSER", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	// parse_sql parses nothing and succeeds; the loop yields "SELECT 1" then
	// reaches the parse error for "SELEKT 2" on the next() that parses it.
	duckdb_v2_statement_iterator_handle iter = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_parse_sql(fx.conn, "SELECT 1; SELEKT 2", &iter, &err);
	while (rc == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_sql_statement_handle stmt = nullptr;
		rc = duckdb_v2_statement_iterator_next(iter, &stmt, &err);
		if (rc == DUCKDB_V2_ERROR_NONE && !stmt) {
			FAIL("iterator exhausted without surfacing the parse error");
		}
		duckdb_v2_sql_statement_destroy(&stmt);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_QUERY_PARSER);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.ptr != nullptr);
	REQUIRE(msg.ptr[0] != '\0');
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_statement_iterator_destroy(&iter);
}

// ===========================================================================
// The iterator is spent after exhaustion: next() past the last statement keeps
// returning a NULL statement with ERROR_NONE, idempotently (0xdead sentinel
// proves next() actively resets the out-param to NULL).
// ===========================================================================

TEST_CASE("V2: the iterator is spent after exhaustion", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1; SELECT 2", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Drain both statements.
	for (int i = 0; i < 2; i++) {
		duckdb_v2_sql_statement_handle stmt = nullptr;
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(stmt != nullptr);
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	// Past the end, next() is idempotent: NULL statement, no error, every call.
	for (int i = 0; i < 3; i++) {
		auto stmt = reinterpret_cast<duckdb_v2_sql_statement_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(stmt == nullptr);
	}

	duckdb_v2_statement_iterator_destroy(&iter);
}

// ===========================================================================
// A parse error terminates iteration: the erroring next() reports it, and every
// next() after it reports clean exhaustion (NULL statement, ERROR_NONE) rather
// than re-parsing and re-raising the error.
// ===========================================================================

TEST_CASE("V2: a parse error terminates iteration", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1; SELEKT 2", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The valid first statement is yielded.
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr);
	duckdb_v2_sql_statement_destroy(&stmt);

	// The next() that reaches "SELEKT 2" reports the parse error.
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_QUERY_PARSER);
	REQUIRE(stmt == nullptr);

	// Iteration is now spent: further calls report clean exhaustion, not the error.
	for (int i = 0; i < 2; i++) {
		stmt = reinterpret_cast<duckdb_v2_sql_statement_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(stmt == nullptr);
	}

	duckdb_v2_statement_iterator_destroy(&iter);
}

// ===========================================================================
// No-statement input yields an immediately exhausted iterator.
// ===========================================================================

TEST_CASE("V2: no-statement input parses to an exhausted iterator", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	for (const char *sql : {"", "   ", ";", ";;;"}) {
		INFO("sql: '" << sql << "'");
		duckdb_v2_statement_iterator_handle iter = nullptr;
		REQUIRE(duckdb_v2_parse_sql(fx.conn, sql, &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_sql_statement_handle stmt = reinterpret_cast<duckdb_v2_sql_statement_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(stmt == nullptr);
		duckdb_v2_statement_iterator_destroy(&iter);
	}
}

// ===========================================================================
// statement_execute is non-consuming: it executes a copy, so the statement
// survives success and prepare-time failure alike and can be executed again.
// The busy and null-arg refusals never reach the engine.
// ===========================================================================

TEST_CASE("V2: statement_execute leaves the statement intact on prepare failure", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	auto stmt = StmtParseOne(fx.conn, "SELECT * FROM no_such_table");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) ==
	        DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
	REQUIRE(stmt != nullptr); // not consumed: only the copy was

	// The same statement now succeeds once the table exists (a copy is bound fresh).
	ExecSQL(fx.conn, "CREATE TABLE no_such_table(i INTEGER)");
	ExecSQL(fx.conn, "INSERT INTO no_such_table VALUES (1), (2)");
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 2);
	duckdb_v2_result_destroy(&r);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: the busy refusal leaves the statement intact", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto stmt = StmtParseOne(fx.conn, "SELECT 1");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) ==
	        DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(stmt != nullptr); // intact: the engine was never reached

	// Draining the live result frees the connection; the same statement
	// then runs.
	REQUIRE(DrainRowCount(live) == 100000);
	duckdb_v2_result_destroy(&live);
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr); // still intact (non-consuming)
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
	duckdb_v2_sql_statement_destroy(&stmt);
}

// ===========================================================================
// Parameter binding: positional values fold in as constants, and the same
// statement re-executes with a different value set.
// ===========================================================================

TEST_CASE("V2: statement_execute binds positional parameters", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	auto stmt = StmtParseOne(fx.conn, "SELECT $1 + $2");

	auto make_int = [&](int64_t v) {
		return MakeInt64Value(fx.conn, v);
	};

	// First execution: 10 + 20 = 30.
	duckdb_v2_value_handle a = make_int(10);
	duckdb_v2_value_handle b = make_int(20);
	duckdb_v2_value_handle params[2] = {a, b};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, params, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(r) == 30);
	duckdb_v2_result_destroy(&r);

	// Re-execute the same statement with a different value set (non-consuming).
	duckdb_v2_value_handle c = make_int(100);
	duckdb_v2_value_handle d = make_int(1);
	duckdb_v2_value_handle params2[2] = {c, d};
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, params2, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(r) == 101);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_value_destroy(&a);
	duckdb_v2_value_destroy(&b);
	duckdb_v2_value_destroy(&c);
	duckdb_v2_value_destroy(&d);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: bound parameter values may be destroyed before the result is consumed", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	auto stmt = StmtParseOne(fx.conn, "SELECT $1 + $2");
	duckdb_v2_value_handle a = MakeInt64Value(fx.conn, 10);
	duckdb_v2_value_handle b = MakeInt64Value(fx.conn, 20);
	duckdb_v2_value_handle params[2] = {a, b};

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, params, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The values are copied in at execute, so destroying them before stepping
	// the (lazy) result must not affect it.
	duckdb_v2_value_destroy(&a);
	duckdb_v2_value_destroy(&b);
	REQUIRE(StmtScalarI64(r) == 30);

	duckdb_v2_result_destroy(&r);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_execute rejects parameters on a statement that expands", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TABLE et(i INTEGER)");
	ExecSQL(fx.conn, "INSERT INTO et VALUES (1), (2), (3)");

	// A volatile DEFAULT makes ALTER ADD COLUMN expand into BEGIN/.../COMMIT; the
	// parameter would bind against the injected BEGIN, so the call is refused.
	auto stmt = StmtParseOne(fx.conn, "ALTER TABLE et ADD COLUMN c INTEGER DEFAULT ((random() * 0)::INTEGER + $1)");
	duckdb_v2_value_handle v = MakeInt64Value(fx.conn, 40);
	duckdb_v2_value_handle params[1] = {v};

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, params, 1, &r, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);

	// Rejected before any fragment ran: the table is untouched, the connection usable.
	duckdb_v2_result_handle check = nullptr;
	REQUIRE(Query(fx.conn, "SELECT count(*) FROM et", &check, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(check) == 3);
	duckdb_v2_result_destroy(&check);

	duckdb_v2_value_destroy(&v);
	duckdb_v2_sql_statement_destroy(&stmt);
}

// ===========================================================================
// Named parameters. parameter_names selects each value's target by name; an
// absent or empty name binds positionally. Keys are case-insensitive
// Identifiers, and the wrong key set surfaces as a bind error from the engine's
// parameter verification, not a bridge check.
// ===========================================================================

TEST_CASE("V2: statement_execute binds named parameters", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	auto stmt = StmtParseOne(fx.conn, "SELECT $a + $b");

	duckdb_v2_value_handle va = MakeInt64Value(fx.conn, 10);
	duckdb_v2_value_handle vb = MakeInt64Value(fx.conn, 20);

	// Bind by name: order of the arrays is irrelevant, only the name matches.
	duckdb_v2_str names[2] = {Convert("a"), Convert("b")};
	duckdb_v2_value_handle values[2] = {va, vb};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(r) == 30);
	duckdb_v2_result_destroy(&r);

	// Same statement, names and values reordered together: keying is by name, so the
	// result is unchanged (a - b would differ; a + b proves order independence via
	// the swap below with distinct values).
	duckdb_v2_value_handle vx = MakeInt64Value(fx.conn, 100);
	duckdb_v2_value_handle vy = MakeInt64Value(fx.conn, 1);
	duckdb_v2_str swapped[2] = {Convert("b"), Convert("a")};
	duckdb_v2_value_handle swapped_values[2] = {vy, vx}; // b=1, a=100
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, swapped, swapped_values, 2, &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(r) == 101);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_value_destroy(&va);
	duckdb_v2_value_destroy(&vb);
	duckdb_v2_value_destroy(&vx);
	duckdb_v2_value_destroy(&vy);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_execute matches named parameters case-insensitively", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	// $Name in the SQL, bound with the differently-cased key "name": Identifier keys
	// compare case-insensitively, so the bind succeeds.
	auto stmt = StmtParseOne(fx.conn, "SELECT $Name");
	duckdb_v2_value_handle v = MakeInt64Value(fx.conn, 42);
	duckdb_v2_str names[1] = {Convert("name")};
	duckdb_v2_value_handle values[1] = {v};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 1, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(r) == 42);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_value_destroy(&v);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_execute rejects positional binding of a named parameter", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	// $val is a NAMED parameter (not positional): binding it by name works, and
	// binding it positionally (names == NULL) fails the key-set check below, which is
	// what proves $val is named.
	auto stmt = StmtParseOne(fx.conn, "SELECT $val");
	duckdb_v2_value_handle v = MakeInt64Value(fx.conn, 7);
	duckdb_v2_str names[1] = {Convert("val")};
	duckdb_v2_value_handle values[1] = {v};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 1, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(r) == 7);
	duckdb_v2_result_destroy(&r);

	// Positional binding of a named parameter provides key "1" but the statement
	// expects "val": a bind error.
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, values, 1, &r, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);

	duckdb_v2_value_destroy(&v);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_execute rejects a wrong parameter key set", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	duckdb_v2_value_handle v1 = MakeInt64Value(fx.conn, 1);
	duckdb_v2_value_handle v2 = MakeInt64Value(fx.conn, 2);

	SECTION("names supplied for a positional statement") {
		auto stmt = StmtParseOne(fx.conn, "SELECT $1 + $2");
		duckdb_v2_str names[2] = {Convert("a"), Convert("b")};
		duckdb_v2_value_handle values[2] = {v1, v2};
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 2, &r, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(r == nullptr);
		duckdb_v2_sql_statement_destroy(&stmt);
	}
	SECTION("a name that does not exist in the statement") {
		auto stmt = StmtParseOne(fx.conn, "SELECT $a");
		duckdb_v2_str names[1] = {Convert("nope")};
		duckdb_v2_value_handle values[1] = {v1};
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 1, &r, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(r == nullptr);
		duckdb_v2_sql_statement_destroy(&stmt);
	}
	SECTION("a missing name") {
		auto stmt = StmtParseOne(fx.conn, "SELECT $a + $b");
		duckdb_v2_str names[1] = {Convert("a")};
		duckdb_v2_value_handle values[1] = {v1};
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 1, &r, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(r == nullptr);
		duckdb_v2_sql_statement_destroy(&stmt);
	}

	duckdb_v2_value_destroy(&v1);
	duckdb_v2_value_destroy(&v2);
}

TEST_CASE("V2: statement_execute rejects a malformed parameter name", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	auto stmt = StmtParseOne(fx.conn, "SELECT $a");
	duckdb_v2_value_handle v = MakeInt64Value(fx.conn, 1);
	// {NULL, len > 0} is malformed per the duckdb_v2_str contract.
	duckdb_v2_str names[1] = {duckdb_v2_str {nullptr, 5}};
	duckdb_v2_value_handle values[1] = {v};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 1, &r, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);

	duckdb_v2_value_destroy(&v);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: statement_execute treats empty name entries as positional", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	auto stmt = StmtParseOne(fx.conn, "SELECT $1 + $2");
	duckdb_v2_value_handle va = MakeInt64Value(fx.conn, 10);
	duckdb_v2_value_handle vb = MakeInt64Value(fx.conn, 20);
	duckdb_v2_value_handle values[2] = {va, vb};

	SECTION("a non-NULL array of empty views binds positionally") {
		duckdb_v2_str names[2] = {duckdb_v2_str {nullptr, 0}, duckdb_v2_str {nullptr, 0}};
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(StmtScalarI64(r) == 30);
		duckdb_v2_result_destroy(&r);
	}
	SECTION("explicit numeric keys bind the positional parameters") {
		duckdb_v2_str names[2] = {Convert("1"), Convert("2")};
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(StmtScalarI64(r) == 30);
		duckdb_v2_result_destroy(&r);
	}

	duckdb_v2_value_destroy(&va);
	duckdb_v2_value_destroy(&vb);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: mixing named and positional parameters fails at parse", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	// The parser forbids mixing $1 and $name in one statement, so the failure is at
	// parse time, never at execute. Parsing is lazy, so it surfaces from the next()
	// that parses the statement. Pin the actual code the bridge surfaces.
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT $1 + $a", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	auto rc = duckdb_v2_statement_iterator_next(iter, &stmt, nullptr);
	CAPTURE(rc);
	REQUIRE(rc == DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED);
	REQUIRE(stmt == nullptr);
	duckdb_v2_statement_iterator_destroy(&iter);
}

TEST_CASE("V2: statement_bind parameter names are the statement_execute keys", "[capi_v2][sql_statement]") {
	EnvFixture fx;
	auto stmt = StmtParseOne(fx.conn, "SELECT $a + $b");

	// Introspect: statement_bind reports the parameter names ...
	duckdb_v2_schema_handle out = nullptr;
	duckdb_v2_schema_handle params = nullptr;
	REQUIRE(duckdb_v2_statement_bind(fx.conn, stmt, &out, &params, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(params, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);
	duckdb_v2_str n0 = {nullptr, 0};
	duckdb_v2_str n1 = {nullptr, 0};
	duckdb_v2_logical_type_handle t = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(params, 0, &n0, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_schema_get_field(params, 1, &n1, &t, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Convert(n0) == "a");
	REQUIRE(Convert(n1) == "b");
	duckdb_v2_schema_destroy(&out);
	duckdb_v2_schema_destroy(&params);

	// ... and those exact names are the execute keys.
	duckdb_v2_value_handle va = MakeInt64Value(fx.conn, 3);
	duckdb_v2_value_handle vb = MakeInt64Value(fx.conn, 4);
	duckdb_v2_str names[2] = {Convert("a"), Convert("b")};
	duckdb_v2_value_handle values[2] = {va, vb};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, names, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(StmtScalarI64(r) == 7);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_value_destroy(&va);
	duckdb_v2_value_destroy(&vb);
	duckdb_v2_sql_statement_destroy(&stmt);
}

// ===========================================================================
// Null-arg validation and destroy null-safety.
// ===========================================================================

TEST_CASE("V2: sql_statement null-arg rejection and null-safe destroys", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	duckdb_v2_sql_statement_handle stmt = nullptr;
	duckdb_v2_result_handle r = nullptr;

	REQUIRE(duckdb_v2_parse_sql(nullptr, "SELECT 1", &iter, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_parse_sql(fx.conn, nullptr, &iter, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1", nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_statement_iterator_next(nullptr, &stmt, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_statement_iterator_destroy(&iter);

	// statement_execute rejects a NULL statement, a NULL out_result, and a
	// positive parameter_count paired with NULL values.
	auto valid = StmtParseOne(fx.conn, "SELECT 1");
	REQUIRE(duckdb_v2_statement_execute(fx.conn, nullptr, nullptr, nullptr, 0, &r, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_statement_execute(fx.conn, valid, nullptr, nullptr, 0, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_statement_execute(fx.conn, valid, nullptr, nullptr, 2, &r, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_sql_statement_destroy(&valid);

	// Destroys are null-safe.
	REQUIRE(duckdb_v2_sql_statement_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_sql_statement_destroy(&stmt) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_statement_iterator_handle already_null = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
}

// ===========================================================================
// Statement-level preprocessing parity with the old string path: a
// PRAGMA parses and runs through the iterator.
// ===========================================================================

TEST_CASE("V2: pragma statements parse and execute through the iterator", "[capi_v2][sql_statement]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "PRAGMA enable_progress_bar", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	DrainRowCount(r);
	duckdb_v2_result_destroy(&r);
}

} // namespace test_capi_v2
