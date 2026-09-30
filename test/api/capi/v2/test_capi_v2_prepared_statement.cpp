#include "test_capi_v2.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 prepared_statement tests: the opt-in cached-execution path. Prepare once,
// execute repeatedly, and ask whether the plan is actually reused. The result
// handle is the same one statement_execute returns and behaves identically, so
// a block of these tests is parity against that path.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {
namespace {

// Parse exactly one statement from sql (raw, unbound). Unique name to avoid a
// unity-build clash with the same-shaped helper in other test files.
duckdb_v2_sql_statement_handle PsParseOne(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(conn, sql, &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stmt != nullptr);
	duckdb_v2_statement_iterator_destroy(&iter);
	return stmt;
}

// Prepares one statement from sql, destroying the parsed statement afterwards (it is
// borrowed, never consumed). Returns nullptr when the prepare fails; `out_rc` takes the
// code so the caller can assert on it.
duckdb_v2_prepared_statement_handle PsPrepare(duckdb_v2_connection_handle conn, const char *sql,
                                              bool require_cacheable = false, DUCKDB_V2_ERROR *out_rc = nullptr) {
	auto stmt = PsParseOne(conn, sql);
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	auto rc = duckdb_v2_prepared_statement_create(conn, stmt, require_cacheable, &prepared, nullptr);
	if (out_rc) {
		*out_rc = rc;
	}
	REQUIRE(stmt != nullptr); // borrowed, not consumed
	duckdb_v2_sql_statement_destroy(&stmt);
	return prepared;
}

bool PsReusesPlan(duckdb_v2_connection_handle conn, const char *sql) {
	auto prepared = PsPrepare(conn, sql);
	REQUIRE(prepared != nullptr);
	bool reuses = false;
	REQUIRE(duckdb_v2_prepared_statement_reuses_plan(prepared, &reuses, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_prepared_statement_destroy(&prepared);
	return reuses;
}

// Drains every chunk of a single-column BIGINT result into a vector. STANDARD_VECTOR_SIZE
// can be as low as 2 in the assertion build, so this must not assume one chunk.
std::vector<int64_t> PsDrainBigints(duckdb_v2_result_handle r) {
	std::vector<int64_t> out;
	while (auto chunk = StepChunk(r)) {
		idx_t size = 0;
		REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_handle vec = nullptr;
		REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_vector_view view {};
		REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
		for (idx_t i = 0; i < size; i++) {
			out.push_back(reinterpret_cast<const int64_t *>(view.data)[SelAt(view.sel, i)]);
		}
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	return out;
}

// The single BIGINT of a one-row, one-column result; pins end-of-stream.
int64_t PsScalarI64(duckdb_v2_result_handle r) {
	auto rows = PsDrainBigints(r);
	REQUIRE(rows.size() == 1);
	return rows[0];
}

// Executes a prepared statement with positional BIGINT parameters and returns its rows.
std::vector<int64_t> PsExecuteWith(duckdb_v2_connection_handle conn, duckdb_v2_prepared_statement_handle prepared,
                                   const std::vector<int64_t> &params) {
	std::vector<duckdb_v2_value_handle> values;
	for (auto param : params) {
		values.push_back(MakeInt64Value(conn, param));
	}
	duckdb_v2_result_handle r = nullptr;
	auto rc = duckdb_v2_prepared_statement_execute(prepared, nullptr, values.empty() ? nullptr : values.data(),
	                                               static_cast<idx_t>(values.size()), &r, nullptr);
	for (auto &value : values) {
		duckdb_v2_value_destroy(&value);
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	auto rows = PsDrainBigints(r);
	duckdb_v2_result_destroy(&r);
	return rows;
}

// Seeds t(x BIGINT) with 1..4.
void PsSeedTable(duckdb_v2_connection_handle conn) {
	ExecSQL(conn, "CREATE TABLE t(x BIGINT)");
	ExecSQL(conn, "INSERT INTO t VALUES (1), (2), (3), (4)");
}

} // namespace

// ===========================================================================
// Prepare once, execute many. The handle is non-consuming in both directions:
// the parsed statement survives the prepare, and the prepared statement
// survives every execution.
// ===========================================================================

TEST_CASE("V2: a prepared statement executes repeatedly", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	auto prepared = PsPrepare(fx.conn, "SELECT x FROM t WHERE x > $1 ORDER BY x");
	REQUIRE(prepared != nullptr);

	// Three executions from one handle, each with its own values; nothing carries over.
	REQUIRE(PsExecuteWith(fx.conn, prepared, {0}) == std::vector<int64_t> {1, 2, 3, 4});
	REQUIRE(PsExecuteWith(fx.conn, prepared, {2}) == std::vector<int64_t> {3, 4});
	REQUIRE(PsExecuteWith(fx.conn, prepared, {9}).empty());
	// And again with the first value, to pin that no state accumulated.
	REQUIRE(PsExecuteWith(fx.conn, prepared, {0}) == std::vector<int64_t> {1, 2, 3, 4});

	duckdb_v2_prepared_statement_destroy(&prepared);
	REQUIRE(prepared == nullptr);
}

TEST_CASE("V2: prepared_statement_create borrows the statement", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto stmt = PsParseOne(fx.conn, "SELECT 42::BIGINT");

	// The same statement prepares twice and still executes directly: only copies are ever
	// consumed.
	duckdb_v2_prepared_statement_handle first = nullptr;
	duckdb_v2_prepared_statement_handle second = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, &first, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(first != nullptr);
	REQUIRE(second != nullptr);
	REQUIRE(first != second);

	REQUIRE(PsExecuteWith(fx.conn, first, {}) == std::vector<int64_t> {42});
	REQUIRE(PsExecuteWith(fx.conn, second, {}) == std::vector<int64_t> {42});

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(PsScalarI64(r) == 42);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_prepared_statement_destroy(&first);
	duckdb_v2_prepared_statement_destroy(&second);
	duckdb_v2_sql_statement_destroy(&stmt);
}

// ===========================================================================
// Plan reuse, reported rather than assumed.
// ===========================================================================

TEST_CASE("V2: prepared_statement_reuses_plan reports reuse honestly", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	// Nothing unresolved and nothing to invalidate: the plan is reused.
	REQUIRE(PsReusesPlan(fx.conn, "SELECT 42"));
	// A parameter whose type the cast anchors at prepare time: still reused.
	REQUIRE(PsReusesPlan(fx.conn, "SELECT $1::INTEGER + 1"));
	// A table scan re-binds every execution so a catalog change is picked up.
	REQUIRE_FALSE(PsReusesPlan(fx.conn, "SELECT x FROM t WHERE x = $1"));
	// Unanchored parameters: the types are unknown until values arrive.
	REQUIRE_FALSE(PsReusesPlan(fx.conn, "SELECT $1 + $2"));
}

TEST_CASE("V2: require_cacheable accepts a reused plan", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto rc = DUCKDB_V2_ERROR_NONE;
	auto prepared = PsPrepare(fx.conn, "SELECT $1::BIGINT + 1", true, &rc);
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(prepared != nullptr);

	bool reuses = false;
	REQUIRE(duckdb_v2_prepared_statement_reuses_plan(prepared, &reuses, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(reuses);
	REQUIRE(PsExecuteWith(fx.conn, prepared, {41}) == std::vector<int64_t> {42});

	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: require_cacheable rejects a re-bound plan", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	auto rc = DUCKDB_V2_ERROR_NONE;
	auto prepared = PsPrepare(fx.conn, "SELECT x FROM t WHERE x = $1", true, &rc);
	REQUIRE(rc == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(prepared == nullptr);

	// The same statement prepares fine without the flag; the caller then knows what it got.
	REQUIRE_FALSE(PsReusesPlan(fx.conn, "SELECT x FROM t WHERE x = $1"));
}

// ===========================================================================
// Parameters. Same arrays, same rules, same error codes as statement_execute.
// ===========================================================================

TEST_CASE("V2: prepared_statement_execute binds positional parameters", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT $1 - $2");
	REQUIRE(prepared != nullptr);

	// Positional binding is order-sensitive: $1 is element 0.
	REQUIRE(PsExecuteWith(fx.conn, prepared, {10, 4}) == std::vector<int64_t> {6});
	REQUIRE(PsExecuteWith(fx.conn, prepared, {4, 10}) == std::vector<int64_t> {-6});

	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: prepared_statement_execute binds named parameters", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT $a - $b");
	REQUIRE(prepared != nullptr);

	duckdb_v2_value_handle va = MakeInt64Value(fx.conn, 10);
	duckdb_v2_value_handle vb = MakeInt64Value(fx.conn, 4);

	// Keyed by name, so the array order is irrelevant: the same pairing both ways.
	duckdb_v2_str names[2] = {Convert("a"), Convert("b")};
	duckdb_v2_value_handle values[2] = {va, vb};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, names, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(PsScalarI64(r) == 6);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_str reversed[2] = {Convert("b"), Convert("a")};
	duckdb_v2_value_handle reversed_values[2] = {vb, va};
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, reversed, reversed_values, 2, &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(PsScalarI64(r) == 6);
	duckdb_v2_result_destroy(&r);

	// Case-insensitive, like every other identifier key.
	duckdb_v2_str upper[2] = {Convert("A"), Convert("B")};
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, upper, values, 2, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(PsScalarI64(r) == 6);
	duckdb_v2_result_destroy(&r);

	// A key set the statement does not have is a bind error, and the values are not consumed.
	duckdb_v2_str wrong[2] = {Convert("a"), Convert("c")};
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, wrong, values, 2, &r, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr);

	duckdb_v2_value_destroy(&va);
	duckdb_v2_value_destroy(&vb);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: prepared_statement_execute copies its parameter values", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT $1::BIGINT");
	REQUIRE(prepared != nullptr);

	// Destroy the value before a single row is read: it was copied in, so the still-lazy
	// result is unaffected.
	duckdb_v2_value_handle v = MakeInt64Value(fx.conn, 7);
	duckdb_v2_value_handle values[1] = {v};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, values, 1, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&v);
	REQUIRE(PsScalarI64(r) == 7);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_prepared_statement_destroy(&prepared);
}

// ===========================================================================
// Parity: the result handle is the one statement_execute returns.
// ===========================================================================

TEST_CASE("V2: a prepared result carries the same rows and schema", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	const char *sql = "SELECT x, x * 2 AS doubled FROM t ORDER BY x";
	auto prepared = PsPrepare(fx.conn, sql);
	REQUIRE(prepared != nullptr);

	duckdb_v2_result_handle prepared_result = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 0, &prepared_result, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	// Metadata is available before the first step, as on the stateless path.
	REQUIRE(ColumnCount(prepared_result) == 2);
	RequireColumn(prepared_result, 0, "x", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	RequireColumn(prepared_result, 1, "doubled", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	DUCKDB_V2_RESULT_TYPE result_type = DUCKDB_V2_RESULT_TYPE_NOTHING;
	REQUIRE(duckdb_v2_result_get_result_type(prepared_result, &result_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(result_type == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);
	DUCKDB_V2_STATEMENT_TYPE statement_type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(prepared_result, &statement_type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(statement_type == DUCKDB_V2_STATEMENT_TYPE_SELECT);

	REQUIRE(PsDrainBigints(prepared_result) == std::vector<int64_t> {1, 2, 3, 4});
	duckdb_v2_result_destroy(&prepared_result);

	// The stateless path agrees, column for column.
	duckdb_v2_result_handle direct = nullptr;
	REQUIRE(Query(fx.conn, sql, &direct) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ColumnCount(direct) == 2);
	RequireColumn(direct, 0, "x", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	RequireColumn(direct, 1, "doubled", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	REQUIRE(PsDrainBigints(direct) == std::vector<int64_t> {1, 2, 3, 4});
	duckdb_v2_result_destroy(&direct);

	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: a prepared DML reports its changed-row count", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	auto prepared = PsPrepare(fx.conn, "INSERT INTO t VALUES ($1)");
	REQUIRE(prepared != nullptr);

	for (int64_t value : {10, 20, 30}) {
		duckdb_v2_value_handle v = MakeInt64Value(fx.conn, value);
		duckdb_v2_value_handle values[1] = {v};
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, values, 1, &r, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_RESULT_TYPE result_type = DUCKDB_V2_RESULT_TYPE_NOTHING;
		REQUIRE(duckdb_v2_result_get_result_type(r, &result_type, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(result_type == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);
		REQUIRE(DrainChangedRows(r) == 1);
		duckdb_v2_result_destroy(&r);
		duckdb_v2_value_destroy(&v);
	}

	// The inserts landed, so the side effects of a prepared execution are real.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT count(*) FROM t", &r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(PsScalarI64(r) == 7);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: a prepared statement re-binds after a catalog change", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	auto prepared = PsPrepare(fx.conn, "SELECT * FROM t ORDER BY x");
	REQUIRE(prepared != nullptr);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ColumnCount(r) == 1);
	REQUIRE(DrainRowCount(r) == 4);
	duckdb_v2_result_destroy(&r);

	// A table scan is re-bound every execution, which is exactly what makes the new
	// column visible without re-preparing.
	ExecSQL(fx.conn, "ALTER TABLE t ADD COLUMN y BIGINT DEFAULT 9");
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ColumnCount(r) == 2);
	RequireColumn(r, 1, "y", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	REQUIRE(DrainRowCount(r) == 4);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_prepared_statement_destroy(&prepared);
}

// ===========================================================================
// Lifetimes. Both directions of "the other handle keeps working".
// ===========================================================================

TEST_CASE("V2: a result outlives its prepared statement", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	auto prepared = PsPrepare(fx.conn, "SELECT x FROM t ORDER BY x");
	REQUIRE(prepared != nullptr);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	// Destroy the prepared statement mid-stream: the result owns its session independently.
	duckdb_v2_prepared_statement_destroy(&prepared);
	REQUIRE(PsDrainBigints(r) == std::vector<int64_t> {1, 2, 3, 4});
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: a prepared statement outlives its connection", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	PsSeedTable(fx.conn);

	duckdb_v2_connection_handle other = nullptr;
	REQUIRE(duckdb_v2_connect(fx.db, &other, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto prepared = PsPrepare(other, "SELECT x FROM t ORDER BY x");
	REQUIRE(prepared != nullptr);

	// The handle keeps the session alive, the same guarantee an undrained result carries.
	duckdb_v2_disconnect(&other);
	REQUIRE(other == nullptr);
	REQUIRE(PsExecuteWith(fx.conn, prepared, {}) == std::vector<int64_t> {1, 2, 3, 4});

	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: prepared_statement_destroy is null-safe and idempotent", "[capi_v2][prepared_statement]") {
	REQUIRE(duckdb_v2_prepared_statement_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_destroy(&prepared) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(prepared == nullptr);

	EnvFixture fx;
	prepared = PsPrepare(fx.conn, "SELECT 1");
	REQUIRE(prepared != nullptr);
	REQUIRE(duckdb_v2_prepared_statement_destroy(&prepared) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(prepared == nullptr);
	REQUIRE(duckdb_v2_prepared_statement_destroy(&prepared) == DUCKDB_V2_ERROR_NONE);
}

// ===========================================================================
// One live result per connection, in both directions, and the error path that
// has to hand the connection back.
// ===========================================================================

TEST_CASE("V2: prepared_statement_create refuses while a result is live", "[capi_v2][prepared_statement]") {
	EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live) == DUCKDB_V2_ERROR_NONE);

	auto stmt = PsParseOne(fx.conn, "SELECT 1");
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	// Preparing would run the engine's cleanup and cancel the live stream, so it refuses
	// before reaching the engine, leaving the statement intact.
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, &prepared, nullptr) ==
	        DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(prepared == nullptr);
	REQUIRE(stmt != nullptr);

	// The live result is untouched by the refusal, and preparing works once it is gone.
	REQUIRE(DrainRowCount(live) == 100000);
	duckdb_v2_result_destroy(&live);
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, &prepared, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(prepared != nullptr);

	duckdb_v2_prepared_statement_destroy(&prepared);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: prepared_statement_execute refuses while a result is live", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT 1::BIGINT");
	REQUIRE(prepared != nullptr);

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 0, &r, nullptr) ==
	        DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(r == nullptr);

	REQUIRE(DrainRowCount(live) == 100000);
	duckdb_v2_result_destroy(&live);
	REQUIRE(PsExecuteWith(fx.conn, prepared, {}) == std::vector<int64_t> {1});

	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: a live prepared result blocks statement_execute", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT i FROM range(100000) t(i)");
	REQUIRE(prepared != nullptr);

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 0, &live, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// The slot the prepared path claims is the one the stateless path checks.
	auto stmt = PsParseOne(fx.conn, "SELECT 1");
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) ==
	        DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(r == nullptr);

	// Destroying the prepared result frees the connection even undrained.
	duckdb_v2_result_destroy(&live);
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, nullptr, nullptr, 0, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_sql_statement_destroy(&stmt);
	duckdb_v2_prepared_statement_destroy(&prepared);
}

TEST_CASE("V2: a failed prepared execution frees the connection", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto prepared = PsPrepare(fx.conn, "SELECT $a");
	REQUIRE(prepared != nullptr);

	// Positional values for a named parameter: a bind error, raised after the slot was
	// claimed, so this pins that the failure path releases it.
	duckdb_v2_value_handle v = MakeInt64Value(fx.conn, 1);
	duckdb_v2_value_handle values[1] = {v};
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, values, 1, &r, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr);
	duckdb_v2_value_destroy(&v);

	// The connection is usable again.
	REQUIRE(Query(fx.conn, "SELECT 1", &r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);

	// And so is the prepared statement: a failed execution does not consume it.
	duckdb_v2_value_handle named = MakeInt64Value(fx.conn, 7);
	duckdb_v2_value_handle named_values[1] = {named};
	duckdb_v2_str names[1] = {Convert("a")};
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, names, named_values, 1, &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(PsScalarI64(r) == 7);
	duckdb_v2_result_destroy(&r);
	duckdb_v2_value_destroy(&named);

	duckdb_v2_prepared_statement_destroy(&prepared);
}

// ===========================================================================
// Prepare-time errors and argument rejection.
// ===========================================================================

TEST_CASE("V2: prepared_statement_create surfaces a catalog error", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto stmt = PsParseOne(fx.conn, "SELECT * FROM no_such_table");

	duckdb_v2_prepared_statement_handle prepared = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	// The typed error survives the prepare, so the code is the catalog one rather than a
	// generic failure.
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, &prepared, &err) ==
	        DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(prepared == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(msg.ptr, msg.len).find("no_such_table") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);

	// The connection is not left busy by the failure.
	REQUIRE(stmt != nullptr);
	ExecSQL(fx.conn, "CREATE TABLE no_such_table(i BIGINT)");
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, &prepared, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_prepared_statement_destroy(&prepared);
	duckdb_v2_sql_statement_destroy(&stmt);
}

TEST_CASE("V2: prepared_statement functions guard null arguments", "[capi_v2][prepared_statement]") {
	EnvFixture fx;
	auto stmt = PsParseOne(fx.conn, "SELECT 1::BIGINT");
	duckdb_v2_prepared_statement_handle prepared = nullptr;

	REQUIRE(duckdb_v2_prepared_statement_create(nullptr, stmt, false, &prepared, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, nullptr, false, &prepared, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(prepared == nullptr);

	REQUIRE(duckdb_v2_prepared_statement_create(fx.conn, stmt, false, &prepared, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_destroy(&stmt);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_prepared_statement_execute(nullptr, nullptr, nullptr, 0, &r, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 0, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	// A count without values, and a null value inside the array, are both rejected.
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, 1, &r, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);
	duckdb_v2_value_handle null_values[1] = {nullptr};
	REQUIRE(duckdb_v2_prepared_statement_execute(prepared, nullptr, null_values, 1, &r, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);

	bool reuses = false;
	REQUIRE(duckdb_v2_prepared_statement_reuses_plan(nullptr, &reuses, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_prepared_statement_reuses_plan(prepared, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Every rejection above left the prepared statement usable.
	REQUIRE(PsExecuteWith(fx.conn, prepared, {}) == std::vector<int64_t> {1});
	duckdb_v2_prepared_statement_destroy(&prepared);
}

} // namespace test_capi_v2
