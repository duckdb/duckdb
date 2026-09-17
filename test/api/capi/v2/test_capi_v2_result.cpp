#include "test_capi_v2.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace test_capi_v2 {

namespace {

idx_t CollectionRowCount(duckdb_v2_column_data_collection_handle collection) {
	idx_t count = 0;
	REQUIRE(duckdb_v2_column_data_collection_row_count(collection, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	return count;
}

duckdb_v2_result_stream_handle OpenStream(duckdb_v2_result_handle &r) {
	duckdb_v2_result_stream_handle stream = nullptr;
	REQUIRE(duckdb_v2_result_stream_create(&r, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr);
	return stream;
}

idx_t StreamFetchRowCount(duckdb_v2_result_stream_handle stream) {
	idx_t total = 0;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		REQUIRE(duckdb_v2_result_stream_fetch(stream, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		if (!chunk) {
			return total;
		}
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		duckdb_v2_data_chunk_destroy(&chunk);
		total += size;
	}
}

bool CanStream(duckdb_v2_result_handle r) {
	bool can_stream = false;
	REQUIRE(duckdb_v2_result_can_stream(r, &can_stream, nullptr) == DUCKDB_V2_ERROR_NONE);
	return can_stream;
}

} // namespace

TEST_CASE("V2: statement_execute SELECT returns QUERY_RESULT with INTEGER column", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1 AS one", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);

	REQUIRE(ColumnCount(r) == 1);
	RequireColumn(r, 0, "one", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	REQUIRE(DrainRowCount(r) == 1);

	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r == nullptr);
}

TEST_CASE("V2: statement_execute multi-column SELECT", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1::INTEGER AS a, 'hi' AS b, 3.14::DOUBLE AS c", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	REQUIRE(ColumnCount(r) == 3);

	struct {
		const char *name;
		DUCKDB_V2_LOGICAL_TYPE_ID id;
	} expected[] = {
	    {"a", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER},
	    {"b", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR},
	    {"c", DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE},
	};
	for (idx_t i = 0; i < 3; i++) {
		RequireColumn(r, i, expected[i].name, expected[i].id);
	}

	duckdb_v2_result_destroy(&r);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: the cursor reads a multi-chunk SELECT to the end", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	idx_t total_rows = 0;
	idx_t chunk_count = 0;
	while (auto chunk = FetchChunk(r)) {
		idx_t size = 0;
		duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
		total_rows += size;
		chunk_count++;
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	REQUIRE(total_rows == 100000);
	REQUIRE(chunk_count > 1); // genuinely multi-chunk

	for (int i = 0; i < 3; i++) {
		auto chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_result_fetch(r, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(chunk == nullptr);
	}

	duckdb_v2_result_destroy(&r);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: metadata valid before, during, and after consumption", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i AS steady FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto check_metadata = [&](const char *phase) {
		INFO("phase: " << phase);
		DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
		REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);
		DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
		REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);
		REQUIRE(ColumnCount(r) == 1);
		RequireColumn(r, 0, "steady", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	};

	check_metadata("before the first call");

	idx_t consumed = 0;
	auto chunk = FetchChunk(r);
	REQUIRE(chunk != nullptr);
	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	consumed += size;
	duckdb_v2_data_chunk_destroy(&chunk);
	check_metadata("mid-cursor");

	consumed += DrainRowCount(r);
	REQUIRE(consumed == 100000);
	check_metadata("after the end");

	duckdb_v2_result_destroy(&r);
}
#endif

TEST_CASE("V2: DDL returns NOTHING and no rows", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "CREATE TABLE u (i INTEGER)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	duckdb_v2_result_get_result_type(r, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_NOTHING);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	duckdb_v2_result_get_statement_type(r, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_CREATE);

	REQUIRE(DrainRowCount(r) == 0);

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: INSERT / UPDATE / DELETE report their changed-row count", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_result_handle ins = nullptr;
	REQUIRE(Query(fx.conn, "INSERT INTO t VALUES (1), (2), (3)", &ins, nullptr) == DUCKDB_V2_ERROR_NONE);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	duckdb_v2_result_get_result_type(ins, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);

	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	duckdb_v2_result_get_statement_type(ins, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_INSERT);

	REQUIRE(DrainChangedRows(ins) == 3);
	duckdb_v2_result_destroy(&ins);

	duckdb_v2_result_handle upd = nullptr;
	Query(fx.conn, "UPDATE t SET i = i + 10 WHERE i >= 2", &upd, nullptr);
	duckdb_v2_result_get_statement_type(upd, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_UPDATE);
	REQUIRE(DrainChangedRows(upd) == 2);
	duckdb_v2_result_destroy(&upd);

	duckdb_v2_result_handle del = nullptr;
	Query(fx.conn, "DELETE FROM t WHERE i = 1", &del, nullptr);
	duckdb_v2_result_get_statement_type(del, &st, nullptr);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_DELETE);
	REQUIRE(DrainChangedRows(del) == 1);
	duckdb_v2_result_destroy(&del);
}

TEST_CASE("V2: the changed-row count is 0 when the WHERE matches nothing", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_result_handle ins = nullptr;
	Query(fx.conn, "INSERT INTO t VALUES (1), (2)", &ins, nullptr);
	REQUIRE(DrainChangedRows(ins) == 2);
	duckdb_v2_result_destroy(&ins);

	duckdb_v2_result_handle upd = nullptr;
	REQUIRE(Query(fx.conn, "UPDATE t SET i = i WHERE 1=0", &upd, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	duckdb_v2_result_get_result_type(upd, &rt, nullptr);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS);
	REQUIRE(DrainChangedRows(upd) == 0);
	duckdb_v2_result_destroy(&upd);

	duckdb_v2_result_handle del = nullptr;
	Query(fx.conn, "DELETE FROM t WHERE 1=0", &del, nullptr);
	REQUIRE(DrainChangedRows(del) == 0);
	duckdb_v2_result_destroy(&del);
}

TEST_CASE("V2: CHANGED_ROWS exposes the synthetic Count column via the schema", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_result_handle ins = nullptr;
	Query(fx.conn, "INSERT INTO t VALUES (1)", &ins, nullptr);

	REQUIRE(ColumnCount(ins) == 1);
	// The synthetic column name is set by core; pin the current spelling
	// so a future rename surfaces here loud rather than silent.
	RequireColumn(ins, 0, "Count", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	duckdb_v2_result_destroy(&ins);
}

TEST_CASE("V2: NOTHING result exposes the synthetic Count column with zero rows", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	Query(fx.conn, "CREATE TABLE t (i INTEGER)", &r, nullptr);

	REQUIRE(ColumnCount(r) == 1);
	RequireColumn(r, 0, "Count", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	REQUIRE(DrainRowCount(r) == 0);

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: an eagerly submitted result completes on the engine's own threads", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "SET threads=4");

	duckdb_v2_sql_statement_handle stmt = nullptr;
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT i FROM range(10000) t(i)", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);

	ExecuteArgs args;
	REQUIRE(duckdb_v2_execute_args_set_eagerness(args, DUCKDB_V2_RESULT_EAGERNESS_FORCED, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, args, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_destroy(&stmt);
	duckdb_v2_statement_iterator_destroy(&iter);

	REQUIRE_FALSE(CanStream(r));

	// The round count and the intermediate statuses are timing-dependent, so the loop latches.
	auto poll_rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	bool ever_ready = false;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
	while (status != DUCKDB_V2_RESULT_STATUS_FINISHED && std::chrono::steady_clock::now() < deadline) {
		poll_rc = duckdb_v2_result_poll(r, &status, nullptr);
		if (poll_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		ever_ready = ever_ready || status == DUCKDB_V2_RESULT_STATUS_READY;
		std::this_thread::yield();
	}
	REQUIRE(poll_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);
	// No producer ever parks for the retention decision on an eager result.
	REQUIRE_FALSE(ever_ready);
	REQUIRE_FALSE(CanStream(r));

	duckdb_v2_column_data_collection_handle collection = nullptr;
	REQUIRE(duckdb_v2_result_get_collection(r, &collection, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CollectionRowCount(collection) == 10000);

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: materialize, poll, cursor, get_collection, take_collection", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "SET threads=4");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(10000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(CanStream(r));
	REQUIRE(duckdb_v2_result_materialize(r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(CanStream(r));

	auto poll_rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
	while (status != DUCKDB_V2_RESULT_STATUS_FINISHED && std::chrono::steady_clock::now() < deadline) {
		poll_rc = duckdb_v2_result_poll(r, &status, nullptr);
		if (poll_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		std::this_thread::yield();
	}
	REQUIRE(poll_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);

	REQUIRE(DrainRowCount(r) == 10000);

	duckdb_v2_column_data_collection_handle borrowed = nullptr;
	REQUIRE(duckdb_v2_result_get_collection(r, &borrowed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CollectionRowCount(borrowed) == 10000);

	duckdb_v2_column_data_collection_handle owned = nullptr;
	REQUIRE(duckdb_v2_result_take_collection(r, &owned, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CollectionRowCount(owned) == 10000);

	REQUIRE(duckdb_v2_result_poll(r, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);
	REQUIRE(duckdb_v2_result_step(r, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);
	REQUIRE(duckdb_v2_result_complete(r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(FetchChunk(r) == nullptr);
	duckdb_v2_column_data_collection_handle again = nullptr;
	REQUIRE(duckdb_v2_result_get_collection(r, &again, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(again == nullptr);
	REQUIRE(duckdb_v2_result_take_collection(r, &again, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	TextSinkTarget rendered;
	REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &rendered,
	                                    nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(rendered.text.empty());

	duckdb_v2_result_destroy(&r);
	REQUIRE(CollectionRowCount(owned) == 10000);
	duckdb_v2_column_data_collection_destroy(&owned);
}

TEST_CASE("V2: destroying a result polled to FINISHED commits it too", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_connection_handle observer = nullptr;
	REQUIRE(duckdb_v2_connection_create(fx.db, &observer, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_sql_statement_handle stmt = nullptr;
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "INSERT INTO t VALUES (1), (2), (3)", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);

	ExecuteArgs args;
	REQUIRE(duckdb_v2_execute_args_set_eagerness(args, DUCKDB_V2_RESULT_EAGERNESS_FORCED, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, args, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_destroy(&stmt);
	duckdb_v2_statement_iterator_destroy(&iter);

	auto poll_rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
	while (status != DUCKDB_V2_RESULT_STATUS_FINISHED && std::chrono::steady_clock::now() < deadline) {
		poll_rc = duckdb_v2_result_poll(r, &status, nullptr);
		if (poll_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		std::this_thread::yield();
	}
	REQUIRE(poll_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);

	// Destroying a result whose query has run keeps what it did, rows taken or not.
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle after = nullptr;
	REQUIRE(Query(observer, "SELECT count(*) FROM t", &after, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ScalarBigint(after) == 3);
	duckdb_v2_result_destroy(&after);
	duckdb_v2_connection_destroy(&observer);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: stepping a zero-row SELECT to FINISHED leaves it streamable", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(1000) t(i) WHERE i < 0", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// No row is ever produced, so nothing parks for the retention decision and stepping reaches
	// the end. The round count is a planning detail, so the loop latches.
	auto step_rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	for (int i = 0; i < 100000 && status != DUCKDB_V2_RESULT_STATUS_FINISHED; i++) {
		step_rc = duckdb_v2_result_step(r, &status, nullptr);
		if (step_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		if (status == DUCKDB_V2_RESULT_STATUS_BLOCKED || status == DUCKDB_V2_RESULT_STATUS_NO_TASKS_AVAILABLE) {
			step_rc = duckdb_v2_result_wait(r, nullptr);
			if (step_rc != DUCKDB_V2_ERROR_NONE) {
				break;
			}
		}
	}
	REQUIRE(step_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);

	// Stepping took no rows, so the choice is still open.
	REQUIRE(CanStream(r));
	auto stream = OpenStream(r);
	REQUIRE(StreamFetchRowCount(stream) == 0);
	duckdb_v2_result_stream_destroy(&stream);
}
#endif

TEST_CASE("V2: a result polled to FINISHED commits when its rows are taken", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_connection_handle observer = nullptr;
	REQUIRE(duckdb_v2_connection_create(fx.db, &observer, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_sql_statement_handle stmt = nullptr;
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "INSERT INTO t VALUES (1), (2)", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);

	ExecuteArgs args;
	REQUIRE(duckdb_v2_execute_args_set_eagerness(args, DUCKDB_V2_RESULT_EAGERNESS_FORCED, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, args, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_destroy(&stmt);
	duckdb_v2_statement_iterator_destroy(&iter);

	auto poll_rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
	while (status != DUCKDB_V2_RESULT_STATUS_FINISHED && std::chrono::steady_clock::now() < deadline) {
		poll_rc = duckdb_v2_result_poll(r, &status, nullptr);
		if (poll_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		std::this_thread::yield();
	}
	REQUIRE(poll_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);

	// The rows are still the result's, so the connection is busy and nothing is committed yet.
	duckdb_v2_result_handle blocked = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &blocked, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	duckdb_v2_result_handle before = nullptr;
	REQUIRE(Query(observer, "SELECT count(*) FROM t", &before, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ScalarBigint(before) == 0);
	duckdb_v2_result_destroy(&before);

	REQUIRE(duckdb_v2_result_complete(r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainChangedRows(r) == 2);
	duckdb_v2_result_destroy(&r);

	duckdb_v2_result_handle after = nullptr;
	REQUIRE(Query(observer, "SELECT count(*) FROM t", &after, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ScalarBigint(after) == 2);
	duckdb_v2_result_destroy(&after);
	duckdb_v2_connection_destroy(&observer);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: step on an unsettled result parks at READY until a decision is made", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "SET threads=1");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// How many steps it takes to produce the first chunk is a planning detail, so the loop latches.
	auto step_rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	for (int i = 0; i < 100000 && status != DUCKDB_V2_RESULT_STATUS_READY; i++) {
		step_rc = duckdb_v2_result_step(r, &status, nullptr);
		if (step_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
	}
	REQUIRE(step_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_READY);

	for (int i = 0; i < 5; i++) {
		REQUIRE(duckdb_v2_result_step(r, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(status == DUCKDB_V2_RESULT_STATUS_READY);
	}
	REQUIRE(CanStream(r));

	auto stream = OpenStream(r);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_result_stream_fetch(stream, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk != nullptr);
	idx_t size = 0;
	duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
	REQUIRE(size > 0);
	duckdb_v2_data_chunk_destroy(&chunk);

	duckdb_v2_result_stream_destroy(&stream);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: the blocking stream fetch drains both buffer shapes", "[capi_v2][query_result]") {
	EnvFixture fx;

	// A batch-index-ordered scan uses the batched buffer; turning insertion order off uses the simple one.
	for (const char *ordering : {"SET preserve_insertion_order=true", "SET preserve_insertion_order=false"}) {
		INFO("ordering: " << ordering);
		ExecSQL(fx.conn, ordering);

		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		auto stream = OpenStream(r);
		REQUIRE(StreamFetchRowCount(stream) == 100000);

		duckdb_v2_data_chunk_handle chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
		REQUIRE(duckdb_v2_result_stream_fetch(stream, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(chunk == nullptr);

		duckdb_v2_result_stream_destroy(&stream);
	}
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: try_fetch alone drains a stream while the engine's threads produce", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "SET threads=4");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto stream = OpenStream(r);

	// The consumer runs no engine work at all: only try_fetch, which never blocks.
	auto rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	idx_t total = 0;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
	while (status != DUCKDB_V2_RESULT_STATUS_FINISHED && std::chrono::steady_clock::now() < deadline) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		rc = duckdb_v2_result_stream_try_fetch(stream, &chunk, &status, nullptr);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		if (chunk) {
			idx_t size = 0;
			duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
			total += size;
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		std::this_thread::yield();
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);
	REQUIRE(total == 100000);

	duckdb_v2_result_stream_destroy(&stream);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: try_fetch, step and wait drain a stream with no worker threads", "[capi_v2][query_result]") {
	EnvFixture fx;
	// One thread, all of it the consumer's: nothing runs unless the consumer steps.
	ExecSQL(fx.conn, "SET threads=1");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto stream = OpenStream(r);

	auto rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	idx_t total = 0;
	for (int i = 0; i < 1000000 && status != DUCKDB_V2_RESULT_STATUS_FINISHED; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		rc = duckdb_v2_result_stream_try_fetch(stream, &chunk, &status, nullptr);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		if (chunk) {
			idx_t size = 0;
			duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
			total += size;
			duckdb_v2_data_chunk_destroy(&chunk);
			continue;
		}
		if (status == DUCKDB_V2_RESULT_STATUS_FINISHED) {
			break;
		}
		rc = duckdb_v2_result_stream_step(stream, &status, nullptr);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		if (status == DUCKDB_V2_RESULT_STATUS_BLOCKED || status == DUCKDB_V2_RESULT_STATUS_NO_TASKS_AVAILABLE) {
			rc = duckdb_v2_result_stream_wait(stream, nullptr);
			if (rc != DUCKDB_V2_ERROR_NONE) {
				break;
			}
		}
		status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);
	REQUIRE(total == 100000);

	duckdb_v2_result_stream_destroy(&stream);
}
#endif

TEST_CASE("V2: result_stream_create refuses a result whose rows are already kept", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(1000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_materialize(r, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_stream_handle stream = reinterpret_cast<duckdb_v2_result_stream_handle>(uintptr_t(0xdead));
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_result_stream_create(&r, &stream, &err) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr); // consumed on failure too
	REQUIRE(stream == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	duckdb_v2_result_handle next = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &next, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(next) == 1);
	duckdb_v2_result_destroy(&next);
}

TEST_CASE("V2: result_stream_create refuses an eager statement", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "INSERT INTO t VALUES (1)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(CanStream(r));

	duckdb_v2_result_stream_handle stream = nullptr;
	REQUIRE(duckdb_v2_result_stream_create(&r, &stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);
	REQUIRE(stream == nullptr);

	duckdb_v2_result_handle count = nullptr;
	REQUIRE(Query(fx.conn, "SELECT count(*) FROM t", &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ScalarBigint(count) == 0);
	duckdb_v2_result_destroy(&count);
}

TEST_CASE("V2: result_stream_create refuses an eager submission", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_sql_statement_handle stmt = nullptr;
	duckdb_v2_statement_iterator_handle iter = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT 1", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);

	ExecuteArgs args;
	REQUIRE(duckdb_v2_execute_args_set_eagerness(args, DUCKDB_V2_RESULT_EAGERNESS_FORCED, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, args, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_sql_statement_destroy(&stmt);
	duckdb_v2_statement_iterator_destroy(&iter);

	REQUIRE_FALSE(CanStream(r));
	duckdb_v2_result_stream_handle stream = nullptr;
	REQUIRE(duckdb_v2_result_stream_create(&r, &stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r == nullptr);
	REQUIRE(stream == nullptr);

	duckdb_v2_result_handle next = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &next, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(next) == 1);
	duckdb_v2_result_destroy(&next);
}

TEST_CASE("V2: wait on a fresh result leaves both consumption choices open", "[capi_v2][query_result]") {
	EnvFixture fx;

	SECTION("materialize after the wait") {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT i FROM range(1000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(CanStream(r));
		REQUIRE(duckdb_v2_result_complete(r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(DrainRowCount(r) == 1000);
		duckdb_v2_result_destroy(&r);
	}

	SECTION("a stream after the wait") {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT i FROM range(1000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_result_wait(r, nullptr) == DUCKDB_V2_ERROR_NONE);
		auto stream = OpenStream(r);
		REQUIRE(StreamFetchRowCount(stream) == 1000);
		duckdb_v2_result_stream_destroy(&stream);
	}
}

TEST_CASE("V2: non-SELECT statements run through bounded steps and are served from the cursor",
          "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	// A query-backed PRAGMA reparses into a plain SELECT, which is why it is the one entry here the
	// engine does not submit eagerly.
	struct Case {
		const char *sql;
		bool eager;
	} statements[] = {
	    {"INSERT INTO t VALUES (1), (2)", true}, {"UPDATE t SET i = i + 1", true},
	    {"DELETE FROM t WHERE i = 2", true},     {"INSERT INTO t VALUES (9) RETURNING i", true},
	    {"CREATE TABLE u (j INTEGER)", true},    {"DROP TABLE u", true},
	    {"SET memory_limit='1GB'", true},        {"CALL pragma_version()", true},
	    {"ATTACH ':memory:' AS other", true},    {"PRAGMA database_size", false},
	};
	for (auto &c : statements) {
		const char *sql = c.sql;
		INFO("sql: " << sql);
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);

		REQUIRE(CanStream(r) == !c.eager);
		REQUIRE(duckdb_v2_result_materialize(r, nullptr) == DUCKDB_V2_ERROR_NONE);

		// A bounded number of steps reaches the terminal state; the bound is what pins "bounded".
		auto step_rc = DUCKDB_V2_ERROR_NONE;
		auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
		for (int steps = 0; steps < 100000 && status != DUCKDB_V2_RESULT_STATUS_FINISHED; steps++) {
			step_rc = duckdb_v2_result_step(r, &status, nullptr);
			if (step_rc != DUCKDB_V2_ERROR_NONE) {
				break;
			}
			if (status == DUCKDB_V2_RESULT_STATUS_BLOCKED || status == DUCKDB_V2_RESULT_STATUS_NO_TASKS_AVAILABLE) {
				step_rc = duckdb_v2_result_wait(r, nullptr);
				if (step_rc != DUCKDB_V2_ERROR_NONE) {
					break;
				}
			}
		}
		REQUIRE(step_rc == DUCKDB_V2_ERROR_NONE);
		REQUIRE(status == DUCKDB_V2_RESULT_STATUS_FINISHED);

		duckdb_v2_column_data_collection_handle collection = nullptr;
		REQUIRE(duckdb_v2_result_get_collection(r, &collection, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(DrainRowCount(r) == CollectionRowCount(collection));

		duckdb_v2_result_destroy(&r);
	}
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: an interrupt from another thread cancels a blocking complete", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT sum(i) FROM range(2000000000) t(i) WHERE i % 3 <> 0", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	std::thread interrupter([&]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		duckdb_v2_connection_interrupt(fx.conn, nullptr);
	});

	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_result_complete(r, &err);
	interrupter.join();

	REQUIRE(rc == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_result_destroy(&r);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: an interrupt reaches a polling consumer as the sticky CANCELLED status", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "SET threads=4");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT sum(i) FROM range(2000000000) t(i) WHERE i % 3 <> 0", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_materialize(r, nullptr) == DUCKDB_V2_ERROR_NONE);

	std::thread interrupter([&]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		duckdb_v2_connection_interrupt(fx.conn, nullptr);
	});

	auto poll_rc = DUCKDB_V2_ERROR_NONE;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
	while (status != DUCKDB_V2_RESULT_STATUS_CANCELLED && std::chrono::steady_clock::now() < deadline) {
		poll_rc = duckdb_v2_result_poll(r, &status, nullptr);
		if (poll_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		std::this_thread::yield();
	}
	interrupter.join();
	REQUIRE(poll_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_CANCELLED);

	REQUIRE(duckdb_v2_result_poll(r, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_CANCELLED);
	REQUIRE(duckdb_v2_result_step(r, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(status == DUCKDB_V2_RESULT_STATUS_CANCELLED);
	REQUIRE(duckdb_v2_result_complete(r, nullptr) == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);

	duckdb_v2_result_destroy(&r);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: an interrupt from another thread cancels a blocking stream fetch", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(2000000000) t(i) WHERE i % 1000000 = 0", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	auto stream = OpenStream(r);

	std::thread interrupter([&]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		duckdb_v2_connection_interrupt(fx.conn, nullptr);
	});

	// Chunks may arrive before the interrupt lands; the loop latches and the assertions run once.
	auto rc = DUCKDB_V2_ERROR_NONE;
	for (int i = 0; i < 1000000; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		rc = duckdb_v2_result_stream_fetch(stream, &chunk, nullptr);
		bool produced = chunk != nullptr;
		if (produced) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		if (rc != DUCKDB_V2_ERROR_NONE || !produced) {
			break;
		}
	}
	interrupter.join();
	REQUIRE(rc == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT);

	duckdb_v2_result_stream_destroy(&stream);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: an execution error is sticky on the cursor", "[capi_v2][query_result]") {
	EnvFixture fx;

	// The cast only fails on the last row, so the failure is raised during execution, after
	// statement_execute has returned successfully.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn,
	              "SELECT (CASE WHEN i < 99999 THEN CAST(i AS VARCHAR) ELSE 'oops' END)::INT FROM range(100000) t(i)",
	              &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_error_info_handle err = nullptr;
	auto chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
	REQUIRE(duckdb_v2_result_fetch(r, &chunk, &err) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(chunk == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.len > 0);
	duckdb_v2_error_info_destroy(&err);

	REQUIRE(duckdb_v2_result_fetch(r, &chunk, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(chunk == nullptr);
	REQUIRE(duckdb_v2_result_complete(r, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	DUCKDB_V2_RESULT_STATUS status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	REQUIRE(duckdb_v2_result_step(r, &status, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(duckdb_v2_result_poll(r, &status, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);

	// Metadata still works after the failure (prepare-time information).
	REQUIRE(ColumnCount(r) == 1);

	duckdb_v2_result_destroy(&r);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: an execution error after the first chunk is sticky on a stream", "[capi_v2][query_result]") {
	EnvFixture fx;

	// Without a buffer far smaller than the result, the blocking fetch runs the whole query looking
	// for a full buffer and the error beats the first chunk.
	ExecSQL(fx.conn, "SET max_streaming_buffer_size='64KB'");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn,
	              "SELECT (CASE WHEN i < 999999 THEN CAST(i AS VARCHAR) ELSE 'oops' END)::INT FROM range(1000000) t(i)",
	              &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto stream = OpenStream(r);

	// Chunks arrive before the failing row; the loop latches, the assertions run once.
	auto rc = DUCKDB_V2_ERROR_NONE;
	idx_t chunks = 0;
	for (int i = 0; i < 1000000; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		rc = duckdb_v2_result_stream_fetch(stream, &chunk, nullptr);
		bool produced = chunk != nullptr;
		if (produced) {
			chunks++;
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		if (rc != DUCKDB_V2_ERROR_NONE || !produced) {
			break;
		}
	}
	REQUIRE(rc == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(chunks > 0);

	auto chunk = reinterpret_cast<duckdb_v2_data_chunk_handle>(uintptr_t(0xdead));
	REQUIRE(duckdb_v2_result_stream_fetch(stream, &chunk, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(chunk == nullptr);
	DUCKDB_V2_RESULT_STATUS status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	REQUIRE(duckdb_v2_result_stream_try_fetch(stream, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);

	duckdb_v2_result_stream_destroy(&stream);
}
#endif

TEST_CASE("V2: interrupt with no active query is a no-op", "[capi_v2][query_result]") {
	EnvFixture fx;

	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The stale interrupt flag must not poison the next query (the engine
	// resets it when a new query starts).
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 42", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: PIVOT expands to a group and its rows come from the cursor", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE sales (city VARCHAR, year INT, amount INT)");
	ExecSQL(fx.conn,
	        "INSERT INTO sales VALUES ('ams', 2023, 10), ('ams', 2024, 20), ('rtm', 2023, 30), ('rtm', 2024, 40)");

	// Auto-PIVOT parses to one raw statement (a MultiStatement) and
	// expands at statement_execute into CREATE TYPE (enum) + SELECT.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "PIVOT sales ON year USING sum(amount)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Metadata is deferred until the row-producing fragment is prepared, and so is can_stream.
	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	RequireSchemaDeferred(r);
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	bool can_stream = true;
	REQUIRE(duckdb_v2_result_can_stream(r, &can_stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_column_data_collection_handle collection = nullptr;
	REQUIRE(duckdb_v2_result_get_collection(r, &collection, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(CollectionRowCount(collection) == 2);

	REQUIRE(ColumnCount(r) == 3); // city + one column per year
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);
	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);

	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: PIVOT expands to a group and can be streamed", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE sales (city VARCHAR, year INT, amount INT)");
	ExecSQL(fx.conn,
	        "INSERT INTO sales VALUES ('ams', 2023, 10), ('ams', 2024, 20), ('rtm', 2023, 30), ('rtm', 2024, 40)");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "PIVOT sales ON year USING sum(amount)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto stream = OpenStream(r);
	REQUIRE(StreamFetchRowCount(stream) == 2);
	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_stream_get_statement_type(stream, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);
	duckdb_v2_result_stream_destroy(&stream);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: ALTER with a non-constant DEFAULT expands and executes fully", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(1000)");

	// Expands into BEGIN / ALTER ADD (DEFAULT NULL) / UPDATE / ALTER SET
	// DEFAULT / COMMIT, wrapped by the engine's preprocessor.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Metadata deferred while the group runs.
	RequireSchemaDeferred(r);

	// No fragment produces rows; the internal UPDATE's Count chunk is
	// discarded, exactly as ClientContext::Query discards it.
	REQUIRE(DrainRowCount(r) == 0);

	// Engine-mirrored principal: the last fragment (the injected COMMIT).
	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT;
	REQUIRE(duckdb_v2_result_get_result_type(r, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_NOTHING);
	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_get_statement_type(r, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_TRANSACTION);
	duckdb_v2_result_destroy(&r);

	// The whole group really ran: the column exists and is populated.
	REQUIRE(Query(fx.conn, "SELECT i FROM t WHERE c IS NOT NULL", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1000);
	duckdb_v2_result_destroy(&r);
}
#endif

TEST_CASE("V2: a finished expanded group leaves a later user transaction alone", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(10)");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_complete(r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Holding a finished result is the normal pattern, so destroying it must not reach into the
	// transaction the caller opened in the meantime.
	ExecSQL(fx.conn, "BEGIN TRANSACTION");
	ExecSQL(fx.conn, "INSERT INTO t VALUES (99, 1.0)");
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);
	ExecSQL(fx.conn, "COMMIT");

	duckdb_v2_result_handle count = nullptr;
	REQUIRE(Query(fx.conn, "SELECT count(*) FROM t WHERE i = 99", &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ScalarBigint(count) == 1);
	duckdb_v2_result_destroy(&count);
}

TEST_CASE("V2: query-backed PRAGMA reparses at statement_execute", "[capi_v2][query_result]") {
	EnvFixture fx;

	// Reparses one-to-one into a row-producing statement, so metadata is
	// available immediately, before the first call.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "PRAGMA database_size", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(ColumnCount(r) > 0);

	REQUIRE(DrainRowCount(r) >= 1);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: an error inside an expanded group is sticky and rolls back", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(100)");

	// The materializing UPDATE fails at execution: '0.xxxx' || 'x' does
	// not convert to INT.
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "ALTER TABLE t ADD COLUMN c INT DEFAULT ((random()::VARCHAR || 'x')::INT)", &r, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_result_complete(r, &err) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);

	DUCKDB_V2_RESULT_STATUS status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	REQUIRE(duckdb_v2_result_step(r, &status, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);
	duckdb_v2_result_destroy(&r);

	// The wrapped transaction rolled back: no column, no partial data.
	REQUIRE(Query(fx.conn, "SELECT c FROM t", &r, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(r == nullptr);

	REQUIRE(Query(fx.conn, "SELECT count(*) FROM t", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ScalarBigint(r) == 100);
	duckdb_v2_result_destroy(&r);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: interrupt during an expanded group cancels and rolls back", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(2000000)");

	QueryResult r;
	REQUIRE(Query(fx.conn, "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Drive part of the group, then interrupt.
	DUCKDB_V2_RESULT_STATUS status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	for (int i = 0; i < 20; i++) {
		REQUIRE(duckdb_v2_result_step(r, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(StepUntilCancelled(r) == DUCKDB_V2_RESULT_STATUS_CANCELLED);
	duckdb_v2_result_destroy(&r);

	// Nothing of the group was committed.
	duckdb_v2_result_handle probe = nullptr;
	REQUIRE(Query(fx.conn, "SELECT c FROM t", &probe, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);

	REQUIRE(Query(fx.conn, "SELECT 1", &probe, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(probe) == 1);
	duckdb_v2_result_destroy(&probe);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: destroying a half-executed expanded group is clean", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");
	ExecSQL(fx.conn, "INSERT INTO t SELECT * FROM range(2000000)");

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_RESULT_STATUS status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	for (int i = 0; i < 10; i++) {
		REQUIRE(duckdb_v2_result_step(r, &status, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	// The abandoned group was not committed, and the connection works.
	REQUIRE(Query(fx.conn, "SELECT c FROM t", &r, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(Query(fx.conn, "SELECT count(*) FROM t", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r) == 1);
	duckdb_v2_result_destroy(&r);
}
#endif

TEST_CASE("V2: statement_execute surfaces parser error and leaves out_result null", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(Query(fx.conn, "SELEKT 1", &r, &err) == DUCKDB_V2_ERROR_QUERY_PARSER);
	REQUIRE(r == nullptr);
	REQUIRE(err != nullptr);

	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.ptr != nullptr);
	REQUIRE(msg.len > 0); // detail propagated from the parser
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: statement_execute binder error (unknown table)", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(Query(fx.conn, "SELECT * FROM no_such_table", &r, &err) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_error_info_destroy(&err);
}

TEST_CASE("V2: statement_execute failure tolerates err == nullptr", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "BADSQL", &r, nullptr) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(r == nullptr);
}

TEST_CASE("V2: querying with a NULL statement is rejected", "[capi_v2][query_result]") {
	EnvFixture fx;

	// No-statement input parses to an immediately exhausted iterator; the
	// NULL statement it yields is rejected by statement_execute.
	for (const char *sql : {"", "   ", ";"}) {
		INFO("sql: '" << sql << "'");
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, sql, &r, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(r == nullptr);
	}
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: destroying an untouched result is clean", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(1000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	// The connection remains fully usable.
	duckdb_v2_result_handle next = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &next, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(next) == 1);
	duckdb_v2_result_destroy(&next);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: destroying a half-drained stream is clean", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(1000000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto stream = OpenStream(r);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_result_stream_fetch(stream, &chunk, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(chunk != nullptr);
	duckdb_v2_data_chunk_destroy(&chunk);
	REQUIRE(duckdb_v2_result_stream_destroy(&stream) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stream == nullptr);

	duckdb_v2_result_handle next = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &next, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(next) == 1);
	duckdb_v2_result_destroy(&next);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: a fetched chunk and a taken collection outlive result, connection and database",
          "[capi_v2][query_result]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_environment_create(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(OpenDatabase(env, duckdb_v2_str {nullptr, 0}, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_create(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(conn, "SELECT i, 'row-' || i AS s FROM range(100) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto chunk = FetchChunk(r);
	REQUIRE(chunk != nullptr);
	duckdb_v2_column_data_collection_handle collection = nullptr;
	REQUIRE(duckdb_v2_result_take_collection(r, &collection, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_destroy(&r);
	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_database_destroy(&db);
	duckdb_v2_environment_destroy(&env);

	// The chunk owns its data; producers are all gone.
	idx_t size = 0;
	REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 100);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.data != nullptr);
	REQUIRE(reinterpret_cast<const int64_t *>(view.data)[99] == 99);
	duckdb_v2_data_chunk_destroy(&chunk);

	REQUIRE(CollectionRowCount(collection) == 100);
	REQUIRE(duckdb_v2_column_data_collection_destroy(&collection) == DUCKDB_V2_ERROR_NONE);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: an unfinished result survives disconnect and close", "[capi_v2][query_result]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_database_handle db = nullptr;
	duckdb_v2_connection_handle conn = nullptr;
	REQUIRE(duckdb_v2_environment_create(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(OpenDatabase(env, duckdb_v2_str {nullptr, 0}, &db, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_create(db, &conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_materialize(r, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_connection_destroy(&conn);
	duckdb_v2_database_destroy(&db);

	// Metadata still reads off the handle, and consumption still works: the result keeps the
	// session alive itself.
	REQUIRE(ColumnCount(r) == 1);
	REQUIRE(DrainRowCount(r) == 100000);
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_environment_destroy(&env);
}
#endif

TEST_CASE("V2: result_get_schema outlives the result", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	Query(fx.conn, "SELECT 1 AS only_column", &r, nullptr);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_destroy(&r);

	// The schema is self-contained; it stays valid after the result is gone.
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle lt = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &name, &lt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Convert(name) == "only_column");
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	REQUIRE(duckdb_v2_logical_type_get_id(lt, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_schema_destroy(&schema);
}

TEST_CASE("V2: result_get_schema mirrors a SELECT's columns", "[capi_v2][query_result]") {
	EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1 AS a, 'x' AS b", &r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 2);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name.ptr, name.len) == "a");
	REQUIRE(duckdb_v2_schema_get_field(schema, 1, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(name.ptr, name.len) == "b");

	duckdb_v2_schema_destroy(&schema);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: result_get_schema of a CHANGED_ROWS statement is a BIGINT count", "[capi_v2][query_result]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TABLE t(a INTEGER)");
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "INSERT INTO t VALUES (1), (2)", &r) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_get_schema(r, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	idx_t count = 0;
	REQUIRE(duckdb_v2_schema_get_count(schema, &count, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(count == 1);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle type = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &name, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_LOGICAL_TYPE_ID id;
	REQUIRE(duckdb_v2_logical_type_get_id(type, &id, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	duckdb_v2_schema_destroy(&schema);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: a stream carries the same metadata the result did", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1 AS only_column", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto stream = OpenStream(r);

	DUCKDB_V2_RESULT_TYPE rt = DUCKDB_V2_RESULT_TYPE_NOTHING;
	REQUIRE(duckdb_v2_result_stream_get_result_type(stream, &rt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rt == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT);
	DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	REQUIRE(duckdb_v2_result_stream_get_statement_type(stream, &st, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(st == DUCKDB_V2_STATEMENT_TYPE_SELECT);

	duckdb_v2_schema_handle schema = nullptr;
	REQUIRE(duckdb_v2_result_stream_get_schema(stream, &schema, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_str name = {nullptr, 0};
	duckdb_v2_logical_type_handle lt = nullptr;
	REQUIRE(duckdb_v2_schema_get_field(schema, 0, &name, &lt, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Convert(name) == "only_column");
	duckdb_v2_schema_destroy(&schema);

	duckdb_v2_result_stream_destroy(&stream);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: statement_execute refuses while a live result exists", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Refused while live, with detail; the live result is untouched.
	duckdb_v2_result_handle second = reinterpret_cast<duckdb_v2_result_handle>(uintptr_t(0xdead));
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &second, &err) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(second == nullptr);
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	duckdb_v2_error_info_get_text(err, &msg);
	REQUIRE(msg.ptr != nullptr);
	REQUIRE(Convert(msg).find("live result") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);

	// Running the live result to the end frees the connection; the handle stays usable.
	REQUIRE(DrainRowCount(live) == 100000);
	REQUIRE(Query(fx.conn, "SELECT 2", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
	duckdb_v2_result_destroy(&live);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: a live stream also holds the connection", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto stream = OpenStream(r);

	duckdb_v2_result_handle second = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);

	duckdb_v2_result_stream_destroy(&stream);
	REQUIRE(Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: destroying an unfinished result frees the connection", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_handle second = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);

	duckdb_v2_result_destroy(&live);
	REQUIRE(Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
}
#endif

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: a cancelled result frees the connection once a call observes it", "[capi_v2][query_result]") {
	EnvFixture fx;

	QueryResult live;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(10000000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_materialize(live, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_interrupt(fx.conn, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The slot is released on the terminal transition, i.e. when a step observes the
	// cancellation, not by the interrupt itself.
	REQUIRE(StepUntilCancelled(live) == DUCKDB_V2_RESULT_STATUS_CANCELLED);

	QueryResult second;
	REQUIRE(Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(second) == 1);
}
#endif

TEST_CASE("V2: a sticky execution error frees the connection", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 'oops'::INT FROM range(10) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_complete(live, nullptr) == DUCKDB_V2_ERROR_TYPE_CONVERSION);

	duckdb_v2_result_handle second = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(second) == 1);
	duckdb_v2_result_destroy(&second);
	duckdb_v2_result_destroy(&live);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: a busy connection does not affect a second connection", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_connection_handle conn2 = nullptr;
	REQUIRE(duckdb_v2_connection_create(fx.db, &conn2, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(100000) t(i)", &live, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle other = nullptr;
	REQUIRE(Query(conn2, "SELECT 1", &other, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(other) == 1);
	duckdb_v2_result_destroy(&other);

	duckdb_v2_result_destroy(&live);
	duckdb_v2_connection_destroy(&conn2);
}
#endif

TEST_CASE("V2: results are independent; destroying one leaves the other usable", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle a = nullptr;
	duckdb_v2_result_handle b = nullptr;
	Query(fx.conn, "SELECT 1 AS aa", &a, nullptr);
	// a must reach a terminal state before the connection accepts b.
	REQUIRE(DrainRowCount(a) == 1);
	Query(fx.conn, "SELECT 'hi' AS bb", &b, nullptr);

	RequireColumn(a, 0, "aa", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	RequireColumn(b, 0, "bb", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);

	// Destroy a; b's schema is still fetchable.
	duckdb_v2_result_destroy(&a);
	RequireColumn(b, 0, "bb", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);

	duckdb_v2_result_destroy(&b);
}

TEST_CASE("V2: statement_type numeric round-trip for higher-numbered values", "[capi_v2][query_result]") {
	EnvFixture fx;

	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	struct Case {
		const char *sql;
		DUCKDB_V2_STATEMENT_TYPE expected;
	} cases[] = {
	    {"EXPLAIN SELECT 1", DUCKDB_V2_STATEMENT_TYPE_EXPLAIN},          // 14
	    {"DROP TABLE t", DUCKDB_V2_STATEMENT_TYPE_DROP},                 // 15
	    {"PRAGMA enable_progress_bar", DUCKDB_V2_STATEMENT_TYPE_PRAGMA}, // 17
	    {"SET memory_limit='1GB'", DUCKDB_V2_STATEMENT_TYPE_SET},        // 20
	    {"ATTACH ':memory:' AS other", DUCKDB_V2_STATEMENT_TYPE_ATTACH}, // 25
	};
	for (auto &c : cases) {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, c.sql, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		DUCKDB_V2_STATEMENT_TYPE st = DUCKDB_V2_STATEMENT_TYPE_INVALID;
		duckdb_v2_result_get_statement_type(r, &st, nullptr);
		REQUIRE(st == c.expected);
		duckdb_v2_result_destroy(&r);
	}
}

// Drift detector: probe the first numeric value past the highest core variant V2 currently mirrors.
// EnumUtil::ToString throws NotImplementedException for values not present in its lookup table; if a new
// variant is appended to duckdb::StatementType, the call will instead return a string and this assertion
// will fire, signalling that DUCKDB_V2_STATEMENT_TYPE needs a matching id.
TEST_CASE("V2: STATEMENT_TYPE has no gaps vs duckdb::StatementType", "[capi_v2][query_result]") {
	constexpr auto highest_known = static_cast<uint8_t>(duckdb::StatementType::EXTERNAL_RESOURCE_STATEMENT);
	auto probe = static_cast<duckdb::StatementType>(highest_known + 1);
	REQUIRE_THROWS_AS(duckdb::EnumUtil::ToString(probe), duckdb::NotImplementedException);
}

TEST_CASE("V2: query_progress reports idle values when no query is active", "[capi_v2][query_result]") {
	EnvFixture fx;

	auto progress = ReadProgress(fx.conn);
	REQUIRE(progress.percentage == -1.0);
	REQUIRE(progress.rows_processed == 0);
	REQUIRE(progress.total_rows_to_process == 0);

	// The snapshot destructor is null-safe.
	REQUIRE(duckdb_v2_query_progress_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_query_progress_handle already_null = nullptr;
	REQUIRE(duckdb_v2_query_progress_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
}

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2: query_progress advances while stepping a query", "[capi_v2][query_result]") {
	EnvFixture fx;

	// Single-threaded so all execution happens in our steps, and the
	// progress bar enabled so the engine publishes progress at all.
	for (const char *setup_sql : {"SET threads=1", "CREATE TABLE tbl AS SELECT range a FROM range(1000000)",
	                              "SET enable_progress_bar=true", "SET enable_progress_bar_print=false"}) {
		ExecSQL(fx.conn, setup_sql);
	}

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT sum(a) FROM tbl", &r, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_materialize(r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Progress restarts at 0 when the query begins and advances as steps
	// drive execution. Each snapshot is an independent owned object, and the
	// round count is timing-dependent, so both the step and the read latch.
	QueryProgress progress;
	bool saw_progress = false;
	auto step_rc = DUCKDB_V2_ERROR_NONE;
	bool progress_reads_ok = true;
	auto status = DUCKDB_V2_RESULT_STATUS_NOT_READY;
	while (status != DUCKDB_V2_RESULT_STATUS_FINISHED) {
		step_rc = duckdb_v2_result_step(r, &status, nullptr);
		if (step_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		bool read_ok = false;
		progress = ReadProgress(fx.conn, &read_ok);
		progress_reads_ok = progress_reads_ok && read_ok;
		if (progress.percentage > 0.0) {
			saw_progress = true;
		}
	}
	REQUIRE(step_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(progress_reads_ok);
	REQUIRE(saw_progress);
	REQUIRE(progress.rows_processed <= progress.total_rows_to_process);

	duckdb_v2_result_destroy(&r);
}
#endif

TEST_CASE("V2: result_render_box renders and leaves the result usable", "[capi_v2][query_result]") {
	EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1 AS one", &r) == DUCKDB_V2_ERROR_NONE);

	TextSinkTarget rendered;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &rendered,
	                                    &err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(r != nullptr); // non-consuming
	REQUIRE_FALSE(rendered.text.empty());
	// A box-drawing glyph and the column name are present.
	REQUIRE(rendered.text.find("\342\224\202") != std::string::npos);
	REQUIRE(rendered.text.find("one") != std::string::npos);

	TextSinkTarget again;
	REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &again,
	                                    nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(again.text == rendered.text);
	REQUIRE(DrainRowCount(r) == 1);
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	// The one-live-result slot was released: a new query runs on the same connection.
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 2 AS two", &r2) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r2) == 1);
	REQUIRE(duckdb_v2_result_destroy(&r2) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: result_render_box reports an execution error and stays destroyable", "[capi_v2][query_result]") {
	EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	// Execution is deferred: error() (volatile, so never folded) throws only when the result runs,
	// i.e. inside render_box's completion.
	REQUIRE(Query(fx.conn, "SELECT error('boom render') FROM range(5) t(i)", &r) == DUCKDB_V2_ERROR_NONE);

	TextSinkTarget text; // the sink must not be invoked on the error path
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &text, &err);
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	REQUIRE(text.text.empty());
	// The engine's error text propagated through the slot.
	REQUIRE(err != nullptr);
	duckdb_v2_str msg = {nullptr, 0};
	REQUIRE(duckdb_v2_error_info_get_text(err, &msg) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(std::string(msg.ptr ? msg.ptr : "", msg.len).find("boom render") != std::string::npos);
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(duckdb_v2_result_destroy(&r) == DUCKDB_V2_ERROR_NONE);

	// The busy slot was released despite the error: a new query runs.
	duckdb_v2_result_handle r2 = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 7", &r2) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(r2) == 1);
	REQUIRE(duckdb_v2_result_destroy(&r2) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2: result_render_box rejects null and out-of-range arguments", "[capi_v2][query_result]") {
	EnvFixture fx;
	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1 AS one", &r) == DUCKDB_V2_ERROR_NONE);

	TextSinkTarget text;
	REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, nullptr, &text, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_render_box(nullptr, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &text,
	                                    nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	// Malformed null_value (null pointer, nonzero length), and a render_mode out of range.
	REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 5}, 0, 0, AppendToString, &text, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 2, 0, AppendToString, &text, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(text.text.empty());

	// The result is untouched by every refusal.
	REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &text, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(text.text.empty());
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: result_render_box sink contract", "[capi_v2][query_result]") {
	EnvFixture fx;

	// Exactly one invocation, carrying the complete text: a caller can size off
	// text.len without waiting for more.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT * FROM range(200) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
		TextSinkTarget target;
		REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &target,
		                                    nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(target.calls == 1);
		REQUIRE_FALSE(target.text.empty());
		// A whole box, not a fragment: header rule, column name, and footer.
		REQUIRE(target.text.find("i") != std::string::npos);
		REQUIRE(target.text.find("rows") != std::string::npos);
		duckdb_v2_result_destroy(&r);
	}

	// A sink that populates the slot fails the call with its own code, and the
	// message it set survives back out to the caller.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT 1 AS one", &r) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_error_info_handle err = nullptr;
		REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, FailWithIOError, nullptr,
		                                    &err) == DUCKDB_V2_ERROR_IO_GENERAL);
		REQUIRE(err != nullptr);
		duckdb_v2_str message {nullptr, 0};
		REQUIRE(duckdb_v2_error_info_get_text(err, &message) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(Convert(message).find("sink could not write") != std::string::npos);
		duckdb_v2_error_info_destroy(&err);
		duckdb_v2_result_destroy(&r);
	}
}

TEST_CASE("V2: result_render_box null_value override and exact footer at the C boundary", "[capi_v2][query_result]") {
	EnvFixture fx;

	// Custom null text: appears instead of "NULL".
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT CAST(NULL AS INTEGER) AS b", &r) == DUCKDB_V2_ERROR_NONE);
		TextSinkTarget text;
		REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, Convert("<nil>"), 0, 0, AppendToString, &text, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(text.text.find("<nil>") != std::string::npos);
		REQUIRE(text.text.find("NULL") == std::string::npos);
		duckdb_v2_result_destroy(&r);
	}

	// max_rows bounds display but the footer counts every materialized row.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT i FROM range(100) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
		TextSinkTarget text;
		REQUIRE(duckdb_v2_result_render_box(r, 4, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &text,
		                                    nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(text.text.find("100 rows") != std::string::npos); // exact total
		REQUIRE(text.text.find("4 shown") != std::string::npos);  // display bounded to max_rows
		duckdb_v2_result_destroy(&r);
	}

	// The valid empty null_value forms both render the default "NULL":
	// {NULL, 0} (canonical empty view) and {ptr, 0} (non-null pointer, zero length).
	for (duckdb_v2_str empty_null : {duckdb_v2_str {nullptr, 0}, duckdb_v2_str {"", 0}}) {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT CAST(NULL AS INTEGER) AS b", &r) == DUCKDB_V2_ERROR_NONE);
		TextSinkTarget text;
		REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, empty_null, 0, 0, AppendToString, &text, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(text.text.find("NULL") != std::string::npos);
		duckdb_v2_result_destroy(&r);
	}
}

TEST_CASE("V2: result_render_box limit renders an approximate footer when the result fills the bound",
          "[capi_v2][query_result]") {
	EnvFixture fx;

	// The caller applied LIMIT 21 upstream and passes it as limit. The result
	// fills the bound exactly, so the true total is unknown: the footer renders
	// "? rows" instead of reporting the truncated count as exact.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT i FROM range(21) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
		TextSinkTarget text;
		REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 21, AppendToString, &text,
		                                    nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(text.text.find("? rows") != std::string::npos);  // count is unknown
		REQUIRE(text.text.find("21 rows") == std::string::npos); // never claims the bound as an exact total
		duckdb_v2_result_destroy(&r);
	}

	// The same result rendered with limit 0: the count is known and exact, so no
	// "? rows" approximation appears.
	{
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(Query(fx.conn, "SELECT i FROM range(21) t(i)", &r) == DUCKDB_V2_ERROR_NONE);
		TextSinkTarget text;
		REQUIRE(duckdb_v2_result_render_box(r, 0, 0, 0, duckdb_v2_str {nullptr, 0}, 0, 0, AppendToString, &text,
		                                    nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(text.text.find("? rows") == std::string::npos);
		duckdb_v2_result_destroy(&r);
	}
}

TEST_CASE("V2: an args handle is reusable across executions", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_statement_iterator_handle iter = nullptr;
	duckdb_v2_sql_statement_handle stmt = nullptr;
	REQUIRE(duckdb_v2_parse_sql(fx.conn, "SELECT $1::INTEGER + 1", &iter, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_statement_iterator_next(iter, &stmt, nullptr) == DUCKDB_V2_ERROR_NONE);

	ExecuteArgs args;
	auto value = MakeInt32Value(fx.conn, 41);
	duckdb_v2_value_handle values[1] = {value};
	REQUIRE(duckdb_v2_execute_args_set_statement_params(args, nullptr, values, 1, nullptr) == DUCKDB_V2_ERROR_NONE);
	// The values are copied in, so the caller's value can go away before the execution.
	duckdb_v2_value_destroy(&value);

	for (int i = 0; i < 2; i++) {
		duckdb_v2_result_handle r = nullptr;
		REQUIRE(duckdb_v2_statement_execute(fx.conn, stmt, args, &r, nullptr) == DUCKDB_V2_ERROR_NONE);
		auto chunk = FetchChunk(r);
		REQUIRE(chunk != nullptr);
		duckdb_v2_vector_handle vec = nullptr;
		duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
		duckdb_v2_vector_view view {};
		duckdb_v2_vector_get_view(vec, &view, nullptr);
		REQUIRE(reinterpret_cast<const int32_t *>(view.data)[0] == 42);
		duckdb_v2_data_chunk_destroy(&chunk);
		duckdb_v2_result_destroy(&r);
	}

	duckdb_v2_sql_statement_destroy(&stmt);
	duckdb_v2_statement_iterator_destroy(&iter);
}

TEST_CASE("V2: execute_args null-arg handling and destroy null-safety", "[capi_v2][query_result]") {
	REQUIRE(duckdb_v2_execute_args_create(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_execute_args_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_execute_args_handle already_null = nullptr;
	REQUIRE(duckdb_v2_execute_args_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);

	ExecuteArgs args;
	REQUIRE(duckdb_v2_execute_args_set_statement_params(nullptr, nullptr, nullptr, 0, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_execute_args_set_statement_params(args, nullptr, nullptr, 1, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_execute_args_set_eagerness(nullptr, DUCKDB_V2_RESULT_EAGERNESS_AUTO, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2: statement_execute null-arg rejection", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(nullptr, "SELECT 1", &r, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(Query(fx.conn, nullptr, &r, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(Query(fx.conn, "SELECT 1", nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2: result_destroy and result_stream_destroy are null-safe", "[capi_v2][query_result]") {
	REQUIRE(duckdb_v2_result_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_handle already_null = nullptr;
	REQUIRE(duckdb_v2_result_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(already_null == nullptr);

	REQUIRE(duckdb_v2_result_stream_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_result_stream_handle stream_null = nullptr;
	REQUIRE(duckdb_v2_result_stream_destroy(&stream_null) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stream_null == nullptr);
}

TEST_CASE("V2: result accessors reject null handle and null out-params", "[capi_v2][query_result]") {
	EnvFixture fx;

	DUCKDB_V2_RESULT_TYPE rt;
	DUCKDB_V2_STATEMENT_TYPE st;
	duckdb_v2_schema_handle schema = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	duckdb_v2_column_data_collection_handle collection = nullptr;
	DUCKDB_V2_RESULT_STATUS status;
	bool can_stream = false;

	REQUIRE(duckdb_v2_result_get_result_type(nullptr, &rt, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_get_statement_type(nullptr, &st, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_get_schema(nullptr, &schema, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_step(nullptr, &status, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_poll(nullptr, &status, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_wait(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_materialize(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_complete(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_fetch(nullptr, &chunk, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_get_collection(nullptr, &collection, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_take_collection(nullptr, &collection, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_can_stream(nullptr, &can_stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_interrupt(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_result_stream_handle stream = nullptr;
	REQUIRE(duckdb_v2_result_stream_create(nullptr, &stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_step(nullptr, &status, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_poll(nullptr, &status, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_wait(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_try_fetch(nullptr, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_fetch(nullptr, &chunk, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_get_schema(nullptr, &schema, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_get_result_type(nullptr, &rt, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_get_statement_type(nullptr, &st, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	double pct;
	uint64_t rows, total;
	duckdb_v2_query_progress_handle progress = nullptr;
	REQUIRE(duckdb_v2_connection_query_progress(nullptr, &progress, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_query_progress_get_percentage(nullptr, &pct, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_query_progress_get_rows_processed(nullptr, &rows, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_query_progress_get_total_rows_to_process(nullptr, &total, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_result_handle r = nullptr;
	Query(fx.conn, "SELECT 1", &r, nullptr);
	REQUIRE(duckdb_v2_result_get_result_type(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_get_statement_type(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_get_schema(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_step(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_poll(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_fetch(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_get_collection(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_take_collection(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_can_stream(r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_result_stream_create(&r, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(r != nullptr); // rejected before the transfer
	REQUIRE(duckdb_v2_connection_query_progress(fx.conn, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_query_progress(fx.conn, &progress, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_query_progress_get_percentage(progress, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_query_progress_get_rows_processed(progress, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_query_progress_get_total_rows_to_process(progress, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_query_progress_destroy(&progress);
	duckdb_v2_result_destroy(&r);
}

TEST_CASE("V2: statement_execute leaves pre-existing err untouched on success", "[capi_v2][query_result]") {
	EnvFixture fx;

	duckdb_v2_result_handle r = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	REQUIRE(Query(fx.conn, "BADSQL", &r, &err) == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	REQUIRE(err != nullptr);

	REQUIRE(Query(fx.conn, "SELECT 1", &r, &err) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(err != nullptr);
	DUCKDB_V2_ERROR code = DUCKDB_V2_ERROR_NONE;
	duckdb_v2_error_info_get_code(err, &code);
	REQUIRE(code == DUCKDB_V2_ERROR_DATABASE_CATALOG);
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_result_destroy(&r);
}

} // namespace test_capi_v2
