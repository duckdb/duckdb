#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/stream_query_result.hpp"

#include <thread>

using namespace duckdb;

TEST_CASE("Test Pending Query API", "[api][.]") {
	DuckDB db;
	Connection con(db);

	SECTION("Materialized result") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)");
		REQUIRE(!pending_query->HasError());
		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Streaming result") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)", true);
		REQUIRE(!pending_query->HasError());
		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Execute tasks") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)", true);
		while (pending_query->ExecuteTask() == PendingExecutionResult::RESULT_NOT_READY)
			;
		REQUIRE(!pending_query->HasError());
		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Create pending query while another pending query exists") {
		auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)");
		auto pending_query2 = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)", true);

		// first pending query is now closed
		REQUIRE_THROWS(pending_query->ExecuteTask());
		REQUIRE_THROWS(pending_query->Execute());

		// we can execute the second one
		auto result = pending_query2->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Binding error in pending query") {
		auto pending_query = con.PendingQuery("SELECT XXXSUM(i) FROM range(1000000) tbl(i)");
		REQUIRE(pending_query->HasError());
		REQUIRE_THROWS(pending_query->ExecuteTask());
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		auto result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Runtime error in pending query (materialized)") {
		// this succeeds initially
		auto pending_query =
		    con.PendingQuery("SELECT concat(SUM(i)::varchar, 'hello')::INT FROM range(1000000) tbl(i)");
		REQUIRE(!pending_query->HasError());
		// we only encounter the failure later on as we are executing the query
		auto result = pending_query->Execute();
		REQUIRE_FAIL(result);

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}

	SECTION("Runtime error in pending query (streaming)") {
		// this succeeds initially
		auto pending_query =
		    con.PendingQuery("SELECT concat(SUM(i)::varchar, 'hello')::INT FROM range(1000000) tbl(i)", true);
		REQUIRE(!pending_query->HasError());
		auto result = pending_query->Execute();
		REQUIRE(result->HasError());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Pending results errors as JSON") {
		con.Query("SET errors_as_json = true;");
		auto pending_query = con.PendingQuery("SELCT 32;");
		REQUIRE(pending_query->HasError());
		REQUIRE(duckdb::StringUtil::Contains(pending_query->GetError(), "SYNTAX_ERROR"));
	}
}

TEST_CASE("ART position scans keep planned RowGroups across checkpoint vacuum", "[api]") {
	DuckDB db;
	Connection query_con(db);
	Connection checkpoint_con(db);
	auto database_path = TestCreatePath("art_position_scan_checkpoint.db");

	// Keep row IDs stable while allowing checkpoint to replace the RowGroup tree.
	REQUIRE_NO_FAIL(query_con.Query("ATTACH '" + database_path +
	                                "' AS position_checkpoint (ROW_GROUP_SIZE 32768, "
	                                "STORAGE_VERSION 'v1.5.0', vacuum_rebuild_indexes 0)"));
	REQUIRE_NO_FAIL(query_con.Query("SET index_scan_percentage = 1.0"));
	REQUIRE_NO_FAIL(query_con.Query("SET index_scan_max_count = 0"));
	REQUIRE_NO_FAIL(query_con.Query("SET threads = 1"));
	REQUIRE_NO_FAIL(query_con.Query("CREATE TABLE position_checkpoint.t("
	                                "id BIGINT, bucket INTEGER, payload BIGINT USING COMPRESSION UNCOMPRESSED)"));
	REQUIRE_NO_FAIL(query_con.Query("INSERT INTO position_checkpoint.t SELECT i, 8, i FROM range(18449) r(i)"));
	REQUIRE_NO_FAIL(query_con.Query("CHECKPOINT position_checkpoint"));
	REQUIRE_NO_FAIL(query_con.Query("INSERT INTO position_checkpoint.t "
	                                "SELECT i, CASE WHEN i < 20497 THEN 7 ELSE 8 END, i "
	                                "FROM range(18449, 32768) r(i)"));
	REQUIRE_NO_FAIL(query_con.Query("CREATE INDEX t_bucket ON position_checkpoint.t USING ART(bucket)"));

	auto row_groups = query_con.Query("SELECT count(DISTINCT row_group_id) "
	                                  "FROM pragma_storage_info('position_checkpoint.t') "
	                                  "WHERE column_name = 'id' AND segment_type = 'BIGINT'");
	REQUIRE(CHECK_COLUMN(row_groups, 0, {2}));

	auto plan = query_con.Query("EXPLAIN ANALYZE SELECT id, payload FROM position_checkpoint.t WHERE bucket = 7");
	REQUIRE_NO_FAIL(*plan);
	REQUIRE(StringUtil::Contains(plan->ToString(), "Type: Index Scan"));

	auto pending = query_con.PendingQuery("SELECT count(*), min(id), max(id), sum(payload)::BIGINT, "
	                                      "bool_and(id = payload) FROM position_checkpoint.t WHERE bucket = 7");
	REQUIRE(!pending->HasError());
	// Initializing this single pipeline creates its global scan state without executing its data task.
	REQUIRE(pending->ExecuteTask() == PendingExecutionResult::RESULT_NOT_READY);

	REQUIRE_NO_FAIL(checkpoint_con.Query("CHECKPOINT position_checkpoint"));
	row_groups = checkpoint_con.Query("SELECT count(DISTINCT row_group_id) "
	                                  "FROM pragma_storage_info('position_checkpoint.t') "
	                                  "WHERE column_name = 'id' AND segment_type = 'BIGINT'");
	const auto row_groups_merged = CHECK_COLUMN(row_groups, 0, {1});

	auto result = pending->Execute();
	REQUIRE(row_groups_merged);
	REQUIRE(!result->HasError());
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(2048)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(18449)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(20496)}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(39879680)}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(true)}));
}

TEST_CASE("ART position scans release the rollback lock between output batches", "[api]") {
	auto database_path = TestCreatePath("art_position_scan_append_rollback.db");
	DuckDB db(database_path);
	Connection query_con(db);
	Connection commit_con(db);

	REQUIRE_NO_FAIL(query_con.Query("SET index_scan_percentage = 1.0"));
	REQUIRE_NO_FAIL(query_con.Query("SET index_scan_max_count = 0"));
	REQUIRE_NO_FAIL(query_con.Query("SET threads = 1"));
	REQUIRE_NO_FAIL(query_con.Query("SET streaming_buffer_size = '16KB'"));
	REQUIRE_NO_FAIL(query_con.Query("CREATE TABLE position_rollback("
	                                "id BIGINT, bucket INTEGER, payload BIGINT USING COMPRESSION UNCOMPRESSED)"));
	REQUIRE_NO_FAIL(query_con.Query("INSERT INTO position_rollback "
	                                "SELECT CASE WHEN i = 10000 THEN 100000 ELSE i END, "
	                                "CASE WHEN i < 10000 THEN 7 ELSE 8 END, "
	                                "CASE WHEN i = 10000 THEN 300000 ELSE i * 3 END "
	                                "FROM range(10001) r(i)"));
	REQUIRE_NO_FAIL(query_con.Query("CREATE INDEX position_rollback_bucket ON position_rollback USING ART(bucket)"));
	auto plan = query_con.Query("EXPLAIN ANALYZE SELECT id, payload FROM position_rollback WHERE bucket = 7");
	REQUIRE_NO_FAIL(*plan);
	REQUIRE(StringUtil::Contains(plan->ToString(), "Type: Index Scan"));

	REQUIRE_NO_FAIL(query_con.Query("SET debug_force_commit_failure = true"));
	REQUIRE_NO_FAIL(commit_con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(commit_con.Query("INSERT INTO position_rollback "
	                                 "SELECT i, 7, i * 3 FROM range(10001, 10101) r(i)"));

	auto stream = query_con.SendQuery("SELECT id, payload FROM position_rollback WHERE bucket = 7");
	REQUIRE(!stream->HasError());
	REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
	auto chunk = stream->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() > 0);
	REQUIRE(chunk->size() < 8192);

	// The failed commit appends to and truncates the tail RowGroup between position-scan output batches.
	auto commit_result = commit_con.Query("COMMIT");
	REQUIRE_FAIL(commit_result);
	REQUIRE(StringUtil::Contains(commit_result->GetError(), "Forced commit failure"));

	idx_t result_count = 0;
	int64_t payload_sum = 0;
	bool values_match = true;
	do {
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			const auto id = chunk->GetValue(0, row_idx).GetValue<int64_t>();
			const auto payload = chunk->GetValue(1, row_idx).GetValue<int64_t>();
			payload_sum += payload;
			values_match = values_match && payload == id * 3;
		}
		result_count += chunk->size();
		chunk = stream->Fetch();
	} while (chunk);
	REQUIRE(result_count == 10000);
	REQUIRE(payload_sum == 149985000);
	REQUIRE(values_match);
	REQUIRE_NO_FAIL(query_con.Query("SET debug_force_commit_failure = false"));

	auto result = query_con.Query("SELECT count(*), count(*) FILTER (WHERE id = 10000) FROM position_rollback");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(10001)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(0)}));
}

TEST_CASE("Abandoned pending query must release the active query", "[api]") {
	// A pending query created but never executed must not leak the active-query state (executor, plan,
	// autocommit transaction). We observe the autocommit transaction it opens, which is created and
	// released together with the active query: abandoning the pending must release it immediately, not
	// defer it to the next query or context teardown.
	DuckDB db;
	Connection con(db);

	REQUIRE(!con.context->transaction.HasActiveTransaction());

	SECTION("Abandon via Close()") {
		auto pending_query = con.PendingQuery("SELECT 42");
		REQUIRE(!pending_query->HasError());
		REQUIRE(con.context->transaction.HasActiveTransaction());

		pending_query->Close();
		REQUIRE(!con.context->transaction.HasActiveTransaction());
	}
	SECTION("Abandon an ATTACH via Close()") {
		auto pending_query = con.PendingQuery("ATTACH ':memory:' AS abandoned_db");
		REQUIRE(!pending_query->HasError());
		REQUIRE(con.context->transaction.HasActiveTransaction());

		pending_query->Close();
		REQUIRE(!con.context->transaction.HasActiveTransaction());
	}
	SECTION("Abandon a prepared pending query") {
		auto prepared = con.Prepare("SELECT 42");
		REQUIRE(!prepared->HasError());
		auto pending_query = prepared->PendingQuery();
		REQUIRE(!pending_query->HasError());
		REQUIRE(con.context->transaction.HasActiveTransaction());

		pending_query->Close();
		REQUIRE(!con.context->transaction.HasActiveTransaction());
	}
	// the connection must remain usable after abandoning pending queries
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Abandoned streaming result must release the active query", "[api]") {
	// A streaming result keeps the active-query state alive to feed the stream; it is normally
	// released when the stream is fully consumed. A stream abandoned before being drained must still
	// release that state, not leak it until the next query or context teardown.
	DuckDB db;
	Connection con(db);

	REQUIRE(!con.context->transaction.HasActiveTransaction());

	SECTION("Abandon via Close() before consuming") {
		auto result = con.SendQuery("SELECT * FROM range(10000)");
		REQUIRE(!result->HasError());
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		// the stream is in flight: the active query is still open
		REQUIRE(con.context->transaction.HasActiveTransaction());

		result->Cast<StreamQueryResult>().Close();
		REQUIRE(!con.context->transaction.HasActiveTransaction());
	}
	SECTION("Abandon via Close() after a partial fetch") {
		auto result = con.SendQuery("SELECT * FROM range(10000)");
		REQUIRE(!result->HasError());
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		auto chunk = result->Fetch(); // consume one chunk; the stream is not drained
		REQUIRE(chunk);
		REQUIRE(con.context->transaction.HasActiveTransaction());

		result->Cast<StreamQueryResult>().Close();
		REQUIRE(!con.context->transaction.HasActiveTransaction());
	}
	// the connection must remain usable after abandoning streaming results
	auto check = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(check, 0, {42}));
}

TEST_CASE("PROBE cancel a streaming producer parked on a full buffer", "[api][.]") {
	// Force the producer to park on a full buffer (result >> streaming_buffer_size), abandon the
	// stream mid-flight, then run another query so InitialCleanup -> CleanupInternal -> CancelTasks
	// runs against the parked producer. If CancelTasks cannot reap a parked result-collector task,
	// this hangs (busy-spins in `while (executor_tasks > 0) WorkOnTasks()`).
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='16KB'"));

	SECTION("abandon by dropping, then run another query") {
		auto result = con.SendQuery("SELECT * FROM range(10000000)");
		REQUIRE(!result->HasError());
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		auto chunk = result->Fetch(); // ensure the pipeline is actually streaming and re-parks
		REQUIRE(chunk);
		result.reset(); // abandon while the producer is parked on the full buffer

		auto check = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(check, 0, {42}));
	}
	SECTION("abandon via Close(), then run another query") {
		auto result = con.SendQuery("SELECT * FROM range(10000000)");
		REQUIRE(!result->HasError());
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		auto chunk = result->Fetch();
		REQUIRE(chunk);
		result->Cast<StreamQueryResult>().Close();

		auto check = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(check, 0, {42}));
	}
}

TEST_CASE("Interrupt is observed by PendingQueryResult::ExecuteTask", "[api]") {
	DuckDB db;
	Connection con(db);

	// Single thread + tiny streaming buffer make the parked-collector RESULT_READY state reachable fast.
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='16KB'"));

	auto pending = con.PendingQuery("SELECT * FROM range(10000000)", true);
	REQUIRE(!pending->HasError());

	PendingExecutionResult state = PendingExecutionResult::RESULT_NOT_READY;
	for (idx_t i = 0; i < 1000000; i++) {
		state = pending->ExecuteTask();
		if (state == PendingExecutionResult::RESULT_READY || state == PendingExecutionResult::EXECUTION_ERROR) {
			break;
		}
	}
	REQUIRE(state == PendingExecutionResult::RESULT_READY);

	con.Interrupt();

	// Without the fix the parked collector keeps reporting RESULT_READY and the interrupt is never seen.
	bool saw_error = false;
	for (idx_t j = 0; j < 1000; j++) {
		if (pending->ExecuteTask() == PendingExecutionResult::EXECUTION_ERROR) {
			saw_error = true;
			break;
		}
	}
	REQUIRE(saw_error);
}

TEST_CASE("Stream results from materialized CTE exchanges", "[api]") {
	DuckDB db;
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers AS SELECT i FROM range(1000000) t(i)"));

	SECTION("Direct batch-indexed exchange") {
		auto pending = con.PendingQuery("WITH c AS MATERIALIZED (SELECT i FROM integers) SELECT i FROM c", true);
		REQUIRE(!pending->HasError());

		auto result = pending->Execute();
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		idx_t count = 0;
		while (auto chunk = result->Fetch()) {
			REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == NumericCast<int64_t>(count));
			count += chunk->size();
		}
		REQUIRE(!result->HasError());
		REQUIRE(count == 1000000);
	}
	SECTION("Buffered unordered exchange") {
		REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
		auto pending = con.PendingQuery("WITH c AS MATERIALIZED (SELECT i FROM integers) SELECT i FROM c", true);
		REQUIRE(!pending->HasError());

		auto result = pending->Execute();
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		idx_t count = 0;
		while (auto chunk = result->Fetch()) {
			count += chunk->size();
		}
		REQUIRE(count == 1000000);
	}
	SECTION("Buffered batch-indexed exchange") {
		auto pending = con.PendingQuery("WITH c AS MATERIALIZED ("
		                                "SELECT i FROM integers WHERE i < 500000 "
		                                "UNION ALL "
		                                "SELECT i FROM integers WHERE i >= 500000) "
		                                "SELECT i FROM c",
		                                true);
		REQUIRE(!pending->HasError());

		auto result = pending->Execute();
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		idx_t count = 0;
		while (auto chunk = result->Fetch()) {
			REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == NumericCast<int64_t>(count));
			count += chunk->size();
		}
		REQUIRE(!result->HasError());
		REQUIRE(count == 1000000);
	}
	SECTION("Abandon buffered batch-indexed exchange") {
		auto result = con.SendQuery("WITH c AS MATERIALIZED ("
		                            "SELECT i FROM integers WHERE i < 500000 "
		                            "UNION ALL "
		                            "SELECT i FROM integers WHERE i >= 500000) "
		                            "SELECT i FROM c");
		REQUIRE(!result->HasError());
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		REQUIRE(result->Fetch());

		result->Cast<StreamQueryResult>().Close();
		auto check = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(check, 0, {42}));
	}
	SECTION("Ordered sink pipeline sequencing") {
		auto pending = con.PendingQuery("WITH c1 AS MATERIALIZED (SELECT i FROM range(10000) t(i)), "
		                                "c2 AS MATERIALIZED (SELECT i FROM c1), "
		                                "c3 AS MATERIALIZED (SELECT i + 10000 AS i FROM c1) "
		                                "SELECT i FROM c2 UNION ALL SELECT i FROM c3",
		                                true);
		REQUIRE(!pending->HasError());

		auto result = pending->Execute();
		REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
		idx_t count = 0;
		while (auto chunk = result->Fetch()) {
			REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == NumericCast<int64_t>(count));
			count += chunk->size();
		}
		REQUIRE(count == 20000);
	}
}

static void parallel_pending_query(Connection *conn, bool *correct, size_t threadnr) {
	correct[threadnr] = true;
	for (size_t i = 0; i < 100; i++) {
		// run pending query and then execute it
		auto executor = conn->PendingQuery("SELECT * FROM integers ORDER BY i");
		try {
			// this will randomly throw an exception if another thread calls pending query first
			auto result = executor->Execute();
			if (!CHECK_COLUMN(result, 0, {1, 2, 3, Value()})) {
				correct[threadnr] = false;
			}
		} catch (...) {
			continue;
		}
	}
}

TEST_CASE("Test parallel usage of pending query API", "[api][.]") {
	auto db = make_uniq<DuckDB>(nullptr);
	auto conn = make_uniq<Connection>(*db);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	bool correct[20];
	std::thread threads[20];
	for (size_t i = 0; i < 20; i++) {
		threads[i] = std::thread(parallel_pending_query, conn.get(), correct, i);
	}
	for (size_t i = 0; i < 20; i++) {
		threads[i].join();
		REQUIRE(correct[i]);
	}
}

TEST_CASE("Test Pending Query Prepared Statements API", "[api][.]") {
	DuckDB db;
	Connection con(db);

	SECTION("Standard prepared") {
		auto prepare = con.Prepare("SELECT SUM(i) FROM range(1000000) tbl(i) WHERE i>=$1");
		REQUIRE(!prepare->HasError());

		auto pending_query = prepare->PendingQuery(0);
		REQUIRE(!pending_query->HasError());

		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());

		// we can use the prepared query again, however
		pending_query = prepare->PendingQuery(500000);
		REQUIRE(!pending_query->HasError());

		result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(374999750000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());
	}
	SECTION("Error during prepare") {
		auto prepare = con.Prepare("SELECT SUM(i+X) FROM range(1000000) tbl(i) WHERE i>=$1");
		REQUIRE(prepare->HasError());

		REQUIRE_FAIL(prepare->PendingQuery(0));
	}
	SECTION("Error during execution") {
		duckdb::vector<Value> parameters;
		auto prepared = con.Prepare("SELECT concat(SUM(i)::varchar, CASE WHEN SUM(i) IS NULL THEN 0 ELSE 'hello' "
		                            "END)::INT FROM range(1000000) tbl(i) WHERE i>$1");
		// this succeeds initially
		parameters = {Value::INTEGER(0)};
		auto pending_query = prepared->PendingQuery(parameters, true);
		REQUIRE(!pending_query->HasError());
		// still succeeds...
		auto result = pending_query->Execute();
		REQUIRE(result->HasError());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));

		// if we change the parameter this works
		parameters = {Value::INTEGER(2000000)};
		pending_query = prepared->PendingQuery(parameters, true);

		result = pending_query->Execute();
		REQUIRE(!result->HasError());
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(0)}));
	}
	SECTION("Multiple prepared statements") {
		auto prepare1 = con.Prepare("SELECT SUM(i) FROM range(1000000) tbl(i) WHERE i>=$1");
		auto prepare2 = con.Prepare("SELECT SUM(i) FROM range(1000000) tbl(i) WHERE i<=$1");
		REQUIRE(!prepare1->HasError());
		REQUIRE(!prepare2->HasError());

		// we can execute from both prepared statements individually
		auto pending_query = prepare1->PendingQuery(500000);
		REQUIRE(!pending_query->HasError());

		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(374999750000)}));

		pending_query = prepare2->PendingQuery(500000);
		REQUIRE(!pending_query->HasError());

		result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(125000250000)}));

		// we can overwrite pending queries all day long
		for (idx_t i = 0; i < 10; i++) {
			pending_query = prepare1->PendingQuery(500000);
			pending_query = prepare2->PendingQuery(500000);
		}

		result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(125000250000)}));

		// however, we can't mix and match...
		pending_query = prepare1->PendingQuery(500000);
		auto pending_query2 = prepare2->PendingQuery(500000);

		// this result is no longer open
		REQUIRE_THROWS(pending_query->Execute());
	}
}
