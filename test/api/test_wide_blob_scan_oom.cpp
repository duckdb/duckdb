#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_size.hpp"

using namespace duckdb;

// A staged-upsert flush: an Appender writes several versions of each key into a staging table, then two
// statements apply them to the target — a DELETE and an INSERT that dedup by key, keeping the version with
// the highest sequence number.
//
// The INSERT projects every column (including a wide BLOB) through STRUCT_PACK, so the staging-table scan
// materializes full 2048-row vectors of BLOBs and OOMs under a tight memory limit. The dedup aggregate
// (LAST(x ORDER BY y) -> arg_max_null in a hash group by) only retains one struct per key, so the scan
// is the sole memory bottleneck — exactly what scan_target_size_bytes byte-budgets away.

namespace {

constexpr idx_t BATCH_ROWS = 4096;  // staged row versions written before the flush
constexpr idx_t BLOB_SIZE = 50000;  // ~50KB per row -> one 2048-row vector is ~100MB
constexpr idx_t DISTINCT_PKS = 128; // many row versions per key -> tiny aggregate output

// is_delete == 0 means the latest version of a key is a live row (kept by the INSERT)
const char *STAGING_DDL = "CREATE TABLE staging(id INTEGER, payload BLOB, seq BIGINT, is_delete INTEGER)";

// dedup keys, then delete the matching target rows
const char *DELETE_SQL = "DELETE FROM t WHERE (id) IN ("
                         "SELECT UNNEST(r) FROM ("
                         "SELECT LAST(STRUCT_PACK(id) ORDER BY seq) AS r, "
                         "LAST(is_delete ORDER BY seq) AS is_delete "
                         "FROM staging GROUP BY id))";

// dedup keys, then insert the latest live version of each
const char *INSERT_SQL = "INSERT INTO t "
                         "SELECT UNNEST(r) FROM ("
                         "SELECT LAST(STRUCT_PACK(id, payload) ORDER BY seq) AS r, "
                         "LAST(is_delete ORDER BY seq) AS is_delete "
                         "FROM staging GROUP BY id) "
                         "WHERE is_delete = 0";

void AppendStagingBatch(Connection &con) {
	string payload(BLOB_SIZE, 'A');
	Appender appender(con, "staging");
	for (idx_t i = 0; i < BATCH_ROWS; i++) {
		appender.BeginRow();
		appender.Append<int32_t>(int32_t(i % DISTINCT_PKS));
		appender.Append<Value>(Value::BLOB_RAW(payload));
		appender.Append<int64_t>(int64_t(i));
		appender.Append<int32_t>(0);
		appender.EndRow();
	}
	appender.Flush();
}

} // namespace

// A full 2048-row vector of 50KB BLOBs (~100MB) is what OOMs under the 60MB limit; with a tiny
// STANDARD_VECTOR_SIZE (e.g. CI's vector_size=2) the vector is ~100KB and never OOMs, so the test
// premise does not hold. Skip at compile time, mirroring `require vector_size 2048` in .test files.
#if STANDARD_VECTOR_SIZE >= 2048
TEST_CASE("Test wide-BLOB scan OOM in a staged upsert and scan_target_size_bytes mitigation",
          "[api][scan_target_size_bytes]") {
	auto db_path = TestCreatePath("wide_blob_scan_oom.db");
	DeleteDatabase(db_path);

	DuckDB db(db_path);
	Connection con(db);

	// ---- setup: stage the batch and seed the target (default memory limit) ----
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(id INTEGER, payload BLOB)"));
	REQUIRE_NO_FAIL(con.Query(STAGING_DDL));
	AppendStagingBatch(con);
	// pre-existing target rows the DELETE must remove (small payloads)
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i, repeat('B', 8)::BLOB FROM range(128) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));

	// ---- constrained phase: one 2048-row BLOB vector (~100MB) cannot fit ----
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='60MB'"));

	SECTION("feature OFF: the INSERT OOMs") {
		REQUIRE_NO_FAIL(con.Query("SET scan_target_size_bytes=0"));

		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// DELETE only projects the key column, so it stays cheap and succeeds
		REQUIRE_NO_FAIL(con.Query(DELETE_SQL));
		// INSERT materializes the wide BLOB vectors -> out of memory
		auto insert_result = con.Query(INSERT_SQL);
		REQUIRE(insert_result->HasError());
		REQUIRE(StringUtil::Contains(insert_result->GetError(), "Out of Memory"));
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		// rollback restored the seed rows untouched
		auto check = con.Query("SELECT COUNT(*) FROM t");
		REQUIRE(CHECK_COLUMN(check, 0, {128}));
	}

	SECTION("feature ON: the same two statements commit within the same memory limit") {
		REQUIRE_NO_FAIL(con.Query("SET scan_target_size_bytes=1048576"));

		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query(DELETE_SQL));
		REQUIRE_NO_FAIL(con.Query(INSERT_SQL));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		// dedup produced exactly one latest 50KB version per key
		auto check = con.Query("SELECT COUNT(*), COUNT(DISTINCT id), SUM(OCTET_LENGTH(payload)) FROM t");
		REQUIRE(CHECK_COLUMN(check, 0, {int64_t(DISTINCT_PKS)}));
		REQUIRE(CHECK_COLUMN(check, 1, {int64_t(DISTINCT_PKS)}));
		REQUIRE(CHECK_COLUMN(check, 2, {Value::BIGINT(int64_t(DISTINCT_PKS * BLOB_SIZE))}));
	}

	con.Query("DROP TABLE t");
	DeleteDatabase(db_path);
}
#endif
