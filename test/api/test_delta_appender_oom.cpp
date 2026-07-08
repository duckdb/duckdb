#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;

// Reproduces the RDS DeltaAppender::flush() path: a batch of delta rows is appended to a temp table,
// then flushed by running the exact two SQLs generateQuery() builds — a DELETE and an INSERT that
// dedup by primary key keeping the latest #alibaba_rds_row_no version.
//
// The INSERT projects every column (including a wide BLOB) through STRUCT_PACK, so the base-table scan
// materializes full 2048-row vectors of BLOBs and OOMs under a tight memory limit. The dedup aggregate
// (LAST(x ORDER BY y) -> arg_max_null in a hash group by) only retains one struct per key, so the scan
// is the sole memory bottleneck — exactly what scan_target_size_bytes byte-budgets away.

namespace {

constexpr idx_t BATCH_ROWS = 4096;    // delta rows appended before flush
constexpr idx_t BLOB_SIZE = 50000;    // ~50KB per row -> one 2048-row vector is ~100MB
constexpr idx_t DISTINCT_PKS = 128;   // many row versions per key -> tiny aggregate output

// #alibaba_rds_delete_flag == 0 means the latest version is a live row (kept by the INSERT)
const char *DELTA_TMP_DDL = "CREATE TABLE delta_tmp("
                            "id INTEGER, payload BLOB, "
                            "\"#alibaba_rds_row_no\" BIGINT, "
                            "\"#alibaba_rds_delete_flag\" INTEGER)";

// mirrors DeltaAppender::generateQuery(delete_flag=true): dedup keys, then delete matching rows
const char *DELETE_SQL = "DELETE FROM t WHERE (id) IN ("
                         "SELECT UNNEST(r) FROM ("
                         "SELECT LAST(STRUCT_PACK(id) ORDER BY \"#alibaba_rds_row_no\") AS r, "
                         "LAST(\"#alibaba_rds_delete_flag\" ORDER BY \"#alibaba_rds_row_no\") AS \"#alibaba_rds_delete_flag\" "
                         "FROM delta_tmp GROUP BY id))";

// mirrors DeltaAppender::generateQuery(delete_flag=false): dedup keys, insert the latest live version
const char *INSERT_SQL = "INSERT INTO t "
                         "SELECT UNNEST(r) FROM ("
                         "SELECT LAST(STRUCT_PACK(id, payload) ORDER BY \"#alibaba_rds_row_no\") AS r, "
                         "LAST(\"#alibaba_rds_delete_flag\" ORDER BY \"#alibaba_rds_row_no\") AS \"#alibaba_rds_delete_flag\" "
                         "FROM delta_tmp GROUP BY id) "
                         "WHERE \"#alibaba_rds_delete_flag\" = 0";

// Append the delta batch through the real Appender and Flush() it, mirroring m_appender->Flush().
void AppendDeltaBatch(Connection &con) {
	string payload(BLOB_SIZE, 'A');
	Appender appender(con, "delta_tmp");
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

TEST_CASE("Test DeltaAppender flush OOM and scan_target_size_bytes mitigation", "[api][delta_appender]") {
	auto db_path = TestCreatePath("delta_appender_oom.db");
	DeleteDatabase(db_path);

	DuckDB db(db_path);
	Connection con(db);

	// ---- setup: append the delta batch and seed the target (default memory limit) ----
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(id INTEGER, payload BLOB)"));
	REQUIRE_NO_FAIL(con.Query(DELTA_TMP_DDL));
	AppendDeltaBatch(con);
	// pre-existing target rows the DELETE must remove (small payloads)
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i, repeat('B', 8)::BLOB FROM range(128) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));

	// ---- constrained phase: one 2048-row BLOB vector (~100MB) cannot fit ----
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='60MB'"));

	SECTION("feature OFF: flush OOMs on the INSERT") {
		REQUIRE_NO_FAIL(con.Query("SET scan_target_size_bytes=0"));

		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		// DELETE only projects the PK column, so it stays cheap and succeeds
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

	SECTION("feature ON: flush commits within the same memory limit") {
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
