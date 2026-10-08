#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/main/config.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

constexpr const char *TEST_INDEX_TYPE = "TEST_EXTENSION_INDEX";

//! Models an index type provided by an extension, by registering ART under another name.
void RegisterTestIndexType(DuckDB &db) {
	auto index_type = ART::GetARTIndexType();
	index_type.name = TEST_INDEX_TYPE;
	DBConfig::GetConfig(*db.instance).GetIndexTypes().RegisterIndexType(index_type);
}

//! Reading the WAL file while the database is open is not portable, so we go through the database size.
void RequireWALEmpty(Connection &con, const bool empty) {
	auto result = con.Query("SELECT wal_size = '0 bytes' FROM pragma_database_size()");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BOOLEAN(empty)}));
}

} // namespace

TEST_CASE("Checkpoint an index type that is not loaded", "[storage][wal]") {
	auto config = GetTestConfig();
	config->options.checkpoint_wal_size = idx_t(-1);
	config->options.checkpoint_on_shutdown = false;

	auto database_path = TestCreatePath("checkpoint_unknown_index_type");
	DeleteDatabase(database_path);

	{
		DuckDB db(database_path, config.get());
		RegisterTestIndexType(db);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE ext(k INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO ext SELECT i FROM range(100) r(i)"));
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE INDEX ext_idx ON ext USING %s (k)", TEST_INDEX_TYPE)));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(id INTEGER, k VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX t_idx ON t(k)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		// Only in the WAL, so that the replay buffers these rows in the unbound index of t.
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i, 'k' || i FROM range(500) r(i)"));
	}

	{
		// The index type of ext is unknown, but its index has no buffered replays: it is written as-is.
		DuckDB db(database_path, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET wal_autocheckpoint='1KB'"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE other AS SELECT * FROM range(20000) r(x)"));
		RequireWALEmpty(con, true);
	}

	{
		DuckDB db(database_path, config.get());
		RegisterTestIndexType(db);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET index_scan_percentage=1"));
		REQUIRE_NO_FAIL(con.Query("SET index_scan_max_count=999999999"));

		// The automatic checkpoint persisted the operations buffered in the index of t.
		auto result = con.Query("SELECT count(*) FROM t WHERE k='k7'");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM t"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM ext"));
	}

	DeleteDatabase(database_path);
}

TEST_CASE("Checkpoint buffered replays of an index type that is not loaded", "[storage][wal]") {
	auto config = GetTestConfig();
	config->options.checkpoint_wal_size = idx_t(-1);
	config->options.checkpoint_on_shutdown = false;

	auto database_path = TestCreatePath("checkpoint_unknown_index_type_buffered");
	DeleteDatabase(database_path);

	{
		DuckDB db(database_path, config.get());
		RegisterTestIndexType(db);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE ext(k INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO ext SELECT i FROM range(100) r(i)"));
		REQUIRE_NO_FAIL(
		    con.Query(StringUtil::Format("CREATE UNIQUE INDEX ext_idx ON ext USING %s (k)", TEST_INDEX_TYPE)));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE big AS SELECT i AS a, i AS b FROM range(100000) r(i)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		// Only in the WAL, so that the replay buffers these rows in the unbound index of ext.
		REQUIRE_NO_FAIL(con.Query("INSERT INTO ext SELECT i FROM range(100, 200) r(i)"));
	}

	{
		// The index type of ext is unknown, and its index has buffered replays that cannot be applied.
		DuckDB db(database_path, config.get());
		Connection con(db);

		// An explicit CHECKPOINT reports that it cannot persist the buffered operations.
		auto result = con.Query("CHECKPOINT");
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "Cannot CHECKPOINT"));
		REQUIRE(StringUtil::Contains(result->GetError(), TEST_INDEX_TYPE));
		RequireWALEmpty(con, false);

		// An automatic checkpoint keeps the WAL instead, and neither fails nor invalidates the database.
		REQUIRE_NO_FAIL(con.Query("SET wal_autocheckpoint='1KB'"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE other AS SELECT * FROM range(20000) r(x)"));
		RequireWALEmpty(con, false);

		// A commit that is large enough to skip its WAL write in favor of the checkpoint must still be durable,
		// because the checkpoint it relies on is cancelled.
		REQUIRE_NO_FAIL(con.Query("UPDATE big SET b = b + 1"));

		// A failed bind leaves the index unbound, so binding it again fails the same way instead of hanging.
		for (idx_t i = 0; i < 2; i++) {
			result = con.Query("INSERT INTO ext VALUES (1000)");
			REQUIRE(result->HasError());
			REQUIRE(StringUtil::Contains(result->GetError(), TEST_INDEX_TYPE));
		}

		result = con.Query("SELECT count(*) FROM ext");
		REQUIRE(CHECK_COLUMN(result, 0, {200}));
	}

	{
		DuckDB db(database_path, config.get());
		Connection con(db);

		auto result = con.Query("SELECT count(*), sum(b) FROM big");
		REQUIRE(CHECK_COLUMN(result, 0, {100000}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(5000050000)}));
		result = con.Query("SELECT count(*) FROM other");
		REQUIRE(CHECK_COLUMN(result, 0, {20000}));

		// Loading the index type after a failed bind allows binding the index in the same database instance.
		REQUIRE(con.Query("CHECKPOINT")->HasError());
		RegisterTestIndexType(db);

		// The buffered index operations are still there: the unique index sees the rows of the WAL.
		result = con.Query("INSERT INTO ext VALUES (150)");
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "Constraint Error"));

		// The index type is loaded, so the checkpoint persists the buffered operations.
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		RequireWALEmpty(con, true);
	}

	{
		DuckDB db(database_path, config.get());
		RegisterTestIndexType(db);
		Connection con(db);
		auto result = con.Query("SELECT count(*) FROM ext");
		REQUIRE(CHECK_COLUMN(result, 0, {200}));
		result = con.Query("INSERT INTO ext VALUES (150)");
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "Constraint Error"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM ext"));
	}

	DeleteDatabase(database_path);
}
