#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
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

} // namespace

TEST_CASE("Automatic checkpoint with an index type that is not loaded", "[storage][wal]") {
	auto config = GetTestConfig();
	config->options.checkpoint_wal_size = idx_t(-1);
	config->options.checkpoint_on_shutdown = false;

	auto database_path = TestCreatePath("checkpoint_unknown_index_type");
	auto wal_path = database_path + ".wal";
	LocalFileSystem fs;
	DeleteDatabase(database_path);

	{
		DuckDB db(database_path, config.get());
		RegisterTestIndexType(db);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE ext(k INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO ext SELECT i FROM range(100) r(i)"));
		REQUIRE_NO_FAIL(con.Query(string("CREATE INDEX ext_idx ON ext USING ") + TEST_INDEX_TYPE + " (k)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(id INTEGER, k VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX t_idx ON t(k)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		// Only in the WAL, so that the replay buffers these rows in the unbound index of t.
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i, 'k' || i FROM range(500) r(i)"));
	}

	{
		// The index type of ext is unknown, and its index has no buffered replays.
		DuckDB db(database_path, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET wal_autocheckpoint='1KB'"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE other AS SELECT * FROM range(20000) r(x)"));
		REQUIRE((!fs.FileExists(wal_path) || fs.GetFileSize(*fs.OpenFile(wal_path, FileFlags::FILE_FLAGS_READ)) == 0));
	}

	{
		DuckDB db(database_path, config.get());
		RegisterTestIndexType(db);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET index_scan_percentage=1"));
		REQUIRE_NO_FAIL(con.Query("SET index_scan_max_count=999999999"));

		auto result = con.Query("SELECT count(*) FROM t WHERE k='k7'");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM t"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM ext"));
	}

	DeleteDatabase(database_path);
}
