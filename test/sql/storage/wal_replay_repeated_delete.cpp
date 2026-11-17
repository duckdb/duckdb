#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/settings.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static idx_t GetWALSize(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	return fs.GetFileSize(*handle);
}

//! Append a copy of the bytes [start, end) of the WAL to its end
static void RepeatWALEntries(FileSystem &fs, const string &path, idx_t start, idx_t end) {
	auto size = end - start;
	auto entries = duckdb::unique_ptr<data_t[]>(new data_t[size]);
	{
		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		handle->Read(QueryContext(), entries.get(), size, start);
	}
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND);
	handle->Write(QueryContext(), entries.get(), size);
}

TEST_CASE("Test WAL that deletes the same rows in two transactions", "[storage][.]") {
	auto config = GetTestConfig();
	auto storage_database = TestCreatePath("wal_repeated_delete");
	auto storage_wal = storage_database + ".wal";

	LocalFileSystem lfs;
	config->options.checkpoint_wal_size = idx_t(-1);
	config->SetOption(CheckpointOnShutdownSetting::SettingIndex, Value::BOOLEAN(false));
	DeleteDatabase(storage_database);

	idx_t delete_start;
	idx_t delete_end;
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range AS i FROM range(10);"));
		delete_start = GetWALSize(lfs, storage_wal);
		REQUIRE_NO_FAIL(con.Query("DELETE FROM t WHERE i < 3;"));
		delete_end = GetWALSize(lfs, storage_wal);
	}
	REQUIRE(delete_end > delete_start);
	REQUIRE(GetWALSize(lfs, storage_wal) == delete_end);

	// the WAL replays as it was written
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		auto result = con.Query("SELECT count(*) FROM t;");
		REQUIRE(CHECK_COLUMN(result, 0, {7}));
	}
	REQUIRE(GetWALSize(lfs, storage_wal) == delete_end);

	// the committed delete once more, as a transaction of its own: its rows are deleted already
	RepeatWALEntries(lfs, storage_wal, delete_start, delete_end);
	REQUIRE_THROWS_WITH(DuckDB(storage_database, config.get()),
	                    Catch::Contains("Corrupt WAL: delete of a row that is already deleted"));
	DeleteDatabase(storage_database);
}
