#include "catch.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
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
	auto entries = make_unsafe_uniq_array_uninitialized<data_t>(size);
	{
		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		handle->Read(QueryContext(), entries.get(), size, start);
	}
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND);
	handle->Write(QueryContext(), entries.get(), size);
}

//! Replay a WAL in which the transaction that deletes the first delete_count of row_count rows is repeated
static void TestReplayRepeatedDelete(const string &name, idx_t row_count, idx_t delete_count) {
	auto config = GetTestConfig();
	auto storage_database = TestCreatePath(name);
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
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range AS i FROM range(" + to_string(row_count) + ");"));
		delete_start = GetWALSize(lfs, storage_wal);
		REQUIRE_NO_FAIL(con.Query("DELETE FROM t WHERE i < " + to_string(delete_count) + ";"));
		delete_end = GetWALSize(lfs, storage_wal);
	}
	REQUIRE(delete_end > delete_start);
	REQUIRE(GetWALSize(lfs, storage_wal) == delete_end);

	// the WAL replays as it was written
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		auto result = con.Query("SELECT count(*) FROM t;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(NumericCast<int64_t>(row_count - delete_count))}));
	}
	REQUIRE(GetWALSize(lfs, storage_wal) == delete_end);

	// the committed delete once more, as a transaction of its own: its rows are deleted already
	RepeatWALEntries(lfs, storage_wal, delete_start, delete_end);
	// the WAL is corrupt, not torn: the error does not depend on debug_abort_on_wal_failure
	for (auto abort_on_wal_failure : {true, false}) {
		config->SetOption(DebugAbortOnWalFailureSetting::SettingIndex, Value::BOOLEAN(abort_on_wal_failure));
		try {
			DuckDB db(storage_database, config.get());
			FAIL("Expected the WAL replay to fail");
		} catch (std::exception &ex) {
			ErrorData error(ex);
			REQUIRE(error.Type() == ExceptionType::DATA_CORRUPTION);
			REQUIRE(StringUtil::Contains(error.Message(), "Corrupt WAL: delete of a row that is already deleted"));
		}
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test WAL that deletes the same rows in two transactions", "[storage][.]") {
	TestReplayRepeatedDelete("wal_repeated_delete", 10, 3);
}

TEST_CASE("Test WAL that deletes the same vector of rows in two transactions", "[storage][.]") {
	TestReplayRepeatedDelete("wal_repeated_vector_delete", 2 * STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE);
}
