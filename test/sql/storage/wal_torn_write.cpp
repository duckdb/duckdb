#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/local_file_system.hpp"

using namespace duckdb;
using namespace std;

static idx_t GetWALFileSize(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	return fs.GetFileSize(*handle);
}

static void TruncateWAL(FileSystem &fs, const string &path, idx_t new_size) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE);
	fs.Truncate(*handle, new_size);
}

TEST_CASE("Test torn WAL writes", "[storage][.]") {
	auto config = GetTestConfig();
	duckdb::unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto storage_wal = storage_database + ".wal";

	LocalFileSystem lfs;
	config->options.checkpoint_wal_size = idx_t(-1);
	config->options.checkpoint_on_shutdown = false;
	config->options.abort_on_wal_failure = false;
	idx_t wal_size_one_table;
	idx_t wal_size_two_table;
	// obtain the size of the WAL when writing one table, and then when writing two tables
	DeleteDatabase(storage_database);
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE A (a INTEGER);"));
		wal_size_one_table = GetWALFileSize(lfs, storage_wal);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE B (a INTEGER);"));
		wal_size_two_table = GetWALFileSize(lfs, storage_wal);
	}
	DeleteDatabase(storage_database);

	// now for all sizes in between these two sizes we have a torn write
	// try all of the possible sizes and truncate the WAL
	for (idx_t i = wal_size_one_table + 1; i < wal_size_two_table; i++) {
		DeleteDatabase(storage_database);
		{
			DuckDB db(storage_database, config.get());
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("CREATE TABLE A (a INTEGER);"));
			REQUIRE_NO_FAIL(con.Query("CREATE TABLE B (a INTEGER);"));
		}
		TruncateWAL(lfs, storage_wal, i);
		{
			// reload and make sure table A is there, and table B is not there
			DuckDB db(storage_database, config.get());
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("FROM A"));
			REQUIRE_FAIL(con.Query("FROM B"));
		}
	}
	DeleteDatabase(storage_database);
}

static void FlipWALByte(FileSystem &fs, const string &path, idx_t byte_pos) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	idx_t wal_size = handle->GetFileSize();
	auto wal_contents = duckdb::unique_ptr<data_t[]>(new data_t[wal_size]);
	handle->Read(wal_contents.get(), wal_size, 0);
	wal_contents[byte_pos]++;
	handle->Write(wal_contents.get(), wal_size, 0);
}

TEST_CASE("Test WAL checksums", "[storage][.]") {
	auto config = GetTestConfig();
	duckdb::unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("wal_checksum");
	auto storage_wal = storage_database + ".wal";

	LocalFileSystem lfs;
	config->options.checkpoint_wal_size = idx_t(-1);
	config->options.checkpoint_on_shutdown = false;
	config->options.abort_on_wal_failure = false;
	idx_t wal_size_one_table;
	idx_t wal_size_two_table;
	// obtain the size of the WAL when writing one table, and then when writing two tables
	DeleteDatabase(storage_database);
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE A (a INTEGER);"));
		wal_size_one_table = GetWALFileSize(lfs, storage_wal);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE B (a INTEGER);"));
		wal_size_two_table = GetWALFileSize(lfs, storage_wal);
	}
	DeleteDatabase(storage_database);

	// now for all sizes in between these two sizes we have a torn write
	// try all of the possible sizes and truncate the WAL
	for (idx_t i = wal_size_one_table + 1; i < wal_size_two_table; i++) {
		DeleteDatabase(storage_database);
		{
			DuckDB db(storage_database, config.get());
			Connection con(db);
			REQUIRE_NO_FAIL(con.Query("CREATE TABLE A (a INTEGER);"));
			REQUIRE_NO_FAIL(con.Query("CREATE TABLE B (a INTEGER);"));
		}
		FlipWALByte(lfs, storage_wal, i);
		{
			// flipping a byte in the checksum leads to an IOException
			// flipping a byte in the size of a WAL entry leads to a torn write
			// we succeed on either of these cases here
			try {
				DuckDB db(storage_database, config.get());
				Connection con(db);
				REQUIRE_NO_FAIL(con.Query("FROM A"));
				REQUIRE_FAIL(con.Query("FROM B"));
			} catch (std::exception &ex) {
				ErrorData error(ex);
				if (error.Type() == ExceptionType::IO) {
					REQUIRE(1 == 1);
				} else {
					throw;
				}
			}
		}
	}
	DeleteDatabase(storage_database);
}
