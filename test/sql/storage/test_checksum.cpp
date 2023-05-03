#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test functioning of checksum", "[storage]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	duckdb::unique_ptr<DuckDB> database;
	auto storage_database = TestCreatePath("checksum_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3);"));
	}
	// we can open the database file now
	REQUIRE_NOTHROW(database = make_uniq<DuckDB>(storage_database, config.get()));
	database.reset();

	// now write random values into the file
	auto handle = fs->OpenFile(storage_database, FileFlags::FILE_FLAGS_WRITE);
	int8_t value = 0x22;
	fs->Write(*handle, &value, sizeof(int8_t), 100);
	handle->Sync();
	handle.reset();
	// reloading the database no longer works
	REQUIRE_THROWS(database = make_uniq<DuckDB>(storage_database, config.get()));

	DeleteDatabase(storage_database);
}
