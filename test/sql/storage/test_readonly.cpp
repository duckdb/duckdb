#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace std;

namespace duckdb {

class ReadOnlyFileSystem : public FileSystem {
	unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags, FileLockType lock_type) override {
		if (flags & FileFlags::WRITE) {
			throw Exception("RO file system");
		}
		return FileSystem::OpenFile(path, flags, lock_type);
	}

	void CreateDirectory(const string &directory) override {
		throw Exception("RO file system");
	}
	void RemoveDirectory(const string &directory) override {
		throw Exception("RO file system");
	}
	void MoveFile(const string &source, const string &target) override {
		throw Exception("RO file system");
	}
	void RemoveFile(const string &filename) override {
		throw Exception("RO file system");
	}
};

TEST_CASE("Test read only storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	DeleteDatabase(storage_database);

	{
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (42)"));
	}
	{
		DBConfig config;
		config.file_system = make_unique_base<FileSystem, ReadOnlyFileSystem>();
		config.access_mode = AccessMode::READ_ONLY;
		config.use_temporary_directory = false;
		DuckDB db(storage_database, &config);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));

		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (43)"));
		REQUIRE_FAIL(con.Query("UPDATE test SET a = 43"));
		REQUIRE_FAIL(con.Query("DROP TABLE test"));
		// temporary tables
		REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE test2(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (22), (23)"));
		REQUIRE_NO_FAIL(con.Query("UPDATE test2 SET i=i+1"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test2 WHERE i=23"));

		result = con.Query("SELECT * FROM test2");
		REQUIRE(CHECK_COLUMN(result, 0, {24}));
	}
	DeleteDatabase(storage_database);
}

} // namespace duckdb
