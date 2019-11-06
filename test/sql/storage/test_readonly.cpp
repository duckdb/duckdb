#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
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
} // namespace duckdb

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
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM test ORDER BY a"));
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (43)"));
		REQUIRE_FAIL(con.Query("UPDATE test SET a = 43"));
		REQUIRE_FAIL(con.Query("DROP TABLE test"));
	}
	DeleteDatabase(storage_database);
}
