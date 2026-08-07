#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Orphaned temp files from a crashed process are cleaned up on startup",
          "[temp_directory]") {
	auto temp_dir = TestCreatePath("temp_dir_crash_recovery");

	auto fs = FileSystem::CreateLocal();
	if (fs->DirectoryExists(temp_dir)) {
		fs->RemoveDirectory(temp_dir);
	}
	fs->CreateDirectory(temp_dir);

	// simulate files left behind by a process that was killed (kill -9 / OOM / power loss)
	auto orphan1 = fs->JoinPath(temp_dir, "duckdb_temp_storage_DEFAULT-0.tmp");
	auto orphan2 = fs->JoinPath(temp_dir, "duckdb_temp_storage_S96K-0.tmp");
	auto unrelated = fs->JoinPath(temp_dir, "not_a_temp_file.txt");

	auto WriteDummyFile = [&](const string &path) {
		auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		string data = "dummy";
		fs->Write(*handle, (void *)data.c_str(), data.size(), 0);
	};
	WriteDummyFile(orphan1);
	WriteDummyFile(orphan2);
	WriteDummyFile(unrelated);

	REQUIRE(fs->FileExists(orphan1));
	REQUIRE(fs->FileExists(orphan2));
	REQUIRE(fs->FileExists(unrelated));

	// open a new DB pointed at the same temp_directory and force a spill,
	// which lazily constructs TemporaryDirectoryHandle
	auto db = make_uniq<DuckDB>(nullptr);
	auto con = make_uniq<Connection>(*db);
	REQUIRE_NO_FAIL(con->Query("SET temp_directory='" + temp_dir + "'"));
	REQUIRE_NO_FAIL(con->Query("SET memory_limit='4MB'"));
	REQUIRE_NO_FAIL(con->Query("CREATE OR REPLACE TABLE t2 AS SELECT random() FROM range(200000)"));

	// orphaned duckdb_temp_* files should have been swept away on startup
	REQUIRE_FALSE(fs->FileExists(orphan1));
	REQUIRE_FALSE(fs->FileExists(orphan2));
	// but anything not matching the prefix must be left alone
	REQUIRE(fs->FileExists(unrelated));

	con.reset();
	db.reset();
	fs->RemoveDirectory(temp_dir);
}
