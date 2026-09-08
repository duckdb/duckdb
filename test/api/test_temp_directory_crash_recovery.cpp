#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Orphaned temp files from a crashed process are cleaned up on startup", "[temp_directory]") {
	auto temp_dir = TestCreatePath("temp_dir_crash_recovery");

	auto fs = FileSystem::CreateLocal();
	if (fs->DirectoryExists(temp_dir)) {
		fs->RemoveDirectory(temp_dir);
	}
	fs->CreateDirectory(temp_dir);

	// simulate files left behind by a process that was killed (kill -9 / OOM / power loss)
	const string orphan_identifier = "dead_instance";
	auto orphan_lock = fs->JoinPath(temp_dir, "duckdb_temp_" + orphan_identifier + ".lock");
	auto orphan1 = fs->JoinPath(temp_dir, "duckdb_temp_storage_" + orphan_identifier + "_DEFAULT-0.tmp");
	auto orphan2 = fs->JoinPath(temp_dir, "duckdb_temp_block_" + orphan_identifier + "-0.block");
	auto unrelated = fs->JoinPath(temp_dir, "not_a_temp_file.txt");

	auto WriteDummyFile = [&](const string &path) {
		auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		string data = "dummy";
		fs->Write(*handle, (void *)data.c_str(), data.size(), 0);
	};
	WriteDummyFile(orphan_lock);
	WriteDummyFile(orphan1);
	WriteDummyFile(orphan2);
	WriteDummyFile(unrelated);

	REQUIRE(fs->FileExists(orphan_lock));
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

	// The unlocked ownership marker identifies this group as belonging to a dead instance.
	REQUIRE_FALSE(fs->FileExists(orphan_lock));
	REQUIRE_FALSE(fs->FileExists(orphan1));
	REQUIRE_FALSE(fs->FileExists(orphan2));
	// but anything not matching the prefix must be left alone
	REQUIRE(fs->FileExists(unrelated));

	con.reset();
	db.reset();
	fs->RemoveDirectory(temp_dir);
}

TEST_CASE("Temporary files owned by a live instance are not reclaimed", "[temp_directory]") {
	auto temp_dir = TestCreatePath("temp_dir_live_owner");
	auto fs = FileSystem::CreateLocal();
	if (fs->DirectoryExists(temp_dir)) {
		fs->RemoveDirectory(temp_dir);
	}

	auto db1 = make_uniq<DuckDB>(nullptr);
	auto con1 = make_uniq<Connection>(*db1);
	REQUIRE_NO_FAIL(con1->Query("SET temp_directory='" + temp_dir + "'"));
	REQUIRE_NO_FAIL(con1->Query("SET memory_limit='4MB'"));
	REQUIRE_NO_FAIL(con1->Query("CREATE TEMP TABLE t AS SELECT random() FROM range(200000)"));

	vector<string> live_files;
	fs->ListFiles(temp_dir, [&](const string &path, bool isdir) {
		if (!isdir && StringUtil::StartsWith(path, "duckdb_temp_")) {
			live_files.push_back(fs->JoinPath(temp_dir, path));
		}
	});
	REQUIRE_FALSE(live_files.empty());

	auto db2 = make_uniq<DuckDB>(nullptr);
	auto con2 = make_uniq<Connection>(*db2);
	REQUIRE_NO_FAIL(con2->Query("SET temp_directory='" + temp_dir + "'"));
	REQUIRE_NO_FAIL(con2->Query("SET memory_limit='4MB'"));
	REQUIRE_NO_FAIL(con2->Query("CREATE TEMP TABLE t AS SELECT random() FROM range(200000)"));

	for (const auto &path : live_files) {
		REQUIRE(fs->FileExists(path));
	}

	con2.reset();
	db2.reset();
	con1.reset();
	db1.reset();
	fs->RemoveDirectory(temp_dir);
}
