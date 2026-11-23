#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static shared_ptr<AttachedDatabase> AttachAndHold(Connection &con, const string &path) {
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS oa"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE oa.t AS SELECT range i FROM range(1000)"));
	auto held = DatabaseManager::Get(*con.context).GetDatabase(Identifier("oa"));
	REQUIRE(held);
	return held;
}

static void VerifyReattach(DuckDB &db, const string &path) {
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS oa"));
	auto result = con.Query("SELECT COUNT(*), SUM(i) FROM oa.t");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {499500}));
	REQUIRE_NO_FAIL(con.Query("DETACH oa"));
}

TEST_CASE("Releasing a reference that outlives a DETACH closes the database", "[api]") {
	auto &fs = TestLocalFileSystem();
	auto path = TestCreatePath("detach_held_reference.db");
	auto wal_path = path + ".wal";
	DeleteDatabase(path);

	DuckDB db(nullptr);
	Connection con1(db), con2(db);
	auto held = AttachAndHold(con1, path);
	REQUIRE_NO_FAIL(con2.Query("DETACH oa"));
	// the held reference keeps the database open
	REQUIRE(fs.FileExists(wal_path));
	held.reset();
	// the last release closes with the close action of the detaching connection
	REQUIRE(!fs.FileExists(wal_path));

	VerifyReattach(db, path);
	DeleteDatabase(path);
}

TEST_CASE("Releasing a reference that outlives a DETACH honors checkpoint_on_detach", "[api]") {
	auto &fs = TestLocalFileSystem();
	auto path = TestCreatePath("detach_held_reference_skip.db");
	auto wal_path = path + ".wal";
	DeleteDatabase(path);

	DuckDB db(nullptr);
	Connection con1(db), con2(db);
	auto held = AttachAndHold(con1, path);
	REQUIRE_NO_FAIL(con2.Query("SET checkpoint_on_detach = 'DISABLED'"));
	REQUIRE_NO_FAIL(con2.Query("DETACH oa"));
	held.reset();
	// closed without a checkpoint: the WAL remains and is replayed on the next attach
	REQUIRE(fs.FileExists(wal_path));

	VerifyReattach(db, path);
	DeleteDatabase(path);
}

TEST_CASE("Releasing a reference that outlives an ATTACH OR REPLACE closes the database", "[api]") {
	auto &fs = TestLocalFileSystem();
	auto path = TestCreatePath("replace_held_reference.db");
	auto other_path = TestCreatePath("replace_held_reference_other.db");
	auto wal_path = path + ".wal";
	DeleteDatabase(path);
	DeleteDatabase(other_path);

	DuckDB db(nullptr);
	Connection con1(db), con2(db);
	auto held = AttachAndHold(con1, path);
	REQUIRE_NO_FAIL(con2.Query("ATTACH OR REPLACE '" + other_path + "' AS oa"));
	REQUIRE_NO_FAIL(con2.Query("DETACH oa"));
	REQUIRE(fs.FileExists(wal_path));
	held.reset();
	REQUIRE(!fs.FileExists(wal_path));

	VerifyReattach(db, path);
	DeleteDatabase(path);
	DeleteDatabase(other_path);
}
