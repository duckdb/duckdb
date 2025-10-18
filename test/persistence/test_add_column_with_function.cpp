#include "catch.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test the default value of added column is FUNCTION", "[persistence]") {
	auto test_dir = TestDirectoryPath();
	string db_name = "my_db";
	string db_path = test_dir + "/" + db_name + ".db";
	string wal_path = db_path + ".wal";

	std::remove(db_path.c_str());
	std::remove(wal_path.c_str());

	DuckDB *db = new DuckDB(db_path);
	Connection con1(*db);

	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE t (id INT);"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t (id) VALUES(1);"));

	REQUIRE_NO_FAIL(con1.Query("BEGIN;"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t(id) VALUES(2);"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD c VARCHAR(30) DEFAULT UUID();"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t(id) VALUES(3);"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD d FLOAT DEFAULT RANDOM();"));
	REQUIRE_NO_FAIL(con1.Query("COMMIT;"));

	auto r1 = con1.Query("SELECT * FROM t;");
	delete db;

	// Crash Recovery, will replay the WAL
	db = new DuckDB(db_path);
	Connection con2(*db);
	auto r2 = con2.Query("SELECT * FROM t;");
	delete db;

	// Check the `stable_result` is applied
	REQUIRE(r1->Equals(*r2));
}

int64_t filesize(const char *filename) {
	std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
	auto pos = in.tellg();
	if (pos == std::streampos(-1)) {
		return -1;
	}
	return static_cast<int64_t>(pos);
}

TEST_CASE("Test Rollback added column with default value FUNCTION", "[persistence]") {
	auto test_dir = TestDirectoryPath();
	string db_name = "my_db";
	string db_path = test_dir + "/" + db_name + ".db";
	string wal_path = db_path + ".wal";

	std::remove(db_path.c_str());
	std::remove(wal_path.c_str());

	DuckDB *db = new DuckDB(db_path);
	Connection con1(*db);

	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE t (id INT, a INT);"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t (id, a) VALUES(1, 1);"));
	REQUIRE_NO_FAIL(con1.Query("CHECKPOINT;"));

	auto before_size = filesize(db_path.c_str());

	// Rollback added column with default value FUNCTION, need to free the data blocks
	REQUIRE_NO_FAIL(con1.Query("BEGIN;"));
	REQUIRE_NO_FAIL(con1.Query("UPDATE t SET a = 3 WHERE id = 1;"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t(id, a) VALUES(2, 2);"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD c VARCHAR(30) DEFAULT UUID();"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t(id, a) VALUES(3, 3);"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD d FLOAT DEFAULT RANDOM();"));
	REQUIRE_NO_FAIL(con1.Query("ROLLBACK;"));

	// INSERT+DELETE to make the next CHECKPOINT actually executed
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t(id, a) VALUES(2, 2);"));
	REQUIRE_NO_FAIL(con1.Query("DELETE FROM t WHERE id = 2;"));

	REQUIRE_NO_FAIL(con1.Query("CHECKPOINT;"));

	// Check the data blocks used by ADD COLUMN are truncated
	auto after_size = filesize(db_path.c_str());
	REQUIRE(before_size == after_size);

	auto r1 = con1.Query("SELECT * FROM t;");
	delete db;

	db = new DuckDB(db_path);
	Connection con2(*db);
	auto r2 = con2.Query("SELECT * FROM t;");
	delete db;

	REQUIRE(r1->Equals(*r2));
}

TEST_CASE("Test ADD and DROP column with default value FUNCTION", "[persistence]") {
	auto test_dir = TestDirectoryPath();
	string db_name = "my_db";
	string db_path = test_dir + "/" + db_name + ".db";
	string wal_path = db_path + ".wal";

	std::remove(db_path.c_str());
	std::remove(wal_path.c_str());

	DuckDB *db = new DuckDB(db_path);
	Connection con1(*db);

	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE t (id INT);"));
	REQUIRE_NO_FAIL(con1.Query("INSERT INTO t (id) VALUES(1),(2),(3);"));
	REQUIRE_NO_FAIL(con1.Query("CHECKPOINT;"));

	auto before_size = filesize(db_path.c_str());

	// ADD a column, and then DROP it
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD c VARCHAR(30) DEFAULT UUID();"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t DROP c;"));

	// ADD and DROP a column in one transaction
	REQUIRE_NO_FAIL(con1.Query("BEGIN;"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD c TIMESTAMP DEFAULT NOW();"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t DROP c;"));
	REQUIRE_NO_FAIL(con1.Query("COMMIT;"));

	// ROLLBACK the ADD and DROP
	REQUIRE_NO_FAIL(con1.Query("BEGIN;"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD c FLOAT DEFAULT RANDOM();"));
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t DROP c;"));
	REQUIRE_NO_FAIL(con1.Query("ROLLBACK;"));

	// Check the file size
	REQUIRE_NO_FAIL(con1.Query("CHECKPOINT;"));
	auto after_size = filesize(db_path.c_str());
	REQUIRE(before_size == after_size);

	// Finally, ADD a column with random default value
	REQUIRE_NO_FAIL(con1.Query("ALTER TABLE t ADD c FLOAT DEFAULT RANDOM();"));

	auto r1 = con1.Query("SELECT * FROM t;");
	delete db;

	// Crash Recovery, will replay the WAL
	db = new DuckDB(db_path);
	Connection con2(*db);
	auto r2 = con2.Query("SELECT * FROM t;");
	delete db;

	REQUIRE(r1->Equals(*r2));
}
