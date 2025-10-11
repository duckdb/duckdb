#include "catch.hpp"
#include "test_helpers.hpp"

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
	REQUIRE_NO_FAIL(con1.Query("COMMIT;"));

	auto r1 = con1.Query("SELECT * FROM t;");
	delete db;

	db = new DuckDB(db_path);
	Connection con2(*db);
	auto r2 = con2.Query("SELECT * FROM t;");
	delete db;

    REQUIRE(r1->Equals(*r2));
}

// todo: alter add column, and then rollback?