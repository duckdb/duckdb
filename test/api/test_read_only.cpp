#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test connection using a read only database", "[readonly]") {
	auto dbdir = TestCreatePath("read_only_test");
	unique_ptr<DuckDB> db, db2;
	unique_ptr<Connection> con;
	// make sure the database does not exist
	DeleteDatabase(dbdir);

	DBConfig readonly_config;
	readonly_config.use_temporary_directory = false;
	readonly_config.access_mode = AccessMode::READ_ONLY;

	// cannot create read-only memory database
	REQUIRE_THROWS(db = make_unique<DuckDB>(nullptr, &readonly_config));
	// cannot create a read-only database in a new directory
	REQUIRE_THROWS(db = make_unique<DuckDB>(dbdir, &readonly_config));

	// create the database file and initialize it with data
	db = make_unique<DuckDB>(dbdir);
	con = make_unique<Connection>(*db);
	REQUIRE_NO_FAIL(con->Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con->Query("INSERT INTO integers VALUES (1), (2), (3), (4), (5)"));
	con.reset();
	db.reset();

	// now connect in read-only mode
	REQUIRE_NOTHROW(db = make_unique<DuckDB>(dbdir, &readonly_config));
	con = make_unique<Connection>(*db);

	// we can query the database
	auto result = con->Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	// however, we can't perform DDL statements
	REQUIRE_FAIL(con->Query("CREATE TABLE integers2(i INTEGER)"));
	REQUIRE_FAIL(con->Query("ALTER TABLE integers RENAME COLUMN i TO k"));
	REQUIRE_FAIL(con->Query("DROP TABLE integers"));
	REQUIRE_FAIL(con->Query("CREATE SEQUENCE seq"));
	REQUIRE_FAIL(con->Query("CREATE VIEW v1 AS SELECT * FROM integers"));
	// neither can we insert/update/delete data
	REQUIRE_FAIL(con->Query("INSERT INTO integers VALUES (3)"));
	REQUIRE_FAIL(con->Query("UPDATE integers SET i=5"));
	REQUIRE_FAIL(con->Query("DELETE FROM integers"));
	// we can run explain queries
	REQUIRE_NO_FAIL(con->Query("EXPLAIN SELECT * FROM integers"));
	// and run prepared statements
	REQUIRE_NO_FAIL(con->Query("PREPARE v1 AS SELECT * FROM integers"));
	REQUIRE_NO_FAIL(con->Query("EXECUTE v1"));
	REQUIRE_NO_FAIL(con->Query("DEALLOCATE v1"));
	// we can also prepare a DDL/update statement
	REQUIRE_NO_FAIL(con->Query("PREPARE v1 AS INSERT INTO integers VALUES ($1)"));
	// however, executing it fails then!
	REQUIRE_FAIL(con->Query("EXECUTE v1(3)"));

	// we can create, alter and query temporary tables however
	REQUIRE_NO_FAIL(con->Query("CREATE TEMPORARY TABLE integers2(i INTEGER)"));
	REQUIRE_NO_FAIL(con->Query("INSERT INTO integers2 VALUES (1), (2), (3), (4), (5)"));
	REQUIRE_NO_FAIL(con->Query("UPDATE integers2 SET i=i+1"));
	result = con->Query("DELETE FROM integers2 WHERE i=3");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con->Query("ALTER TABLE integers2 RENAME COLUMN i TO k"));
	result = con->Query("SELECT k FROM integers2 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4, 5, 6}));
	REQUIRE_NO_FAIL(con->Query("DROP TABLE integers2"));

	// also temporary views and sequences
	REQUIRE_NO_FAIL(con->Query("CREATE TEMPORARY SEQUENCE seq"));
	result = con->Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(con->Query("DROP SEQUENCE seq"));

	REQUIRE_NO_FAIL(con->Query("CREATE TEMPORARY VIEW v1 AS SELECT 42"));
	result = con->Query("SELECT * FROM v1");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE_NO_FAIL(con->Query("DROP VIEW v1"));

	con.reset();
	db.reset();
	// FIXME: these tests currently don't work as we don't do any locking of the database directory
	// this should be fixed with the new storage
	// we can connect multiple read only databases to the same dbdir
	// REQUIRE_NOTHROW(db = make_unique<DuckDB>(dbdir, true));
	// REQUIRE_NOTHROW(db2 = make_unique<DuckDB>(dbdir, true));
	// db.reset();
	// db2.reset();

	// // however, if there is read-only database, we can't connect a read-write database
	// REQUIRE_NOTHROW(db = make_unique<DuckDB>(dbdir, true));
	// REQUIRE_THROWS(db2 = make_unique<DuckDB>(dbdir));
	// db.reset();
	// db2.reset();

	// // if we add a read-write database first, we can't add a reading database afterwards either
	// REQUIRE_NOTHROW(db = make_unique<DuckDB>(dbdir));
	// REQUIRE_THROWS(db2 = make_unique<DuckDB>(dbdir, true));
	// db.reset();
	// db2.reset();
	DeleteDatabase(dbdir);
}
