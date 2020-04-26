#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test case insensitive collation", "[collate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOCASE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('hello'), ('WoRlD'), ('world'), ('Mühleisen')"));

	// collate in equality
	result = con.Query("SELECT * FROM collate_test WHERE s='HeLlo'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con.Query("SELECT * FROM collate_test WHERE s='MÜhleisen'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Mühleisen"}));
	result = con.Query("SELECT * FROM collate_test WHERE s='world'");
	REQUIRE(CHECK_COLUMN(result, 0, {"WoRlD", "world"}));

	// join with collation
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_join_table(s VARCHAR, i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_join_table VALUES ('HeLlO', 1), ('mÜHLEISEN', 3)"));

	result = con.Query("SELECT collate_test.s, collate_join_table.s, i FROM collate_test JOIN collate_join_table ON "
	                   "(collate_test.s=collate_join_table.s) ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "Mühleisen"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"HeLlO", "mÜHLEISEN"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3}));

	// ORDER BY with collation
	REQUIRE_NO_FAIL(con.Query("DROP TABLE collate_test"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOCASE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Hallo'), ('ham'), ('HELLO'), ('hElp')"));

	result = con.Query("SELECT * FROM collate_test ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hallo", "ham", "HELLO", "hElp"}));

	// DISTINCT with collation
	REQUIRE_NO_FAIL(con.Query("DROP TABLE collate_test"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOCASE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Hallo'), ('hallo')"));

	result = con.Query("SELECT DISTINCT s FROM collate_test");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hallo"}));

	// LIKE with collation: not yet supported
	// REQUIRE_NO_FAIL(con.Query("DROP TABLE collate_test"));
	// REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOCASE)"));
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Hallo'), ('hallo')"));

	// result = con.Query("SELECT * FROM collate_test WHERE s LIKE 'h%'");
	// REQUIRE(CHECK_COLUMN(result, 0, {"Hallo", "hallo"}));
	// result = con.Query("SELECT * FROM collate_test WHERE s LIKE 'HA%'");
	// REQUIRE(CHECK_COLUMN(result, 0, {"Hallo", "hallo"}));
}

TEST_CASE("Test accent insensitive collation", "[collate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOACCENT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Mühleisen'), ('Hëllö')"));

	// collate in equality
	result = con.Query("SELECT * FROM collate_test WHERE s='Muhleisen'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Mühleisen"}));
	result = con.Query("SELECT * FROM collate_test WHERE s='mühleisen'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM collate_test WHERE s='Hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hëllö"}));

	// join with collation
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_join_table(s VARCHAR, i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_join_table VALUES ('Hello', 1), ('Muhleisen', 3)"));

	result = con.Query("SELECT collate_test.s, collate_join_table.s, i FROM collate_test JOIN collate_join_table ON "
	                   "(collate_test.s=collate_join_table.s) ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hëllö", "Mühleisen"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"Hello", "Muhleisen"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 3}));

	// ORDER BY with collation
	REQUIRE_NO_FAIL(con.Query("DROP TABLE collate_test"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOACCENT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Hällo'), ('Hallo'), ('Hello')"));

	result = con.Query("SELECT * FROM collate_test ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hällo", "Hallo", "Hello"}));

	// DISTINCT with collation
	REQUIRE_NO_FAIL(con.Query("DROP TABLE collate_test"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOACCENT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Hällo'), ('Hallo')"));

	result = con.Query("SELECT DISTINCT s FROM collate_test");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hällo"}));

	// LIKE with collation: not yet supported
	// REQUIRE_NO_FAIL(con.Query("DROP TABLE collate_test"));
	// REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOACCENT)"));
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Hällo'), ('Hallö')"));

	// result = con.Query("SELECT * FROM collate_test WHERE s LIKE '%a%'");
	// REQUIRE(CHECK_COLUMN(result, 0, {"Hällo", "Hallö"}));
	// result = con.Query("SELECT * FROM collate_test WHERE s LIKE '%_ö'");
	// REQUIRE(CHECK_COLUMN(result, 0, {"Hällo", "Hallö"}));
	// result = con.Query("SELECT * FROM collate_test WHERE s LIKE '%_ó'");
	// REQUIRE(CHECK_COLUMN(result, 0, {"Hällo", "Hallö"}));
}

TEST_CASE("Test combined collations", "[collate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOACCENT.NOCASE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('Mühleisen'), ('Hëllö')"));

	// collate in equality
	result = con.Query("SELECT * FROM collate_test WHERE s='Muhleisen'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Mühleisen"}));
	result = con.Query("SELECT * FROM collate_test WHERE s='muhleisen'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Mühleisen"}));
	result = con.Query("SELECT * FROM collate_test WHERE s='hEllô'");
	REQUIRE(CHECK_COLUMN(result, 0, {"Hëllö"}));
}

TEST_CASE("Test COLLATE in individual expressions", "[collate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('hEllO'), ('WöRlD'), ('wozld')"));

	// collate in equality
	result = con.Query("SELECT 'hëllo' COLLATE NOACCENT='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT * FROM collate_test WHERE s='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT * FROM collate_test WHERE s='hello' COLLATE NOCASE");
	REQUIRE(CHECK_COLUMN(result, 0, {"hEllO"}));
	result = con.Query("SELECT * FROM collate_test WHERE s COLLATE NOCASE='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hEllO"}));

	// conflict in collate statements results in an error
	REQUIRE_FAIL(con.Query("SELECT * FROM collate_test WHERE s COLLATE NOCASE='hello' COLLATE NOACCENT"));

	// we can also do this in the order
	result = con.Query("SELECT * FROM collate_test ORDER BY s COLLATE NOCASE");
	REQUIRE(CHECK_COLUMN(result, 0, {"hEllO", "wozld", "WöRlD"}));
	result = con.Query("SELECT * FROM collate_test ORDER BY s COLLATE NOCASE.NOACCENT");
	REQUIRE(CHECK_COLUMN(result, 0, {"hEllO", "WöRlD", "wozld"}));
}

TEST_CASE("Test default collations", "[collate]") {
	unique_ptr<QueryResult> result;
	DBConfig config;
	config.collation = "NOCASE";
	DuckDB db(nullptr, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO collate_test VALUES ('hEllO'), ('WöRlD'), ('wozld')"));

	// collate in equality
	result = con.Query("SELECT COUNT(*) FROM collate_test WHERE 'BlA'='bLa'");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT * FROM collate_test WHERE s='hello'");
	REQUIRE(CHECK_COLUMN(result, 0, {"hEllO"}));

	// collate in order
	result = con.Query("SELECT * FROM collate_test ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {"hEllO", "wozld", "WöRlD"}));

	// switch default collate using pragma
	REQUIRE_NO_FAIL(con.Query("PRAGMA default_collation='NOCASE.NOACCENT'"));

	result = con.Query("SELECT * FROM collate_test ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {"hEllO", "WöRlD", "wozld"}));
}

TEST_CASE("Get list of collations", "[collate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	result = con.Query("PRAGMA collations");
	REQUIRE(CHECK_COLUMN(result, 0, {"noaccent", "nocase"}));

	REQUIRE_FAIL(con.Query("PRAGMA collations=3"));
}

TEST_CASE("Test unsupported collations", "[collate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE blabla)"));
	REQUIRE_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE NOACCENT.NOACCENT)"));
	REQUIRE_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE 1)"));
	REQUIRE_FAIL(con.Query("CREATE TABLE collate_test(s VARCHAR COLLATE 'hello')"));

	REQUIRE_FAIL(con.Query("PRAGMA default_collation='blabla'"));
}
