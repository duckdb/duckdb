#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple relation API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	shared_ptr<Relation> tbl, filter, proj;

	// create some tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// simple projection
	REQUIRE_NOTHROW(tbl = con.Table("integers"));
	REQUIRE_NOTHROW(result = tbl->Project("i + 1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// we support * expressions
	REQUIRE_NOTHROW(result = tbl->Project("*")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// we can also read the table directly
	REQUIRE_NOTHROW(result = tbl->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// we can stack projections
	REQUIRE_NOTHROW(result = tbl->Project("i + 1 AS i")->Project("i + 1 AS i")->Project("i + 1 AS i")->Project("i + 1 AS i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {5, 6, 7}));

	// we can execute the same projection multiple times
	REQUIRE_NOTHROW(proj = tbl->Project("i + 1"));
	for(idx_t i = 0; i < 10; i++) {
		REQUIRE_NOTHROW(result = proj->Execute());
		REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	}

	// filter and projection
	REQUIRE_NOTHROW(filter = tbl->Filter("i <> 2"));
	REQUIRE_NOTHROW(proj = filter->Project("i + 1"));
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));

	// we can reuse the same filter again and perform a different projection
	REQUIRE_NOTHROW(proj = filter->Project("i * 10"));
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {10, 30}));

	// add a limit
	// REQUIRE_NOTHROW(result = proj->Limit(1)->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {10}));

	// // and an offset
	// REQUIRE_NOTHROW(result = proj->Limit(1, 1)->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {30}));

	// // lets add some aliases
	// REQUIRE_NOTHROW(proj = filter->Project("i + 1 AS a"));
	// // we can check the column names
	// REQUIRE(proj->Columns()[0].name == "a");
	// REQUIRE(proj->Columns()[0].type == SQLType::INTEGER);

	// // we can also alias like this
	// REQUIRE_NOTHROW(proj = filter->Project("i + 1", "a"));
	// // we can check the column names
	// REQUIRE(proj->Columns()[0].name == "a");
	// REQUIRE(proj->Columns()[0].type == SQLType::INTEGER);

	// // now we can use that column to perform additional projections
	// REQUIRE_NOTHROW(result = proj->Project("a + 1")->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));

	// // we can also filter on this again
	// REQUIRE_NOTHROW(result = proj->Filter("a=2")->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// // filters can also contain conjunctions
	// REQUIRE_NOTHROW(result = proj->Filter("a=2 OR a=4")->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));

	// // now test ordering
	// REQUIRE_NOTHROW(result = proj->Order("a DESC")->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {4, 2}));

	// // top n
	// REQUIRE_NOTHROW(result = proj->Order("a")->Limit(1)->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// REQUIRE_NOTHROW(result = proj->Order("a DESC")->Limit(1)->Execute());
	// REQUIRE(CHECK_COLUMN(result, 0, {4}));
}

// TEST_CASE("We can mix statements from multiple databases", "[api]") {
// 	DuckDB db(nullptr), db2(nullptr);
// 	Connection con(db), con2(db);
// 	unique_ptr<QueryResult> result;

// 	// create some tables
// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
// 	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));
// 	REQUIRE_NO_FAIL(con2.Query("CREATE TABLE integers(i INTEGER)"));
// 	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (4, 5, 6)"));

// 	// we can combine tables from different databases
// 	auto i1 = con.Table("integers");
// 	auto i2 = con2.Table("integers");

// 	result = i2->Union(i1, "i")->Order("i")->Execute();
// 	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6}));
// }
