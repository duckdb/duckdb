#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple relation API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	shared_ptr<Relation> tbl, filter, proj, proj2;

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
	REQUIRE_NOTHROW(result = proj->Limit(1)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	// and an offset
	REQUIRE_NOTHROW(result = proj->Limit(1, 1)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {30}));

	// lets add some aliases
	REQUIRE_NOTHROW(proj = filter->Project("i + 1 AS a"));
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	// we can check the column names
	REQUIRE(proj->Columns()[0].name == "a");
	REQUIRE(proj->Columns()[0].type == SQLType::INTEGER);

	// we can also alias like this
	REQUIRE_NOTHROW(proj = filter->Project("i + 1", "a"));
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	// we can check the column names
	REQUIRE(proj->Columns()[0].name == "a");
	REQUIRE(proj->Columns()[0].type == SQLType::INTEGER);

	// now we can use that column to perform additional projections
	REQUIRE_NOTHROW(result = proj->Project("a + 1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {3, 5}));

	// we can also filter on this again
	REQUIRE_NOTHROW(result = proj->Filter("a=2")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// filters can also contain conjunctions
	REQUIRE_NOTHROW(result = proj->Filter("a=2 OR a=4")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));

	// now test ordering
	REQUIRE_NOTHROW(result = proj->Order("a DESC")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {4, 2}));

	// top n
	REQUIRE_NOTHROW(result = proj->Order("a")->Limit(1)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE_NOTHROW(result = proj->Order("a DESC")->Limit(1)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {4}));

	// test set operations
	REQUIRE_NOTHROW(result = tbl->Union(tbl)->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2, 2, 3, 3}));
	REQUIRE_NOTHROW(result = tbl->Except(tbl)->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE_NOTHROW(result = tbl->Intersect(tbl)->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE_NOTHROW(result = tbl->Except(tbl->Filter("i=2"))->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
	REQUIRE_NOTHROW(result = tbl->Intersect(tbl->Filter("i=2"))->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// set operations with projections
	REQUIRE_NOTHROW(proj = tbl->Project("i::TINYINT AS i, i::SMALLINT, i::BIGINT, i::VARCHAR"));
	REQUIRE_NOTHROW(proj2 = tbl->Project("(i+10)::TINYINT, (i+10)::SMALLINT, (i+10)::BIGINT, (i+10)::VARCHAR"));
	REQUIRE_NOTHROW(result = proj->Union(proj2)->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 3, {"1", "2", "3", "11", "12", "13"}));

	// distinct
	REQUIRE_NOTHROW(result = tbl->Union(tbl)->Union(tbl)->Distinct()->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
}

TEST_CASE("Test view creation of relations", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	shared_ptr<Relation> tbl, filter, proj, proj2;

	// create some tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// simple view creation
	REQUIRE_NOTHROW(tbl = con.Table("integers"));
	REQUIRE_NOTHROW(result = tbl->SQL("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// add a projection
	REQUIRE_NOTHROW(result = tbl->Project("i + 1")->SQL("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// multiple projections
	proj = tbl->Project("i + 1", "i");
	for(idx_t i = 0; i < 10; i++) {
		proj = proj->Project("i + 1", "i");
	}
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 14}));
	REQUIRE_NOTHROW(result = proj->SQL("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 14}));

	// we can also use more complex SQL
	REQUIRE_NOTHROW(result = proj->SQL("test", "SELECT SUM(t1.i) FROM test t1 JOIN test t2 ON t1.i=t2.i"));
	REQUIRE(CHECK_COLUMN(result, 0, {39}));

	// limit
	REQUIRE_NOTHROW(result = tbl->Limit(1)->SQL("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// order
	REQUIRE_NOTHROW(result = tbl->Order("i DESC")->Limit(1)->SQL("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// union
	auto node = tbl->Order("i DESC")->Limit(1);
	REQUIRE_NOTHROW(result = node->Union(node)->SQL("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3}));
	// distinct
	REQUIRE_NOTHROW(result = node->Union(node)->Distinct()->SQL("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// manually create views and query from them
	result = con.Query("SELECT i+1 FROM integers UNION SELECT i+10 FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4, 11, 12, 13}));

	tbl->Project("i + 1")->CreateView("test1");
	tbl->Project("i + 10")->CreateView("test2");
	result = con.Query("SELECT * FROM test1 UNION SELECT * FROM test2 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4, 11, 12, 13}));
}

TEST_CASE("Test table creations using the relation API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	shared_ptr<Relation> values;

	// create a table from a Values statement
	REQUIRE_NOTHROW(values = con.Values({{1, 10}, {2, 5}, {3, 4}}, {"i", "j"}));
	REQUIRE_NOTHROW(values->Create("integers"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));

	// insert from a set of values
	REQUIRE_NOTHROW(con.Values({{4, 7}, {5, 8}})->Insert("integers"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4, 7, 8}));

	// create a table from a query
	REQUIRE_NOTHROW(con.Table("integers")->Filter("i BETWEEN 3 AND 4")->Project("i + 1 AS k, 'hello' AS l")->Create("new_values"));

	result = con.Query("SELECT * FROM new_values ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "hello"}));
}

TEST_CASE("Test table deletions and updates", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	auto tbl = con.Table("integers");

	// update
	tbl->Update("i=i+10", "i=2");

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 12}));

	tbl->Delete("i=3");

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 12}));
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
