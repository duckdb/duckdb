#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "iostream"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test simple relation API", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
	shared_ptr<Relation> tbl, filter, proj, proj2, v1, v2, v3;

	// create some tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// simple projection
	REQUIRE_NOTHROW(tbl = con.Table("integers"));
	REQUIRE_NOTHROW(result = tbl->Project("i + 1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	REQUIRE_NOTHROW(result = tbl->Project(duckdb::vector<string> {"i + 1", "i + 2"})->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 4, 5}));

	REQUIRE_NOTHROW(result = tbl->Project(duckdb::vector<string> {"i + 1"}, {"i"})->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// we support * expressions
	REQUIRE_NOTHROW(result = tbl->Project("*")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// we can also read the table directly
	REQUIRE_NOTHROW(result = tbl->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// we can stack projections
	REQUIRE_NOTHROW(
	    result =
	        tbl->Project("i + 1 AS i")->Project("i + 1 AS i")->Project("i + 1 AS i")->Project("i + 1 AS i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {5, 6, 7}));

	// we can execute the same projection multiple times
	REQUIRE_NOTHROW(proj = tbl->Project("i + 1"));
	for (idx_t i = 0; i < 10; i++) {
		REQUIRE_NOTHROW(result = proj->Execute());
		REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	}

	result = con.Query("SELECT i+1 FROM (SELECT * FROM integers WHERE i <> 2) relation");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	// filter and projection
	REQUIRE_NOTHROW(filter = tbl->Filter("i <> 2"));
	REQUIRE_NOTHROW(proj = filter->Project("i + 1"));
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));

	// multi filter
	REQUIRE_NOTHROW(result = tbl->Filter(duckdb::vector<string> {"i <> 2", "i <> 3"})->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

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
	REQUIRE(proj->Columns()[0].Name() == "a");
	REQUIRE(proj->Columns()[0].Type() == LogicalType::INTEGER);

	// we can also alias like this
	REQUIRE_NOTHROW(proj = filter->Project("i + 1", "a"));
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	// we can check the column names
	REQUIRE(proj->Columns()[0].Name() == "a");
	REQUIRE(proj->Columns()[0].Type() == LogicalType::INTEGER);

	// now we can use that column to perform additional projections
	REQUIRE_NOTHROW(result = proj->Project("a + 1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {3, 5}));

	// we can also filter on this again
	REQUIRE_NOTHROW(result = proj->Filter("a=2")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// filters can also contain conjunctions
	REQUIRE_NOTHROW(result = proj->Filter("a=2 OR a=4")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));

	// alias
	REQUIRE_NOTHROW(result = proj->Project("a + 1")->Alias("bla")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {3, 5}));

	// now test ordering
	REQUIRE_NOTHROW(result = proj->Order("a DESC")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {4, 2}));
	REQUIRE_NOTHROW(result = proj->Order(duckdb::vector<string> {"a DESC", "a ASC"})->Execute());
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
	REQUIRE_NOTHROW(result = tbl->Union(tbl)->Union(tbl)->Distinct()->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// join
	REQUIRE_NOTHROW(v1 = con.Values({{1, 10}, {2, 5}, {3, 4}}, {"id", "j"}, "v1"));
	REQUIRE_NOTHROW(v2 = con.Values({{1, 27}, {2, 8}, {3, 20}}, {"id", "k"}, "v2"));
	REQUIRE_NOTHROW(v3 = con.Values({{1, 2}, {2, 6}, {3, 10}}, {"id", "k"}, "v3"));
	REQUIRE_NOTHROW(result = v1->Join(v2, "v1.id=v2.id")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {27, 8, 20}));

	// asof join
	REQUIRE_NOTHROW(v1 = con.Values({{1, 10}, {6, 5}, {8, 4}, {10, 23}, {12, 12}, {15, 14}}, {"id", "j"}, "v1"));
	REQUIRE_NOTHROW(v2 = con.Values({{4, 27}, {8, 8}, {14, 20}}, {"id", "k"}, "v2"));
	REQUIRE_NOTHROW(result = v1->Join(v2, "v1.id>=v2.id", JoinType::INNER, JoinRefType::ASOF)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {6, 8, 10, 12, 15}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 4, 23, 12, 14}));
	REQUIRE(CHECK_COLUMN(result, 2, {4, 8, 8, 8, 14}));
	REQUIRE(CHECK_COLUMN(result, 3, {27, 8, 8, 8, 20}));

	REQUIRE_NOTHROW(v1 = con.Values({{1, 10}, {2, 5}, {3, 4}}, {"id", "j"}, "v1"));
	REQUIRE_NOTHROW(v2 = con.Values({{1, 27}, {2, 8}, {3, 20}}, {"id", "k"}, "v2"));
	REQUIRE_NOTHROW(v3 = con.Values({{1, 2}, {2, 6}, {3, 10}}, {"id", "k"}, "v3"));

	// projection after a join
	REQUIRE_NOTHROW(result = v1->Join(v2, "v1.id=v2.id")->Project("v1.id+v2.id, j+k")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {37, 13, 24}));

	// chain multiple joins
	auto multi_join = v1->Join(v2, "v1.id=v2.id")->Join(v3, "v1.id=v3.id");
	REQUIRE_NOTHROW(result = multi_join->Project("v1.id+v2.id+v3.id")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {3, 6, 9}));

	// multiple joins followed by a filter and a projection
	REQUIRE_NOTHROW(result = multi_join->Filter("v1.id=1")->Project("v1.id+v2.id+v3.id")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// multiple joins followed by multiple filters
	REQUIRE_NOTHROW(result = multi_join->Filter("v1.id>0")
	                             ->Filter("v2.id < 3")
	                             ->Filter("v3.id=2")
	                             ->Project("v1.id+v2.id+v3.id")
	                             ->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	// test explain
	REQUIRE_NO_FAIL(multi_join->Explain());

	// incorrect API usage
	REQUIRE_THROWS(tbl->Project(duckdb::vector<string> {})->Execute());
	REQUIRE_THROWS(tbl->Project(duckdb::vector<string> {"1, 2, 3"})->Execute());
	REQUIRE_THROWS(tbl->Project(duckdb::vector<string> {""})->Execute());
	REQUIRE_THROWS(tbl->Filter("i=1, i=2")->Execute());
	REQUIRE_THROWS(tbl->Filter("")->Execute());
	REQUIRE_THROWS(tbl->Filter(duckdb::vector<string> {})->Execute());
	REQUIRE_THROWS(tbl->Filter(duckdb::vector<string> {"1, 2, 3"})->Execute());
	REQUIRE_THROWS(tbl->Filter(duckdb::vector<string> {""})->Execute());
	REQUIRE_THROWS(tbl->Order(duckdb::vector<string> {})->Execute());
	REQUIRE_THROWS(tbl->Order(duckdb::vector<string> {"1, 2, 3"})->Execute());
	REQUIRE_THROWS(tbl->Order("1 LIMIT 3")->Execute());
	REQUIRE_THROWS(tbl->Order("1; SELECT 42")->Execute());
	REQUIRE_THROWS(tbl->Join(tbl, "")->Execute());
	REQUIRE_THROWS(tbl->Join(tbl, "a, a+1")->Execute());
	REQUIRE_THROWS(tbl->Join(tbl, "a, bla.bla")->Execute());
}

TEST_CASE("Test combinations of set operations", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
	shared_ptr<Relation> values, v1, v2, v3;

	REQUIRE_NOTHROW(values = con.Values({{1, 10}, {2, 5}, {3, 4}}, {"i", "j"}));

	// union between values
	auto vunion = values->Union(values);
	REQUIRE_NOTHROW(result = vunion->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2, 2, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 10, 5, 5, 4, 4}));

	// different ops after a union
	// order and limit
	REQUIRE_NOTHROW(result = vunion->Order("i")->Limit(1)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	// multiple orders and limits
	REQUIRE_NOTHROW(result = vunion->Order("i")->Limit(4)->Order("j")->Limit(2)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 5}));
	// filter
	REQUIRE_NOTHROW(result = vunion->Filter("i=1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 10}));
	// multiple filters
	REQUIRE_NOTHROW(result = vunion->Filter("i<3")->Filter("j=5")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 5}));
	// distinct
	REQUIRE_NOTHROW(result = vunion->Distinct()->Order("j DESC")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));
	// multiple distincts followed by a top-n
	REQUIRE_NOTHROW(result = vunion->Distinct()->Distinct()->Distinct()->Order("j DESC")->Limit(2)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5}));
	// top-n followed by multiple distincts
	REQUIRE_NOTHROW(result = vunion->Order("j DESC")->Limit(2)->Distinct()->Distinct()->Distinct()->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));

	// multiple set ops
	REQUIRE_NOTHROW(result = vunion->Union(vunion)->Distinct()->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));
	REQUIRE_NOTHROW(result = vunion->Intersect(vunion)->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));
	REQUIRE_NOTHROW(result = vunion->Except(vunion)->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	REQUIRE(CHECK_COLUMN(result, 1, {}));

	// setops require the same amount of columns on both sides
	REQUIRE_NOTHROW(v1 = con.Values("(1, 2), (3, 4)"));
	REQUIRE_NOTHROW(v2 = con.Values("(1)"));
	REQUIRE_THROWS(v1->Union(v2)->Execute());

	// setops require the same types on both sides
	REQUIRE_NOTHROW(v1 = con.Values("(DATE '1992-01-01', 2)"));
	REQUIRE_NOTHROW(v2 = con.Values("(3.0, 'hello')"));
	REQUIRE_FAIL(v1->Union(v2)->Execute());
}

TEST_CASE("Test combinations of joins", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
	shared_ptr<Relation> values, vjoin;

	REQUIRE_NOTHROW(values = con.Values({{1, 10}, {2, 5}, {3, 4}}, {"i", "j"}));

	auto v1 = values->Alias("v1");
	auto v2 = values->Alias("v2");

	// join on explicit join condition
	vjoin = v1->Join(v2, "v1.i=v2.i");
	REQUIRE_NOTHROW(result = vjoin->Order("v1.i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {10, 5, 4}));

	// aggregate on EXPLICIT join
	REQUIRE_NOTHROW(result = vjoin->Aggregate("SUM(v1.i) + SUM(v2.i), SUM(v1.j) + SUM(v2.j)")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {12}));
	REQUIRE(CHECK_COLUMN(result, 1, {38}));

	// implicit join
	vjoin = v1->Join(v2, "i");
	REQUIRE_NOTHROW(result = vjoin->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {10, 5, 4}));

	// implicit join on multiple columns
	vjoin = v1->Join(v2, "i, j");
	REQUIRE_NOTHROW(result = vjoin->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));

	// aggregate on USING join
	REQUIRE_NOTHROW(result = vjoin->Aggregate("SUM(i), SUM(j)")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {19}));

	// joining on a column that doesn't exist results in an error
	REQUIRE_THROWS(v1->Join(v2, "blabla"));
	// also with explicit join condition
	REQUIRE_THROWS(v1->Join(v2, "v1.i=v2.blabla"));

	// set ops involving joins
	REQUIRE_NOTHROW(result = vjoin->Union(vjoin)->Distinct()->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));

	// do a bunch of joins in a loop
	auto v1tmp = v1;
	auto v2tmp = v2;
	for (idx_t i = 0; i < 4; i++) {
		REQUIRE_NOTHROW(v1tmp = v1tmp->Join(v2tmp->Alias(to_string(i)), "i, j"));
	}
	REQUIRE_NOTHROW(result = v1tmp->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));

	// now add on some projections and such
	auto complex_join = v1tmp->Order("i")->Limit(2)->Order("i DESC")->Limit(1)->Project("i+1, j+1");
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE_NOTHROW(result = complex_join->Execute());
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
		REQUIRE(CHECK_COLUMN(result, 1, {6}));
	}

	// create and query a view
	REQUIRE_NOTHROW(complex_join->CreateView("test123"));
	result = con.Query("SELECT * FROM test123");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));

	// joins of tables that have output modifiers attached to them (limit, order by, distinct)
	auto v1_modified = v1->Limit(100)->Order("1")->Distinct()->Filter("i<1000");
	auto v2_modified = v2->Limit(100)->Order("1")->Distinct()->Filter("i<1000");
	vjoin = v1_modified->Join(v2_modified, "i, j");
	REQUIRE_NOTHROW(result = vjoin->Order("i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM sqlite_master"));
}

TEST_CASE("Test crossproduct relation", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
	shared_ptr<Relation> values, vcross;

	REQUIRE_NOTHROW(values = con.Values({{1, 10}, {2, 5}, {3, 4}}, {"i", "j"}), "v1");
	REQUIRE_NOTHROW(values = con.Values({{1, 10}, {2, 5}, {3, 4}}, {"i", "j"}), "v2");

	auto v1 = values->Alias("v1");
	auto v2 = values->Alias("v2");

	// run cross product
	vcross = v1->CrossProduct(v2);
	REQUIRE_NOTHROW(result = vcross->Order("v1.i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 2, 2, 2, 3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 10, 10, 5, 5, 5, 4, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 1, 2, 3, 1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {10, 5, 4, 10, 5, 4, 10, 5, 4}));

	// run a positional cross product
	auto join_ref_type = JoinRefType::POSITIONAL;
	vcross = v1->CrossProduct(v2, join_ref_type);
	REQUIRE_NOTHROW(result = vcross->Order("v1.i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {10, 5, 4}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 3, {10, 5, 4}));
}

TEST_CASE("Test view creation of relations", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
	shared_ptr<Relation> tbl, filter, proj, proj2;

	// create some tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NOTHROW(con.Table("integers")->Insert({{Value::INTEGER(1)}, {Value::INTEGER(2)}, {Value::INTEGER(3)}}));

	// insertion failure because of primary key
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl_pk(i INTEGER PRIMARY KEY)"));
	REQUIRE_NOTHROW(con.Table("tbl_pk")->Insert({{Value::INTEGER(1)}, {Value::INTEGER(2)}, {Value::INTEGER(3)}}));
	REQUIRE_THROWS(con.Table("tbl_pk")->Insert({{Value::INTEGER(1)}, {Value::INTEGER(2)}, {Value::INTEGER(3)}}));

	// simple view creation
	REQUIRE_NOTHROW(tbl = con.Table("integers"));
	REQUIRE_NOTHROW(result = tbl->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	duckdb::vector<duckdb::unique_ptr<ParsedExpression>> expressions;
	expressions.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("i"));
	duckdb::vector<duckdb::string> aliases;
	aliases.push_back("j");

	REQUIRE_NOTHROW(result = tbl->Project(std::move(expressions), aliases)->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// add a projection
	REQUIRE_NOTHROW(result = tbl->Project("i + 1")->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// multiple projections
	proj = tbl->Project("i + 1", "i");
	for (idx_t i = 0; i < 10; i++) {
		proj = proj->Project("i + 1", "i");
	}
	REQUIRE_NOTHROW(result = proj->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 14}));
	REQUIRE_NOTHROW(result = proj->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 14}));

	// we can also use more complex SQL
	REQUIRE_NOTHROW(result = proj->Query("test", "SELECT SUM(t1.i) FROM test t1 JOIN test t2 ON t1.i=t2.i"));
	REQUIRE(CHECK_COLUMN(result, 0, {39}));

	// limit
	REQUIRE_NOTHROW(result = tbl->Limit(1)->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	// order
	REQUIRE_NOTHROW(result = tbl->Order("i DESC")->Limit(1)->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	// union
	auto node = tbl->Order("i DESC")->Limit(1);
	REQUIRE_NOTHROW(result = node->Union(node)->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {3, 3}));
	// distinct
	REQUIRE_NOTHROW(result = node->Union(node)->Distinct()->Query("test", "SELECT * FROM test"));
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// manually create views and query from them
	result = con.Query("SELECT i+1 FROM integers UNION SELECT i+10 FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4, 11, 12, 13}));

	REQUIRE_NOTHROW(tbl->Project("i + 1")->CreateView("test1"));
	REQUIRE_NOTHROW(tbl->Project("i + 10")->CreateView("test2"));
	result = con.Query("SELECT * FROM test1 UNION SELECT * FROM test2 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4, 11, 12, 13}));

	// project <> alias column count mismatch
	REQUIRE_THROWS(proj->Project("i + 1, i + 2", "i"));
	// view already exists
	REQUIRE_THROWS(tbl->Project("i + 10")->CreateView("test2", false));
	// table already exists
	REQUIRE_THROWS(tbl->Project("i + 10")->Create("test2"));
}

TEST_CASE("Test table creations using the relation API", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
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
	REQUIRE_NOTHROW(
	    con.Table("integers")->Filter("i BETWEEN 3 AND 4")->Project("i + 1 AS k, 'hello' AS l")->Create("new_values"));

	result = con.Query("SELECT * FROM new_values ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "hello"}));
}

TEST_CASE("Test table deletions and updates", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	auto tbl = con.Table("integers");

	// update
	tbl->Update("i=i+10", "i=2");

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3, 12}));

	// we can only have a single expression in the condition liset
	REQUIRE_THROWS(tbl->Update("i=1", "i=3,i<100"));

	tbl->Delete("i=3");

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 12}));

	// delete without condition
	tbl->Delete();

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// we cannot run update/delete on anything but base table relations
	REQUIRE_THROWS(tbl->Limit(1)->Delete());
	REQUIRE_THROWS(tbl->Limit(1)->Update("i=1"));
}

TEST_CASE("Test aggregates in relation API", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;

	// create a table
	REQUIRE_NOTHROW(con.Values("(1, 5), (2, 6), (1, 7)", {"i", "j"})->Create("integers"));

	// perform some aggregates
	auto tbl = con.Table("integers");

	// ungrouped aggregate
	REQUIRE_NOTHROW(result = tbl->Aggregate("SUM(i), SUM(j)")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {18}));

	REQUIRE_NOTHROW(result = tbl->Aggregate(duckdb::vector<string> {"SUM(i)", "SUM(j)"})->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {18}));

	// we cannot put aggregates in a Project clause
	REQUIRE_THROWS(result = tbl->Project("SUM(i), SUM(j)")->Execute());
	REQUIRE_THROWS(result = tbl->Project("i, SUM(j)")->Execute());
	// implicitly grouped aggregate
	REQUIRE_NOTHROW(result = tbl->Aggregate("i, SUM(j)")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 6}));
	REQUIRE_NOTHROW(result = tbl->Aggregate("SUM(j), i")->Order("2")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {12, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	// explicitly grouped aggregate
	REQUIRE_NOTHROW(
	    result =
	        tbl->Aggregate(duckdb::vector<string> {"SUM(j)"}, duckdb::vector<string> {"i"})->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {6, 12}));

	// grouped aggregates can be expressions
	REQUIRE_NOTHROW(result = tbl->Aggregate("i+1 AS i, SUM(j)")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 6}));
	// they can also involve multiple columns
	REQUIRE_NOTHROW(result = tbl->Aggregate("i+i AS i, SUM(j)")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 6}));
	// we cannot combine non-aggregates with aggregates
	REQUIRE_THROWS(result = tbl->Aggregate("i + SUM(j) AS i")->Order("1")->Execute());
	REQUIRE_THROWS(result = tbl->Aggregate("i, i + SUM(j)")->Order("1")->Execute());
	// group by multiple columns
	REQUIRE_NOTHROW(result = tbl->Aggregate("i, j, SUM(i + j)")->Order("1, 2")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 7, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {6, 8, 8}));
	// subqueries as groups
	REQUIRE_NOTHROW(result = tbl->Aggregate("(SELECT i), SUM(i + j)")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {14, 8}));
	// subqueries as aggregates
	REQUIRE_NOTHROW(result = tbl->Aggregate("(SELECT i), (SELECT SUM(i + j))")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {14, 8}));
	// constants without a grouping column
	REQUIRE_NOTHROW(result = tbl->Aggregate("'hello', SUM(i + j)")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	REQUIRE(CHECK_COLUMN(result, 1, {22}));
	// constants with a grouping column
	REQUIRE_NOTHROW(result = tbl->Aggregate("i, 'hello', SUM(i + j)")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"hello", "hello"}));
	REQUIRE(CHECK_COLUMN(result, 2, {14, 8}));
	// aggregate with only non-aggregate columns becomes a distinct
	REQUIRE_NOTHROW(result = tbl->Aggregate("i")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE_NOTHROW(result = tbl->Aggregate("i, j")->Order("1, 2")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 7, 6}));

	// now test aggregates with explicit groups
	REQUIRE_NOTHROW(result = tbl->Aggregate("i", "i")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE_NOTHROW(result = tbl->Aggregate("i, SUM(j)", "i")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {12, 6}));
	// explicit groups can be combined with aggregates
	REQUIRE_NOTHROW(result = tbl->Aggregate("i, i+SUM(j)", "i")->Order("1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {13, 8}));
	// when using explicit groups, we cannot have non-explicit groups
	REQUIRE_THROWS(tbl->Aggregate("j, i+SUM(j)", "i")->Order("1")->Execute());

	// Coverage: Groups expressions can not create multiple statements
	REQUIRE_THROWS(tbl->Aggregate("i", "i; select 42")->Execute());

	// project -> aggregate -> project -> aggregate
	// SUM(j) = 18 -> 18 + 1 = 19 -> 19 * 2 = 38
	result = tbl->Aggregate("SUM(j) AS k")->Project("k+1 AS l")->Aggregate("SUM(l) AS m")->Project("m*2")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {38}));

	// aggregate after output modifiers
	result = tbl->Order("i")->Limit(100)->Aggregate("SUM(j) AS k")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {18}));
}

TEST_CASE("Test interaction of relations with transactions", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con1(db), con2(db);
	duckdb::unique_ptr<QueryResult> result;

	con1.BeginTransaction();
	con2.BeginTransaction();

	// create a table in con1
	REQUIRE_NOTHROW(con1.Values("(1), (2), (3)")->Create("integers"));

	// con1 can see it, but con2 can't see it yet
	REQUIRE_NOTHROW(con1.Table("integers"));
	REQUIRE_THROWS(con2.Table("integers"));

	// we can also rollback
	con1.Rollback();

	REQUIRE_THROWS(con1.Table("integers"));
	REQUIRE_THROWS(con2.Table("integers"));

	// recreate the table, this time in auto-commit mode
	REQUIRE_NOTHROW(con1.Values("(1), (2), (3)")->Create("integers"));

	// con2 still can't see it, because it is in its own transaction
	REQUIRE_NOTHROW(con1.Table("integers"));
	REQUIRE_THROWS(con2.Table("integers"));

	// after con2 commits, both can see the table
	con2.Commit();

	REQUIRE_NOTHROW(con1.Table("integers"));
	REQUIRE_NOTHROW(con2.Table("integers"));
}

TEST_CASE("Test interaction of relations with schema changes", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	duckdb::unique_ptr<QueryResult> result;

	// create some tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// create a table scan of the integers table
	auto tbl_scan = con.Table("integers")->Project("i+1")->Order("1");
	REQUIRE_NOTHROW(result = tbl_scan->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// now drop the table
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));

	// the scan now fails, because the table is dropped!
	REQUIRE_FAIL(tbl_scan->Execute());

	// if we recreate the table, it works again
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	REQUIRE_NOTHROW(result = tbl_scan->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));

	// but what if we recreate an incompatible table?
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i VARCHAR, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ('hello', 3)"));

	// this results in a binding error!
	REQUIRE_FAIL(tbl_scan->Execute());

	// now what if we run a query that still binds successfully, but changes result?
	auto tbl = con.Table("integers");
	REQUIRE_NO_FAIL(tbl->Execute());

	// add extra columns
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i VARCHAR, j VARCHAR)"));
	REQUIRE_FAIL(tbl->Execute());

	// change type of column
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i DATE, j INTEGER)"));
	REQUIRE_FAIL(tbl->Execute());

	// different name also results in an error
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(k VARCHAR, j INTEGER)"));
	REQUIRE_FAIL(tbl->Execute());

	// but once we go back to the original table it works again!
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i VARCHAR, j INTEGER)"));
	REQUIRE_NO_FAIL(tbl->Execute());
}

TEST_CASE("Test junk SQL in expressions", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	duckdb::unique_ptr<QueryResult> result;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	auto tbl = con.Table("integers");
	REQUIRE_THROWS(tbl->Filter("1=1; DELETE FROM tables;"));
	REQUIRE_THROWS(tbl->Filter("1=1 UNION SELECT 42"));
	REQUIRE_THROWS(tbl->Order("1 DESC; DELETE FROM tables;"));
	REQUIRE_THROWS(tbl->Update("i=1; DELETE FROM TABLES"));
	REQUIRE_THROWS(con.Values("(1, 1); SELECT 42"));
	REQUIRE_THROWS(con.Values("(1, 1) UNION SELECT 42"));
}

TEST_CASE("We cannot mix statements from multiple databases", "[relation_api]") {
	DuckDB db(nullptr), db2(nullptr);
	Connection con(db), con2(db2);
	duckdb::unique_ptr<QueryResult> result;

	// create some tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));
	REQUIRE_NO_FAIL(con2.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (4)"));

	auto i1 = con.Table("integers");
	auto i2 = con2.Table("integers");

	// we cannot mix statements from different connections without a wrapper!
	REQUIRE_THROWS(i2->Union(i1));
	REQUIRE_THROWS(i2->Except(i1));
	REQUIRE_THROWS(i2->Intersect(i1));
	REQUIRE_THROWS(i2->Join(i1, "i"));
	REQUIRE_THROWS(i2->Join(i1, "i1.i=i2.i"));

	// FIXME: what about a wrapper to scan data from other databases/connections?
}

TEST_CASE("Test view relations", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT i+1 AS i, i+2 FROM integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v2(a,b) AS SELECT i+1, i+2 FROM integers"));

	auto i1 = con.View("v1");
	result = i1->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 4, 5}));

	result = con.View("v2")->Project("a+b")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {5, 7, 9}));

	// non-existant view
	REQUIRE_THROWS(con.View("blabla"));

	// combining views
	result = con.View("v1")->Join(con.View("v2"), "v1.i=v2.a")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 3, {3, 4, 5}));
}

TEST_CASE("Test table function relations", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));

	auto i1 = con.TableFunction("duckdb_tables");
	result = i1->Execute();
	REQUIRE(CHECK_COLUMN(result, 2, {"main"}));

	// function with parameters
	auto i2 = con.TableFunction("pragma_table_info", {"integers"});
	result = i2->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {"i"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER"}));

	// we can do ops on table functions
	result = i2->Filter("cid=0")->Project("concat(name, ' ', type), length(name), reverse(lower(type))")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {"i INTEGER"}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {"regetni"}));

	// table function that takes a relation as input
	auto values = con.Values("(42)", {"i"});
	auto summary = values->TableFunction("summary", duckdb::vector<Value> {});
	result = summary->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {"[42]"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"42"}));

	// non-existant table function
	REQUIRE_THROWS(con.TableFunction("blabla"));
}

TEST_CASE("Test CSV reading/writing from relations", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;

	// write a bunch of values to a CSV
	auto csv_file = TestCreatePath("relationtest.csv");

	con.Values("(1), (2), (3)", {"i"})->WriteCSV(csv_file);

	REQUIRE_THROWS(con.Values("(1), (2), (3)", {"i"})->WriteCSV("//fef//gw/g/bla/bla"));

	// now scan the CSV file
	auto csv_scan = con.ReadCSV(csv_file, {"i INTEGER"});
	result = csv_scan->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	// with auto detect
	auto auto_csv_scan = con.ReadCSV(csv_file);
	result = auto_csv_scan->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	REQUIRE_THROWS(con.ReadCSV(csv_file, {"i INTEGER); SELECT 42;--"}));
	REQUIRE_THROWS(con.ReadCSV(csv_file, {"i INTEGER, j INTEGER"}));
}

TEST_CASE("Test query relation", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
	shared_ptr<Relation> tbl;

	// create some tables
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	REQUIRE_NOTHROW(tbl = con.RelationFromQuery("SELECT * FROM integers WHERE i >= 2"));
	REQUIRE_NOTHROW(result = tbl->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3}));
	REQUIRE_NOTHROW(result = tbl->Project("i + 1")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4}));

	REQUIRE_NOTHROW(result = tbl->Alias("q1")->Join(tbl->Alias("q2")->Filter("i=3"), "q1.i=q2.i")->Execute());
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	REQUIRE_THROWS(tbl->Project("k+1"));
	// multiple queries
	REQUIRE_THROWS(con.RelationFromQuery("SELECT 42; SELECT 84"));
	// not a select statement
	REQUIRE_THROWS(con.RelationFromQuery("DELETE FROM tbl"));
}

TEST_CASE("Test TopK relation", "[relation_api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	duckdb::unique_ptr<QueryResult> result;
	shared_ptr<Relation> tbl;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (i integer,j VARCHAR, k varchar )"));
	REQUIRE_NO_FAIL(con.Query("insert into test values (10,'a','a'), (20,'a','b')"));

	REQUIRE_NOTHROW(tbl = con.Table("test"));
	REQUIRE_NOTHROW(tbl = tbl->Filter("k < 'f' and k is not null")
	                          ->Project("#3,#2,#1")
	                          ->Project("#2,#3")
	                          ->Order("(#2-10)::UTINYINT ASC")
	                          ->Limit(1));
}

TEST_CASE("Test Relation Pending Query API", "[relation_api]") {
	DuckDB db;
	Connection con(db);

	SECTION("Materialized result") {
		auto tbl = con.TableFunction("range", {Value(1000000)});
		auto aggr = tbl->Aggregate("SUM(range)");
		auto pending_query = con.context->PendingQuery(aggr, false);
		REQUIRE(!pending_query->HasError());
		auto result = pending_query->Execute();
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)}));

		// cannot fetch twice from the same pending query
		REQUIRE_THROWS(pending_query->Execute());
		REQUIRE_THROWS(pending_query->Execute());

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	SECTION("Runtime error in pending query (materialized)") {
		auto tbl = con.TableFunction("range", {Value(1000000)});
		auto aggr = tbl->Aggregate("SUM(range) AS s")->Project("concat(s::varchar, 'hello')::int");
		// this succeeds initially
		auto pending_query = con.context->PendingQuery(aggr, false);
		REQUIRE(!pending_query->HasError());
		// we only encounter the failure later on as we are executing the query
		auto result = pending_query->Execute();
		REQUIRE_FAIL(result);

		// query the connection as normal after
		result = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
}
