#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test GetTableNames", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	unordered_set<string> table_names;

	// standard
	table_names = con.GetTableNames("SELECT * FROM my_table");
	REQUIRE(table_names.size() == 1);
	REQUIRE(table_names.count("my_table"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE my_table(i INT)"));

	// fetch a specific column
	table_names = con.GetTableNames("SELECT col_a FROM my_table");
	REQUIRE(table_names.size() == 1);
	REQUIRE(table_names.count("my_table"));

	// multiple tables
	table_names = con.GetTableNames("SELECT * FROM my_table1, my_table2, my_table3");
	REQUIRE(table_names.size() == 3);
	REQUIRE(table_names.count("my_table1"));
	REQUIRE(table_names.count("my_table2"));
	REQUIRE(table_names.count("my_table3"));

	// same table is mentioned multiple times
	table_names = con.GetTableNames("SELECT col_a FROM my_table, my_table m2, my_table m3");
	REQUIRE(table_names.size() == 1);
	REQUIRE(table_names.count("my_table"));

	// cte
	table_names = con.GetTableNames("WITH cte AS (SELECT * FROM my_table) SELECT * FROM cte");
	REQUIRE(table_names.size() == 1);
	REQUIRE(table_names.count("my_table"));

	// subqueries
	table_names = con.GetTableNames("SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) bla) bla3");
	REQUIRE(table_names.size() == 1);
	REQUIRE(table_names.count("my_table"));

	// join
	table_names = con.GetTableNames("SELECT col_a FROM my_table JOIN my_table2 ON (my_table.col_b=my_table2.col_d)");
	REQUIRE(table_names.size() == 2);
	REQUIRE(table_names.count("my_table"));
	REQUIRE(table_names.count("my_table2"));

	// scalar subquery
	table_names = con.GetTableNames("SELECT (SELECT COUNT(*) FROM my_table)");
	REQUIRE(table_names.size() == 1);
	REQUIRE(table_names.count("my_table"));

	// set operations
	table_names =
	    con.GetTableNames("SELECT * FROM my_table UNION ALL SELECT * FROM my_table2 INTERSECT SELECT * FROM my_table3");
	REQUIRE(table_names.size() == 3);
	REQUIRE(table_names.count("my_table"));
	REQUIRE(table_names.count("my_table2"));
	REQUIRE(table_names.count("my_table3"));

	// window functions
	table_names = con.GetTableNames("SELECT row_number() OVER (ORDER BY (SELECT i+j FROM my_table2)) FROM my_table");
	REQUIRE(table_names.size() == 2);
	REQUIRE(table_names.count("my_table"));
	REQUIRE(table_names.count("my_table2"));

	// views are expanded
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT * FROM my_table"));

	table_names = con.GetTableNames("SELECT col_a FROM v1");
	REQUIRE(table_names.size() == 1);
	REQUIRE(table_names.count("my_table"));

	if (!db.ExtensionIsLoaded("tpch")) {
		return;
	}

	// TPCH
	// run all TPC-H queries twice
	// one WITHOUT the tables in the catalog
	// once WITH the tables in the catalog
	for (idx_t i = 0; i < 2; i++) {
		table_names = con.GetTableNames("PRAGMA tpch(1)");
		REQUIRE(table_names.size() == 1);
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(2)");
		REQUIRE(table_names.size() == 5);
		REQUIRE(table_names.count("part"));
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("partsupp"));
		REQUIRE(table_names.count("nation"));
		REQUIRE(table_names.count("region"));

		table_names = con.GetTableNames("PRAGMA tpch(3)");
		REQUIRE(table_names.size() == 3);
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(4)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(5)");
		REQUIRE(table_names.size() == 6);
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("nation"));
		REQUIRE(table_names.count("region"));

		table_names = con.GetTableNames("PRAGMA tpch(6)");
		REQUIRE(table_names.size() == 1);
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(7)");
		REQUIRE(table_names.size() == 5);
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("nation"));

		table_names = con.GetTableNames("PRAGMA tpch(8)");
		REQUIRE(table_names.size() == 7);
		REQUIRE(table_names.count("part"));
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("nation"));
		REQUIRE(table_names.count("region"));

		table_names = con.GetTableNames("PRAGMA tpch(9)");
		REQUIRE(table_names.size() == 6);
		REQUIRE(table_names.count("part"));
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("partsupp"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("nation"));

		table_names = con.GetTableNames("PRAGMA tpch(10)");
		REQUIRE(table_names.size() == 4);
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("nation"));

		table_names = con.GetTableNames("PRAGMA tpch(11)");
		REQUIRE(table_names.size() == 3);
		REQUIRE(table_names.count("partsupp"));
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("nation"));

		table_names = con.GetTableNames("PRAGMA tpch(12)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(13)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("orders"));

		table_names = con.GetTableNames("PRAGMA tpch(14)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("part"));
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(15)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(16)");
		REQUIRE(table_names.size() == 3);
		REQUIRE(table_names.count("partsupp"));
		REQUIRE(table_names.count("part"));
		REQUIRE(table_names.count("supplier"));

		table_names = con.GetTableNames("PRAGMA tpch(17)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("part"));

		table_names = con.GetTableNames("PRAGMA tpch(18)");
		REQUIRE(table_names.size() == 3);
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(19)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("part"));

		table_names = con.GetTableNames("PRAGMA tpch(20)");
		REQUIRE(table_names.size() == 5);
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("nation"));
		REQUIRE(table_names.count("partsupp"));
		REQUIRE(table_names.count("part"));
		REQUIRE(table_names.count("lineitem"));

		table_names = con.GetTableNames("PRAGMA tpch(21)");
		REQUIRE(table_names.size() == 4);
		REQUIRE(table_names.count("supplier"));
		REQUIRE(table_names.count("lineitem"));
		REQUIRE(table_names.count("orders"));
		REQUIRE(table_names.count("nation"));

		table_names = con.GetTableNames("PRAGMA tpch(22)");
		REQUIRE(table_names.size() == 2);
		REQUIRE(table_names.count("customer"));
		REQUIRE(table_names.count("orders"));

		REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=0)"));
	}
}
