
#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"

using namespace std;

TEST_CASE("Test TPC-H Q1", "[tpch]") {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	// open and close a database in in-memory mode
	REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

	// TPC-H
	REQUIRE(
	    duckdb_query(
	        connection,
	        "create table lineitem ( l_orderkey INTEGER NOT NULL, l_partkey "
	        "INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, l_linenumber "
	        "INTEGER "
	        "NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice "
	        "DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax "
	        "DECIMAL(15,2) NOT NULL, l_returnflag CHAR(1) NOT NULL, "
	        "l_linestatus "
	        "CHAR(1) NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT "
	        "NULL, l_receiptdate DATE NOT NULL, l_shipinstruct CHAR(25) NOT "
	        "NULL, "
	        "l_shipmode CHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);",
	        NULL) == DuckDBSuccess);

	REQUIRE(
	    duckdb_query(
	        connection,
	        "insert into lineitem values ('1', '155190', '7706', '1', '17', "
	        "'21168.23', '0.04', '0.02', 'N', 'O', '1996-03-13', '1996-02-12', "
	        "'1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'egular courts above "
	        "the')",
	        NULL) == DuckDBSuccess);

	// TPC-H Q1
	REQUIRE(
	    duckdb_query(
	        connection,
	        "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, "
	        "sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 "
	        "- l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - "
	        "l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as "
	        "avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as "
	        "avg_disc, count(*) as count_order"
	        " from lineitem where l_shipdate <= "
	        "cast('1998-09-02' as date) group by l_returnflag, l_linestatus "
	        "order "
	        "by l_returnflag, l_linestatus;",
	        &result) == DuckDBSuccess);
	// duckdb_print_result(result);
	REQUIRE(CHECK_STRING_COLUMN(result, 0, {"N"}));
	REQUIRE(CHECK_STRING_COLUMN(result, 1, {"O"}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 2, {17.0}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 3, {21168.23}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 4, {20321.5008}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 5, {20727.9308}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 6, {17.0}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 7, {21168.23}));
	REQUIRE(CHECK_DECIMAL_COLUMN(result, 8, {0.04}));
	REQUIRE(CHECK_NUMERIC_COLUMN(result, 9, {1}));
	duckdb_destroy_result(result);
	//
	//	EXEC("CREATE TABLE part (p_partkey INT NOT NULL, p_name VARCHAR(55) NOT
	//" 		     "NULL, p_mfgr VARCHAR(25) NOT NULL, p_brand VARCHAR(10) NOT NULL, "
	//		     "p_type VARCHAR(25) NOT NULL, p_size INT NOT NULL, p_container
	//" 		     "VARCHAR(10) NOT NULL, p_retailprice DECIMAL(15,2) NOT NULL, "
	//		     "p_comment VARCHAR(23) NOT NULL) ;");
	//		EXEC("CREATE TABLE supplier (s_suppkey INT NOT NULL, s_name
	//VARCHAR(25) " 		     "NOT NULL, s_address VARCHAR(40) NOT NULL, s_nationkey INT
	//NOT NULL, " 		     "s_phone VARCHAR(15) NOT NULL, s_acctbal DECIMAL(15,2) NOT
	//NULL, " 		     "s_comment VARCHAR(101) NOT NULL);"); 		EXEC("CREATE TABLE partsupp
	//(ps_partkey INT NOT NULL, ps_suppkey INT NOT " 		     "NULL, ps_availqty INT NOT
	//NULL, ps_supplycost DECIMAL(15,2) NOT " 		     "NULL, ps_comment VARCHAR(199) NOT
	//NULL) ;"); 		EXEC("CREATE TABLE region (r_regionkey INT NOT NULL, r_name
	//VARCHAR(25) " 		     "NOT NULL, r_comment VARCHAR(152) NOT NULL) ;"); 		EXEC(
	//		    "CREATE TABLE nation (n_nationkey INT NOT NULL, n_name VARCHAR(25)
	//NOT " 		    "NULL, n_regionkey INT NOT NULL, n_comment VARCHAR(152) NOT NULL)
	//;");
	//
	//		EXEC("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address,
	//" 		     "s_phone, s_comment from part, supplier, partsupp, nation, region "
	//		     "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and
	//p_size = " 		     "15 and p_type like '%BRASS' and s_nationkey = n_nationkey and
	//" 		     "n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = "
	//		     "( select min(ps_supplycost) from partsupp, supplier, nation,
	//region " 		     "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and "
	//		     "s_nationkey = n_nationkey and n_regionkey = r_regionkey and
	//r_name = "
	//		     "'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey
	//limit " 		     "100");

	REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
	REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
