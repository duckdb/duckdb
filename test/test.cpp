 
#include <stdlib.h>
#include <stdio.h>

#include "duckdb.h"

#define EXEC(query) if (duckdb_query(connection, query, &result) != DuckDBSuccess) { return 1; }

int main() {
	duckdb_database database;
	duckdb_connection connection;
	duckdb_result result;

	if (duckdb_open(NULL, &database) != DuckDBSuccess) {
		fprintf(stderr, "Database startup failed!\n");
		return 1;
	}

	if (duckdb_connect(database, &connection) != DuckDBSuccess) {
		fprintf(stderr, "Database connection failed!\n");
		return 1;
	}

	EXEC("SELECT 42;");
	EXEC("SELECT 42 + 1;");
	EXEC("SELECT 2 * (42 + 1), 33;");

	EXEC("CREATE TABLE a (i integer, j integer);")
	EXEC("INSERT INTO a VALUES (42, 84)");
	EXEC("SELECT * FROM a");

	EXEC("CREATE TABLE test (a INTEGER, b INTEGER)");
	EXEC("INSERT INTO test VALUES (11, 22)");
	EXEC("INSERT INTO test VALUES (12, 21)");
	EXEC("INSERT INTO test VALUES (13, 22)");
	EXEC("SELECT a,b FROM test;");
	EXEC("SELECT a + 2, b FROM test WHERE a = 11;");
	EXEC("SELECT a + 2, b FROM test WHERE a = 12;");

	EXEC("SELECT SUM(41), COUNT(*);");
	EXEC("SELECT SUM(a), COUNT(*) FROM test;");
	EXEC("SELECT SUM(a), COUNT(*) FROM test WHERE a = 11;");
	EXEC("SELECT SUM(a), SUM(b), SUM(a) + SUM (b) FROM test;");
	EXEC("SELECT SUM(a+2), SUM(a) + 2 * COUNT(*) FROM test;");
	EXEC("SELECT SUM(a), SUM(a+2) FROM test GROUP BY b;");
	EXEC("SELECT SUM(a), COUNT(*), SUM(a+2) FROM test GROUP BY b;");

	
	// EXEC("SELECT l_orderkey, l_orderkey + 1 FROM lineitem;", &result) != DuckDBSuccess) {
	// 	return 1;
	// }

	// // TPC-H Query 1
	// EXEC("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= '1998-09-02' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;", &result) != DuckDBSuccess) {
	// 	return 1;
	// }

	// TPC-H Query 2
	// EXEC("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100;", &result) != DuckDBSuccess) {
	// 	return 1;
	// }

	// TPC-H Query 3
	// EXEC("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15'and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;", &result) != DuckDBSuccess) {
	// 	return 1;
	// }

	if (duckdb_disconnect(connection) != DuckDBSuccess) {
		fprintf(stderr, "Database exit failed!\n");
		return 1;
	}
	if (duckdb_close(database) != DuckDBSuccess) {
		fprintf(stderr, "Database exit failed!\n");
		return 1;
	}
	return 0;
}
