
#include <stdio.h>
#include <stdlib.h>

#include "duckdb.h"

void execute(duckdb_connection connection, const char *query) {
	duckdb_result result;

	printf("%s\n", query);
	if (duckdb_query(connection, query, &result) != DuckDBSuccess) {
		printf("Failure!\n");
		exit(1);
	}

	duckdb_print_result(result);
	duckdb_destroy_result(result);
}

#define EXEC(query) execute(connection, query)

int main() {
	duckdb_database database;
	duckdb_connection connection;

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

	EXEC("SELECT CAST (100 AS TINYINT) + CAST(100 AS TINYINT);");

	EXEC("SELECT 4/0;");

	EXEC("CREATE TABLE a (i integer, j integer);");
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
	EXEC("SELECT b, SUM(a), COUNT(*), SUM(a+2) FROM test GROUP BY b;");
	EXEC("SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f;");

	EXEC("SELECT a, b FROM test ORDER BY a;");
	EXEC("SELECT a, b FROM test ORDER BY a DESC;");
	EXEC("SELECT a, b FROM test ORDER BY b;");
	EXEC("SELECT a, b FROM test ORDER BY b DESC;");
	EXEC("SELECT a, b FROM test ORDER BY b, a;");
	EXEC("SELECT a, b FROM test ORDER BY b, a DESC;");

	EXEC("SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1;");
	EXEC("SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1 OFFSET 1;");

	EXEC("SELECT cast(a as BIGINT) FROM test;");
	EXEC("SELECT cast(3 as INTEGER) + cast(a as BIGINT) FROM test;");
	EXEC("SELECT cast(10 as INTEGER) / cast(a as DECIMAL) FROM test;");

	// FIXME: cast a to DECIMAL in AVG HT
	EXEC("SELECT b, AVG(a) FROM test GROUP BY b;");

	// TPC-H
	EXEC("create table lineitem ( l_orderkey INTEGER NOT NULL, l_partkey "
	     "INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, l_linenumber INTEGER "
	     "NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice "
	     "DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax "
	     "DECIMAL(15,2) NOT NULL, l_returnflag CHAR(1) NOT NULL, l_linestatus "
	     "CHAR(1) NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT "
	     "NULL, l_receiptdate DATE NOT NULL, l_shipinstruct CHAR(25) NOT NULL, "
	     "l_shipmode CHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);");
	EXEC("insert into lineitem values ('1', '155190', '7706', '1', '17', "
	     "'21168.23', '0.04', '0.02', 'N', 'O', '1996-03-13', '1996-02-12', "
	     "'1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'egular courts above "
	     "the')");

	// TPC-H Q1
	EXEC("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, "
	     "sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - "
	     "l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - "
	     "l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as "
	     "avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as "
	     "avg_disc, count(*) as count_order from lineitem where l_shipdate <= "
	     "cast('1998-09-02' as date) group by l_returnflag, l_linestatus order "
	     "by l_returnflag, l_linestatus;");

	EXEC("SELECT a, b FROM test WHERE a < 13 ORDER BY b;");
	EXEC("SELECT a, b FROM test WHERE a < 13 ORDER BY b DESC;");

	// TPC-H Query 2
	// EXEC("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address,
	// s_phone, s_comment from part, supplier, partsupp, nation, region where
	// p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and
	// p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey =
	// r_regionkey and r_name = 'EUROPE' and ps_supplycost = ( select
	// min(ps_supplycost) from partsupp, supplier, nation, region where
	// p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey =
	// n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' ) order
	// by s_acctbal desc, n_name, s_name, p_partkey limit 100;", &result) !=
	// DuckDBSuccess) { 	return 1;
	// }

	// TPC-H Query 3
	// EXEC("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as
	// revenue, o_orderdate, o_shippriority from customer, orders, lineitem
	// where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey
	// = o_orderkey and o_orderdate < date '1995-03-15'and l_shipdate > date
	// '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by
	// revenue desc, o_orderdate limit 10;", &result) != DuckDBSuccess) {
	// return 1;
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
