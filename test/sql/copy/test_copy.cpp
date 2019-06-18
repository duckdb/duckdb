#include "catch.hpp"
#include "common/file_system.hpp"
#include "common/types/date.hpp"
#include "test_csv_header.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

static FileSystem fs;

static string GetCSVPath() {
	string csv_path = TestCreatePath("csv_files");
	if (fs.DirectoryExists(csv_path)) {
		fs.RemoveDirectory(csv_path);
	}
	fs.CreateDirectory(csv_path);
	return csv_path;
}

static void WriteCSV(string path, const char *csv) {
	ofstream csv_writer(path);
	csv_writer << csv;
	csv_writer.close();
}

TEST_CASE("Test copy statement", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// Generate CSV file With ; as delimiter and complex strings
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	for (int i = 0; i < 5000; i++) {
		from_csv_file << i << "," << i << ", test" << endl;
	}
	from_csv_file.close();

	// Loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	result = con.Query("SELECT COUNT(a), SUM(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	REQUIRE(CHECK_COLUMN(result, 1, {12497500}));

	result = con.Query("SELECT * FROM test ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {" test", " test", " test"}));

	//  Creating CSV from table
	result = con.Query("COPY test to '" + fs.JoinPath(csv_path, "test2.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	// load the same CSV back again
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(a INTEGER, b INTEGER, c VARCHAR(10));"));
	result = con.Query("COPY test2 FROM '" + fs.JoinPath(csv_path, "test2.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	result = con.Query("SELECT * FROM test2 ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {" test", " test", " test"}));

	// test too few rows
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_too_few_rows(a INTEGER, b INTEGER, c VARCHAR, d INTEGER);"));
	REQUIRE_FAIL(con.Query("COPY test_too_few_rows FROM '" + fs.JoinPath(csv_path, "test2.csv") + "';"));

	//  Creating CSV from Query
	result = con.Query("COPY (select a,b from test where a < 4000) to '" + fs.JoinPath(csv_path, "test3.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {4000}));
	// load the same CSV back again
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test3(a INTEGER, b INTEGER);"));
	result = con.Query("COPY test3 FROM '" + fs.JoinPath(csv_path, "test3.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {4000}));
	result = con.Query("SELECT * FROM test3 ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2}));

	// Exporting selected columns from a table to a CSV.
	result = con.Query("COPY test(a,c) to '" + fs.JoinPath(csv_path, "test4.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	// Importing CSV to Selected Columns
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test4 (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY test4(a,c) from '" + fs.JoinPath(csv_path, "test4.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	result = con.Query("SELECT * FROM test4 ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {" test", " test", " test"}));

	// use a different delimiter
	auto pipe_csv = fs.JoinPath(csv_path, "test_pipe.csv");
	ofstream from_csv_file_pipe(pipe_csv);
	for (int i = 0; i < 10; i++) {
		from_csv_file_pipe << i << "|" << i << "|test" << endl;
	}
	from_csv_file_pipe.close();

	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER,c VARCHAR(10));");
	result = con.Query("COPY test FROM '" + pipe_csv + "' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	// test null
	auto null_csv = fs.JoinPath(csv_path, "null.csv");
	ofstream from_csv_file_null(null_csv);
	for (int i = 0; i < 1; i++)
		from_csv_file_null << i << "||test" << endl;
	from_csv_file_null.close();
	result = con.Query("COPY test FROM '" + null_csv + "' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// test invalid UTF8
	auto invalid_utf_csv = fs.JoinPath(csv_path, "invalid_utf.csv");
	ofstream from_csv_file_utf(invalid_utf_csv);
	for (int i = 0; i < 1; i++)
		from_csv_file_utf << i << "42|42|\xe2\x82\x28" << endl;
	from_csv_file_utf.close();
	REQUIRE_FAIL(con.Query("COPY test FROM '" + invalid_utf_csv + "' DELIMITER '|';"));

	// empty file
	ofstream empty_file(fs.JoinPath(csv_path, "empty.csv"));
	empty_file.close();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE empty_table (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY empty_table FROM '" + fs.JoinPath(csv_path, "empty.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// unterminated quotes

	// empty file
	ofstream unterminated_quotes_file(fs.JoinPath(csv_path, "unterminated.csv"));
	unterminated_quotes_file << "\"hello\n\n world\n";
	unterminated_quotes_file.close();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE unterminated (a VARCHAR);"));
	REQUIRE_FAIL(con.Query("COPY unterminated FROM '" + fs.JoinPath(csv_path, "unterminated.csv") + "';"));

	// 1024 rows
	ofstream csv_vector_size(fs.JoinPath(csv_path, "vsize.csv"));
	for (int i = 0; i < 1024; i++) {
		csv_vector_size << i << "," << i << ", test" << endl;
	}
	csv_vector_size.close();

	// Loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE vsize (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY vsize FROM '" + fs.JoinPath(csv_path, "vsize.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {1024}));
}

TEST_CASE("Test copy statement with default values", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	int64_t expected_sum_a = 0;
	int64_t expected_sum_c = 0;
	for (int i = 0; i < 5000; i++) {
		from_csv_file << i << endl;

		expected_sum_a += i;
		expected_sum_c += i + 7;
	}
	from_csv_file.close();

	// Loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR DEFAULT('hello'), c INTEGER DEFAULT(3+4));"));
	result = con.Query("COPY test (a) FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	result = con.Query("COPY test (c) FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	result =
	    con.Query("SELECT COUNT(a), COUNT(b), COUNT(c), MIN(LENGTH(b)), MAX(LENGTH(b)), SUM(a), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	REQUIRE(CHECK_COLUMN(result, 1, {10000}));
	REQUIRE(CHECK_COLUMN(result, 2, {10000}));
	REQUIRE(CHECK_COLUMN(result, 3, {5}));
	REQUIRE(CHECK_COLUMN(result, 4, {5}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value::BIGINT(expected_sum_a)}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value::BIGINT(expected_sum_c)}));
}

TEST_CASE("Test copy statement with long lines", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// Generate CSV file with a very long string
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	string big_string_a(100000, 'a');
	string big_string_b(200000, 'b');
	from_csv_file << 10 << "," << big_string_a << "," << 20 << endl;
	from_csv_file << 20 << "," << big_string_b << "," << 30 << endl;
	from_csv_file.close();

	// loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT LENGTH(b) FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {100000, 200000}));

	result = con.Query("SELECT SUM(a), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {30}));
	REQUIRE(CHECK_COLUMN(result, 1, {50}));
}

TEST_CASE("Test copy statement with quotes and newlines", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// Generate CSV file with quotes and newlines in the quotes
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "\"hello\nworld\",\"5\"" << endl;
	from_csv_file << "\"what,\n brings, you here\n, today\",\"6\"" << endl;
	from_csv_file.close();

	// loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT SUM(b) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));

	result = con.Query("SELECT a FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello\nworld", "what,\n brings, you here\n, today"}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// quotes in the middle of a quoted string are ignored
	from_csv_file.open(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "\"hello\n\"w\"o\"rld\",\"5\"" << endl;
	from_csv_file << "\"what,\n brings, you here\n, today\",\"6\"" << endl;
	from_csv_file.close();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT SUM(b) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {11}));
	result = con.Query("SELECT a FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello\n\"w\"o\"rld", "what,\n brings, you here\n, today"}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// unclosed quotes results in failure
	from_csv_file.open(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "\"hello\nworld\",\"5" << endl;
	from_csv_file << "\"what,\n brings, you here\n, today\",\"6\"" << endl;
	from_csv_file.close();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR, b INTEGER);"));
	REQUIRE_FAIL(con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';"));
}

TEST_CASE("Test copy statement with many empty lines", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// Generate CSV file with a very long string
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << "1\n";
	for (index_t i = 0; i < 19999; i++) {
		from_csv_file << "\n";
	}
	from_csv_file.close();

	// loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {20000}));

	result = con.Query("SELECT SUM(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test line endings", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// Generate CSV file with different line endings
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	from_csv_file << 10 << ","
	              << "hello"
	              << "," << 20 << "\r\n";
	from_csv_file << 20 << ","
	              << "world"
	              << "," << 30 << '\n';
	from_csv_file << 30 << ","
	              << "test"
	              << "," << 30 << '\r';
	from_csv_file.close();

	// loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("SELECT LENGTH(b) FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 5, 4}));

	result = con.Query("SELECT SUM(a), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));
	REQUIRE(CHECK_COLUMN(result, 1, {80}));
}

TEST_CASE("Test Windows Newlines with a long file", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	index_t line_count = 20000;
	int64_t sum_a = 0, sum_c = 0;
	// Generate CSV file with many strings
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	for (index_t i = 0; i < line_count; i++) {
		from_csv_file << i << ","
		              << "hello"
		              << "," << i + 2 << "\r\n";
		sum_a += i;
		sum_c += i + 2;
	}
	from_csv_file.close();

	// loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(line_count)}));

	result = con.Query("SELECT SUM(a), MIN(LENGTH(b)), MAX(LENGTH(b)), SUM(LENGTH(b)), SUM(c) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum_a)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(5)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(5)}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(5 * line_count)}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::BIGINT(sum_c)}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate a csv file with one value and many empty values
	ofstream from_csv_file_empty(fs.JoinPath(csv_path, "test2.csv"));
	from_csv_file_empty << 1 << "\r\n";
	for (index_t i = 0; i < line_count - 1; i++) {
		from_csv_file_empty << "\r\n";
	}
	from_csv_file_empty.close();

	// loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	result = con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test2.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(line_count)}));

	result = con.Query("SELECT SUM(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(1)}));
}

TEST_CASE("Test lines that exceed the maximum allowed line size", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// Generate CSV file with many strings
	ofstream from_csv_file(fs.JoinPath(csv_path, "test.csv"));
	// 20 MB string
	string big_string(2048576, 'a');
	from_csv_file << 10 << "," << big_string << "," << 20 << endl;
	from_csv_file.close();

	// the load fails because the value is too big
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, c INTEGER);"));
	REQUIRE_FAIL(con.Query("COPY test FROM '" + fs.JoinPath(csv_path, "test.csv") + "';"));
}

TEST_CASE("Test copy into from on-time dataset", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto ontime_csv = fs.JoinPath(csv_path, "ontime.csv");
	WriteCSV(ontime_csv, ontime_sample);

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE ontime(year SMALLINT, quarter SMALLINT, month SMALLINT, dayofmonth SMALLINT, dayofweek SMALLINT, "
	    "flightdate DATE, uniquecarrier CHAR(7), airlineid DECIMAL(8,2), carrier CHAR(2), tailnum VARCHAR(50), "
	    "flightnum VARCHAR(10), originairportid INTEGER, originairportseqid INTEGER, origincitymarketid INTEGER, "
	    "origin CHAR(5), origincityname VARCHAR(100), originstate CHAR(2), originstatefips VARCHAR(10), "
	    "originstatename VARCHAR(100), originwac DECIMAL(8,2), destairportid INTEGER, destairportseqid INTEGER, "
	    "destcitymarketid INTEGER, dest CHAR(5), destcityname VARCHAR(100), deststate CHAR(2), deststatefips "
	    "VARCHAR(10), deststatename VARCHAR(100), destwac DECIMAL(8,2), crsdeptime DECIMAL(8,2), deptime DECIMAL(8,2), "
	    "depdelay DECIMAL(8,2), depdelayminutes DECIMAL(8,2), depdel15 DECIMAL(8,2), departuredelaygroups "
	    "DECIMAL(8,2), deptimeblk VARCHAR(20), taxiout DECIMAL(8,2), wheelsoff DECIMAL(8,2), wheelson DECIMAL(8,2), "
	    "taxiin DECIMAL(8,2), crsarrtime DECIMAL(8,2), arrtime DECIMAL(8,2), arrdelay DECIMAL(8,2), arrdelayminutes "
	    "DECIMAL(8,2), arrdel15 DECIMAL(8,2), arrivaldelaygroups DECIMAL(8,2), arrtimeblk VARCHAR(20), cancelled "
	    "SMALLINT, cancellationcode CHAR(1), diverted SMALLINT, crselapsedtime DECIMAL(8,2), actualelapsedtime "
	    "DECIMAL(8,2), airtime DECIMAL(8,2), flights DECIMAL(8,2), distance DECIMAL(8,2), distancegroup SMALLINT, "
	    "carrierdelay DECIMAL(8,2), weatherdelay DECIMAL(8,2), nasdelay DECIMAL(8,2), securitydelay DECIMAL(8,2), "
	    "lateaircraftdelay DECIMAL(8,2), firstdeptime VARCHAR(10), totaladdgtime VARCHAR(10), longestaddgtime "
	    "VARCHAR(10), divairportlandings VARCHAR(10), divreacheddest VARCHAR(10), divactualelapsedtime VARCHAR(10), "
	    "divarrdelay VARCHAR(10), divdistance VARCHAR(10), div1airport VARCHAR(10), div1aiportid INTEGER, "
	    "div1airportseqid INTEGER, div1wheelson VARCHAR(10), div1totalgtime VARCHAR(10), div1longestgtime VARCHAR(10), "
	    "div1wheelsoff VARCHAR(10), div1tailnum VARCHAR(10), div2airport VARCHAR(10), div2airportid INTEGER, "
	    "div2airportseqid INTEGER, div2wheelson VARCHAR(10), div2totalgtime VARCHAR(10), div2longestgtime VARCHAR(10), "
	    "div2wheelsoff VARCHAR(10), div2tailnum VARCHAR(10), div3airport VARCHAR(10), div3airportid INTEGER, "
	    "div3airportseqid INTEGER, div3wheelson VARCHAR(10), div3totalgtime VARCHAR(10), div3longestgtime VARCHAR(10), "
	    "div3wheelsoff VARCHAR(10), div3tailnum VARCHAR(10), div4airport VARCHAR(10), div4airportid INTEGER, "
	    "div4airportseqid INTEGER, div4wheelson VARCHAR(10), div4totalgtime VARCHAR(10), div4longestgtime VARCHAR(10), "
	    "div4wheelsoff VARCHAR(10), div4tailnum VARCHAR(10), div5airport VARCHAR(10), div5airportid INTEGER, "
	    "div5airportseqid INTEGER, div5wheelson VARCHAR(10), div5totalgtime VARCHAR(10), div5longestgtime VARCHAR(10), "
	    "div5wheelsoff VARCHAR(10), div5tailnum VARCHAR(10));"));

	result = con.Query("COPY ontime FROM '" + ontime_csv + "' DELIMITER ',' HEADER");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	result = con.Query("SELECT year, uniquecarrier, origin, origincityname FROM ontime");
	REQUIRE(CHECK_COLUMN(result, 0, {1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {"New York, NY", "New York, NY", "New York, NY", "New York, NY", "New York, NY",
	                      "New York, NY", "New York, NY", "New York, NY", "New York, NY"}));
}

TEST_CASE("Test copy from lineitem csv", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto lineitem_csv = fs.JoinPath(csv_path, "lineitem.csv");
	WriteCSV(lineitem_csv, lineitem_sample);

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE lineitem(l_orderkey INT NOT NULL, l_partkey INT NOT NULL, l_suppkey INT NOT NULL, l_linenumber "
	    "INT NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) "
	    "NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL, l_linestatus VARCHAR(1) NOT NULL, "
	    "l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR(25) "
	    "NOT NULL, l_shipmode VARCHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);"));
	result = con.Query("COPY lineitem FROM '" + lineitem_csv + "' DELIMITER '|'");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber");
	REQUIRE(CHECK_COLUMN(result, 0, {15519, 6731, 6370, 214, 2403, 1564}));
	REQUIRE(
	    CHECK_COLUMN(result, 1,
	                 {"egular courts above the", "ly final dependencies: slyly bold ", "riously. regular, express dep",
	                  "lites. fluffily even de", " pending foxes. slyly re", "arefully slyly ex"}));

	// test COPY TO with HEADER
	result = con.Query("COPY lineitem TO '" + lineitem_csv + "' DELIMITER ' ' HEADER");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	// clear out the table
	REQUIRE_NO_FAIL(con.Query("DELETE FROM lineitem"));
	result = con.Query("SELECT * FROM lineitem");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	// now copy back into the table
	result = con.Query("COPY lineitem FROM '" + lineitem_csv + "' DELIMITER ' ' HEADER");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber");
	REQUIRE(CHECK_COLUMN(result, 0, {15519, 6731, 6370, 214, 2403, 1564}));
	REQUIRE(
	    CHECK_COLUMN(result, 1,
	                 {"egular courts above the", "ly final dependencies: slyly bold ", "riously. regular, express dep",
	                  "lites. fluffily even de", " pending foxes. slyly re", "arefully slyly ex"}));
}

TEST_CASE("Test copy from web_page csv", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto webpage_csv = fs.JoinPath(csv_path, "web_page.csv");
	WriteCSV(webpage_csv, web_page);

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE web_page(wp_web_page_sk integer not null, wp_web_page_id char(16) not null, wp_rec_start_date "
	    "date, wp_rec_end_date date, wp_creation_date_sk integer, wp_access_date_sk integer, wp_autogen_flag char(1), "
	    "wp_customer_sk integer, wp_url varchar(100), wp_type char(50), wp_char_count integer, wp_link_count integer, "
	    "wp_image_count integer, wp_max_ad_count integer, primary key (wp_web_page_sk))"));

	result = con.Query("COPY web_page FROM '" + webpage_csv + "' DELIMITER '|'");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));

	result = con.Query("SELECT * FROM web_page ORDER BY wp_web_page_sk LIMIT 3");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AAAAAAAABAAAAAAA", "AAAAAAAACAAAAAAA", "AAAAAAAACAAAAAAA"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::DATE(1997, 9, 3), Value::DATE(1997, 9, 3), Value::DATE(2000, 9, 3)}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value::DATE(2000, 9, 2), Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {2450810, 2450814, 2450814}));
	REQUIRE(CHECK_COLUMN(result, 5, {2452620, 2452580, 2452611}));
	REQUIRE(CHECK_COLUMN(result, 6, {"Y", "N", "N"}));
	REQUIRE(CHECK_COLUMN(result, 7, {98539, Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 8, {"http://www.foo.com", "http://www.foo.com", "http://www.foo.com"}));
	REQUIRE(CHECK_COLUMN(result, 9, {"welcome", "protected", "feedback"}));
	REQUIRE(CHECK_COLUMN(result, 10, {2531, 1564, 1564}));
	REQUIRE(CHECK_COLUMN(result, 11, {8, 4, 4}));
	REQUIRE(CHECK_COLUMN(result, 12, {3, 3, 3}));
	REQUIRE(CHECK_COLUMN(result, 13, {4, 1, 4}));
}

TEST_CASE("Test date copy", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE date_test(d date)"));

	auto csv_path = GetCSVPath();
	auto date_csv = fs.JoinPath(csv_path, "date.csv");
	WriteCSV(date_csv, "2019-06-05\n");

	result = con.Query("COPY date_test FROM '" + date_csv + "'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT cast(d as string) FROM date_test");
	REQUIRE(CHECK_COLUMN(result, 0, {"2019-06-05"}));
}



TEST_CASE("Test cranlogs broken gzip copy", "[copy]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE cranlogs (date date,time string,size int,r_version string,r_arch string,r_os string,package string,version string,country string,ip_id int)"));


	result = con.Query("COPY cranlogs FROM 'test/sql/copy/tmp2013-06-15.csv.gz' DELIMITER ',' HEADER");
	REQUIRE(CHECK_COLUMN(result, 0, {37459}));

}
