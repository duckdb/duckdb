#include "catch.hpp"
#include "test_csv_header.hpp"
#include "test_helpers.hpp"
#include "common/file_system.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

static string GetCSVPath() {
	string csv_path = JoinPath(TESTING_DIRECTORY_NAME, "csv_files");
	if (DirectoryExists(csv_path)) {
		RemoveDirectory(csv_path);
	}
	CreateDirectory(csv_path);
	return csv_path;
}

static void WriteCSV(string path, const char *csv) {
	ofstream csv_writer(path);
	csv_writer << csv;
	csv_writer.close();
}

TEST_CASE("Test copy statement", "[copy]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	auto csv_path = GetCSVPath();

	// Generate CSV file With ; as delimiter and complex strings
	ofstream from_csv_file(JoinPath(csv_path, "test.csv"));
	for (int i = 0; i < 5000; i++)
		from_csv_file << i << "," << i << ", test" << endl;
	from_csv_file.close();

	// Loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY test FROM '" + JoinPath(csv_path, "test.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	result = con.Query("SELECT COUNT(a), SUM(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	REQUIRE(CHECK_COLUMN(result, 1, {12497500}));

	result = con.Query("SELECT * FROM test ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {" test", " test", " test"}));

	//  Creating CSV from table
	result = con.Query("COPY test to '" + JoinPath(csv_path, "test2.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	// load the same CSV back again
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(a INTEGER, b INTEGER, c VARCHAR(10));"));
	result = con.Query("COPY test2 FROM '" + JoinPath(csv_path, "test2.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	result = con.Query("SELECT * FROM test2 ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 2, {" test", " test", " test"}));

	//  Creating CSV from Query
	result = con.Query("COPY (select a,b from test where a < 4000) to '" + JoinPath(csv_path, "test3.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {4000}));
	// load the same CSV back again
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test3(a INTEGER, b INTEGER);"));
	result = con.Query("COPY test3 FROM '" + JoinPath(csv_path, "test3.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {4000}));
	result = con.Query("SELECT * FROM test3 ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 2}));

	// Exporting selected columns from a table to a CSV.
	result = con.Query("COPY test(a,c) to '" + JoinPath(csv_path, "test4.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	// Importing CSV to Selected Columns
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test4 (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY test4(a,c) from '" + JoinPath(csv_path, "test4.csv") + "';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	result = con.Query("SELECT * FROM test4 ORDER BY 1 LIMIT 3 ");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {" test", " test", " test"}));

	// use a different delimiter
	auto pipe_csv = JoinPath(csv_path, "test_pipe.csv");
	ofstream from_csv_file_pipe(pipe_csv);
	for (int i = 0; i < 10; i++)
		from_csv_file_pipe << i << "|" << i << "|test" << endl;
	from_csv_file_pipe.close();

	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER,c VARCHAR(10));");
	result = con.Query("COPY test FROM '" + pipe_csv + "' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	// test null
	auto null_csv = JoinPath(csv_path, "null.csv");
	ofstream from_csv_file_null(null_csv);
	for (int i = 0; i < 1; i++)
		from_csv_file_null << i << "||test" << endl;
	from_csv_file_null.close();
	result = con.Query("COPY test FROM '" + null_csv + "' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// test invalid UTF8
	auto invalid_utf_csv = JoinPath(csv_path, "invalid_utf.csv");
	ofstream from_csv_file_utf(invalid_utf_csv);
	for (int i = 0; i < 1; i++)
		from_csv_file_utf << i << "42|42|\xe2\x82\x28" << endl;
	from_csv_file_utf.close();
	REQUIRE_FAIL(con.Query("COPY test FROM '" + invalid_utf_csv + "' DELIMITER '|';"));
}

TEST_CASE("Test copy into from on-time dataset", "[copy]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	auto csv_path = GetCSVPath();
	auto ontime_csv = JoinPath(csv_path, "ontime.csv");
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
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	auto csv_path = GetCSVPath();
	auto lineitem_csv = JoinPath(csv_path, "lineitem.csv");
	WriteCSV(lineitem_csv, lineitem_sample);

	REQUIRE_NO_FAIL(con.Query(" CREATE TABLE lineitem(l_orderkey INT NOT NULL, l_partkey INT NOT NULL, l_suppkey INT NOT NULL, l_linenumber INT NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL, l_linestatus VARCHAR(1) NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR(25) NOT NULL, l_shipmode VARCHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);"));
	result = con.Query("COPY lineitem FROM '" + lineitem_csv + "' DELIMITER '|'");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber");
	REQUIRE(CHECK_COLUMN(result, 0, {15519, 6731, 6370, 214, 2403, 1564}));
	REQUIRE(CHECK_COLUMN(result, 1, {"egular courts above the", "ly final dependencies: slyly bold ", "riously. regular, express dep", "lites. fluffily even de", " pending foxes. slyly re", "arefully slyly ex"}));
}