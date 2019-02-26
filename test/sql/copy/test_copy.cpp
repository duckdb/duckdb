#include "catch.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test copy statement", "[copystatement]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// Generate CSV file With ; as delimiter and complex strings
	ofstream from_csv_file("test.csv");
	for (int i = 0; i < 5000; i++)
		from_csv_file << i << "," << i << ", test" << endl;
	from_csv_file.close();

	// Loading CSV into a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY test FROM 'test.csv';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	result = con.Query("SELECT COUNT(a), SUM(a) FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));
	REQUIRE(CHECK_COLUMN(result, 1, {12497500}));

	//  Creating CSV from table
	result = con.Query("COPY test to 'test2.csv';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	//  Creating CSV from Query
	result = con.Query("COPY (select a,b from test where a < 4000) to 'test3.csv';");
	REQUIRE(CHECK_COLUMN(result, 0, {4000}));

	// Exporting selected columns from a table to a CSV.
	result = con.Query("COPY test(a,c) to 'test4.csv';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	// Importing CSV to Selected Columns
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, b INTEGER,c VARCHAR(10));"));
	result = con.Query("COPY test2(a,c) from 'test4.csv';");
	REQUIRE(CHECK_COLUMN(result, 0, {5000}));

	// use a different delimiter
	ofstream from_csv_file_pipe("test_pipe.csv");
	for (int i = 0; i < 10; i++)
		from_csv_file_pipe << i << "|" << i << "|test" << endl;
	from_csv_file_pipe.close();

	result = con.Query("CREATE TABLE test (a INTEGER, b INTEGER,c VARCHAR(10));");
	result = con.Query("COPY test FROM 'test_pipe.csv' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	// test null
	ofstream from_csv_file_null("null.csv");
	for (int i = 0; i < 1; i++)
		from_csv_file_null << i << "||test" << endl;
	from_csv_file_null.close();
	result = con.Query("COPY test FROM 'null.csv' DELIMITER '|';");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// test invalid UTF8
	ofstream from_csv_file_utf("invalid_utf.csv");
	for (int i = 0; i < 1; i++)
		from_csv_file_utf << i << "42|42|\xe2\x82\x28" << endl;
	from_csv_file_utf.close();
	REQUIRE_FAIL(con.Query("COPY test FROM 'invalid_utf.csv' DELIMITER '|';"));

	remove("test.csv");
	remove("test2.csv");
	remove("test3.csv");
	remove("test4.csv");
	remove("test_pipe.csv");
	remove("null.csv");
	remove("invalid_utf.csv");
}

TEST_CASE("Test copy into from on-time dataset", "[copystatement]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	string fname =
	    StringUtil::Replace(StringUtil::Replace(string(__FILE__), "test_copy.cpp", "ontime-sample.csv"), "'", "\\'");

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

	result = con.Query("COPY ontime FROM '" + fname + "' DELIMITER ',' HEADER");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	result = con.Query("SELECT year, uniquecarrier, origin, origincityname FROM ontime");
	REQUIRE(CHECK_COLUMN(result, 0, {1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {"New York, NY", "New York, NY", "New York, NY", "New York, NY", "New York, NY",
	                      "New York, NY", "New York, NY", "New York, NY", "New York, NY"}));
}
