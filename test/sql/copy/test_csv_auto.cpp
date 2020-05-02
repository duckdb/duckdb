#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/date.hpp"
#include "test_csv_header.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

#if STANDARD_VECTOR_SIZE >= 16

TEST_CASE("Test copy into auto from lineitem csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto lineitem_csv = fs.JoinPath(csv_path, "lineitem.csv");
	WriteBinary(lineitem_csv, lineitem_sample, sizeof(lineitem_sample));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE lineitem(l_orderkey INT NOT NULL, l_partkey INT NOT NULL, l_suppkey INT NOT NULL, l_linenumber "
	    "INT NOT NULL, l_quantity INTEGER NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) "
	    "NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL, l_linestatus VARCHAR(1) NOT NULL, "
	    "l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR(25) "
	    "NOT NULL, l_shipmode VARCHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);"));
	result = con.Query("COPY lineitem FROM '" + lineitem_csv + "' (FORMAT CSV_AUTO);");

	result = con.Query("SELECT COUNT(*) FROM lineitem;");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT l_partkey, l_comment FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber;");
	REQUIRE(CHECK_COLUMN(result, 0, {15519, 6731, 6370, 214, 2403, 1564}));
	REQUIRE(
	    CHECK_COLUMN(result, 1,
	                 {"egular courts above the", "ly final dependencies: slyly bold ", "riously. regular, express dep",
	                  "lites. fluffily even de", " pending foxes. slyly re", "arefully slyly ex"}));
}

TEST_CASE("Test read_csv_auto from on-time dataset", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto ontime_csv = fs.JoinPath(csv_path, "ontime.csv");
	WriteBinary(ontime_csv, ontime_sample, sizeof(ontime_sample));

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
	    "DECIMAL(8,2), cancellationcode CHAR(1), diverted DECIMAL(8,2), crselapsedtime DECIMAL(8,2), actualelapsedtime "
	    "DECIMAL(8,2), airtime DECIMAL(8,2), flights DECIMAL(8,2), distance DECIMAL(8,2), distancegroup DECIMAL(8,2), "
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

	result = con.Query("COPY ontime FROM '" + ontime_csv + "' (FORMAT CSV_AUTO);");
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	result = con.Query("SELECT year, uniquecarrier, origin, origincityname, div5longestgtime FROM ontime;");
	REQUIRE(CHECK_COLUMN(result, 0, {1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988, 1988}));
	REQUIRE(CHECK_COLUMN(result, 1, {"AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA", "AA"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK", "JFK"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {"New York, NY", "New York, NY", "New York, NY", "New York, NY", "New York, NY",
	                      "New York, NY", "New York, NY", "New York, NY", "New York, NY"}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value()}));
}

TEST_CASE("Test read_csv_auto from web_page csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto webpage_csv = fs.JoinPath(csv_path, "web_page.csv");
	WriteBinary(webpage_csv, web_page, sizeof(web_page));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE web_page AS SELECT * FROM read_csv_auto ('" + webpage_csv + "');"));

	result = con.Query("SELECT COUNT(*) FROM web_page;");
	REQUIRE(CHECK_COLUMN(result, 0, {60}));

	result = con.Query("SELECT * FROM web_page ORDER BY column00 LIMIT 3;");
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

TEST_CASE("Test read_csv_auto from greek-utf8 csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto csv_file = fs.JoinPath(csv_path, "greek_utf8.csv");
	WriteBinary(csv_file, greek_utf8, sizeof(greek_utf8));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE greek_utf8 AS SELECT * FROM read_csv_auto ('" + csv_file + "');"));

	result = con.Query("SELECT COUNT(*) FROM greek_utf8;");
	REQUIRE(CHECK_COLUMN(result, 0, {8}));

	result = con.Query("SELECT * FROM greek_utf8 ORDER BY 1;");
	REQUIRE(CHECK_COLUMN(result, 0, {1689, 1690, 41561, 45804, 51981, 171067, 182773, 607808}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"\x30\x30\x69\\047\x6d", "\x30\x30\x69\\047\x76", "\x32\x30\x31\x35\xe2\x80\x8e",
	                      "\x32\x31\xcf\x80", "\x32\x34\x68\x6f\x75\x72\x73\xe2\x80\xac",
	                      "\x61\x72\x64\x65\xcc\x80\x63\x68", "\x61\xef\xac\x81",
	                      "\x70\x6f\x76\x65\x72\x74\x79\xe2\x80\xaa"}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 2, 1, 1, 1, 2, 1, 1}));
}

TEST_CASE("Test read_csv_auto from ncvoter csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto ncvoter_csv = fs.JoinPath(csv_path, "ncvoter.csv");
	WriteBinary(ncvoter_csv, ncvoter, sizeof(ncvoter));

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE IF NOT EXISTS ncvoters(county_id INTEGER, county_desc STRING, voter_reg_num STRING,status_cd "
	    "STRING, voter_status_desc STRING, reason_cd STRING, voter_status_reason_desc STRING, absent_ind STRING, "
	    "name_prefx_cd STRING,last_name STRING, first_name STRING, midl_name STRING, name_sufx_cd STRING, "
	    "full_name_rep STRING,full_name_mail STRING, house_num STRING, half_code STRING, street_dir STRING, "
	    "street_name STRING, street_type_cd STRING, street_sufx_cd STRING, unit_designator STRING, unit_num STRING, "
	    "res_city_desc STRING,state_cd STRING, zip_code STRING, res_street_address STRING, res_city_state_zip STRING, "
	    "mail_addr1 STRING, mail_addr2 STRING, mail_addr3 STRING, mail_addr4 STRING, mail_city STRING, mail_state "
	    "STRING, mail_zipcode STRING, mail_city_state_zip STRING, area_cd STRING, phone_num STRING, full_phone_number "
	    "STRING, drivers_lic STRING, race_code STRING, race_desc STRING, ethnic_code STRING, ethnic_desc STRING, "
	    "party_cd STRING, party_desc STRING, sex_code STRING, sex STRING, birth_age STRING, birth_place STRING, "
	    "registr_dt STRING, precinct_abbrv STRING, precinct_desc STRING,municipality_abbrv STRING, municipality_desc "
	    "STRING, ward_abbrv STRING, ward_desc STRING, cong_dist_abbrv STRING, cong_dist_desc STRING, super_court_abbrv "
	    "STRING, super_court_desc STRING, judic_dist_abbrv STRING, judic_dist_desc STRING, nc_senate_abbrv STRING, "
	    "nc_senate_desc STRING, nc_house_abbrv STRING, nc_house_desc STRING,county_commiss_abbrv STRING, "
	    "county_commiss_desc STRING, township_abbrv STRING, township_desc STRING,school_dist_abbrv STRING, "
	    "school_dist_desc STRING, fire_dist_abbrv STRING, fire_dist_desc STRING, water_dist_abbrv STRING, "
	    "water_dist_desc STRING, sewer_dist_abbrv STRING, sewer_dist_desc STRING, sanit_dist_abbrv STRING, "
	    "sanit_dist_desc STRING, rescue_dist_abbrv STRING, rescue_dist_desc STRING, munic_dist_abbrv STRING, "
	    "munic_dist_desc STRING, dist_1_abbrv STRING, dist_1_desc STRING, dist_2_abbrv STRING, dist_2_desc STRING, "
	    "confidential_ind STRING, age STRING, ncid STRING, vtd_abbrv STRING, vtd_desc STRING);"));
	result = con.Query("COPY ncvoters FROM '" + ncvoter_csv + "' (FORMAT CSV_AUTO);");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));

	result = con.Query("SELECT county_id, county_desc, vtd_desc, name_prefx_cd FROM ncvoters;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {"ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE", "ALAMANCE",
	                      "ALAMANCE", "ALAMANCE", "ALAMANCE"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"09S", "09S", "03W", "09S", "1210", "035", "124", "06E", "035", "064"}));
	REQUIRE(CHECK_COLUMN(result, 3,
	                     {Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value(), Value()}));
}

TEST_CASE("Test read_csv_auto from imdb csv", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto imdb_movie_info = fs.JoinPath(csv_path, "imdb_movie_info.csv");
	WriteBinary(imdb_movie_info, imdb_movie_info_escaped, sizeof(imdb_movie_info_escaped));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE movie_info AS SELECT * FROM read_csv_auto ('" + imdb_movie_info + "');"));

	result = con.Query("SELECT COUNT(*) FROM movie_info;");
	REQUIRE(CHECK_COLUMN(result, 0, {201}));
}

TEST_CASE("Test read_csv_auto from cranlogs broken gzip", "[copy][.]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();
	auto cranlogs_csv = fs.JoinPath(csv_path, "cranlogs.csv.gz");
	WriteBinary(cranlogs_csv, tmp2013_06_15, sizeof(tmp2013_06_15));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE cranlogs AS SELECT * FROM read_csv_auto ('" + cranlogs_csv + "');"));

	result = con.Query("SELECT COUNT(*) FROM cranlogs;");
	REQUIRE(CHECK_COLUMN(result, 0, {37459}));
}

TEST_CASE("Test csv dialect detection", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// generate CSV file with RFC-conform dialect
	ofstream csv_file(fs.JoinPath(csv_path, "test.csv"));
	csv_file << "123,TEST1,one space" << endl;
	csv_file << "345,TEST1,trailing_space " << endl;
	csv_file << "567,TEST1,no_space" << endl;
	csv_file.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST1", "TEST1", "TEST1"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"one space", "trailing_space ", "no_space"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with RFC-conform dialect quote
	ofstream csv_file2(fs.JoinPath(csv_path, "test.csv"));
	csv_file2 << "123,TEST2,\"one space\"" << endl;
	csv_file2 << "345,TEST2,\"trailing_space, \"" << endl;
	csv_file2 << "567,TEST2,\"no\"\"space\"" << endl;
	csv_file2.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST2", "TEST2", "TEST2"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"one space", "trailing_space, ", "no\"space"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with RFC-conform dialect quote/leading space of numerics
	ofstream csv_file3(fs.JoinPath(csv_path, "test.csv"));
	csv_file3 << "123,TEST3,text1" << endl;
	csv_file3 << "\"345\",TEST3,text2" << endl;
	csv_file3 << " 567,TEST3,text3" << endl;
	csv_file3.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST3", "TEST3", "TEST3"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with bar delimiter
	ofstream csv_file4(fs.JoinPath(csv_path, "test.csv"));
	csv_file4 << "123|TEST4|text1" << endl;
	csv_file4 << "345|TEST4|text2" << endl;
	csv_file4 << "567|TEST4|text3" << endl;
	csv_file4.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST4", "TEST4", "TEST4"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with bar delimiter and double quotes
	ofstream csv_file5(fs.JoinPath(csv_path, "test.csv"));
	csv_file5 << "123|TEST5|text1" << endl;
	csv_file5 << "345|TEST5|\"text2|\"" << endl;
	csv_file5 << "567|TEST5|text3" << endl;
	csv_file5.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST5", "TEST5", "TEST5"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2|", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with bar delimiter and double quotes and double escape
	ofstream csv_file6(fs.JoinPath(csv_path, "test.csv"));
	csv_file6 << "123|TEST6|text1" << endl;
	csv_file6 << "345|TEST6|\"text\"\"2\"\"text\"" << endl;
	csv_file6 << "\"567\"|TEST6|text3" << endl;
	csv_file6.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST6", "TEST6", "TEST6"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text\"2\"text", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with bar delimiter and double quotes and backslash escape
	ofstream csv_file7(fs.JoinPath(csv_path, "test.csv"));
	csv_file7 << "123|TEST7|text1" << endl;
	csv_file7 << "345|TEST7|\"text\\\"2\\\"\"" << endl;
	csv_file7 << "\"567\"|TEST7|text3" << endl;
	csv_file7.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST7", "TEST7", "TEST7"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text\"2\"", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with bar delimiter and single quotes and backslash escape
	ofstream csv_file8(fs.JoinPath(csv_path, "test.csv"));
	csv_file8 << "123|TEST8|text1" << endl;
	csv_file8 << "345|TEST8|'text\\'2\\'text'" << endl;
	csv_file8 << "'567'|TEST8|text3" << endl;
	csv_file8.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST8", "TEST8", "TEST8"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text'2'text", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with semicolon delimiter
	ofstream csv_file9(fs.JoinPath(csv_path, "test.csv"));
	csv_file9 << "123;TEST9;text1" << endl;
	csv_file9 << "345;TEST9;text2" << endl;
	csv_file9 << "567;TEST9;text3" << endl;
	csv_file9.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST9", "TEST9", "TEST9"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with semicolon delimiter and double quotes
	ofstream csv_file10(fs.JoinPath(csv_path, "test.csv"));
	csv_file10 << "123;TEST10;text1" << endl;
	csv_file10 << "\"345\";TEST10;text2" << endl;
	csv_file10 << "567;TEST10;\"te;xt3\"" << endl;
	csv_file10.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST10", "TEST10", "TEST10"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2", "te;xt3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with semicolon delimiter, double quotes and RFC escape
	ofstream csv_file11(fs.JoinPath(csv_path, "test.csv"));
	csv_file11 << "123;TEST11;text1" << endl;
	csv_file11 << "\"345\";TEST11;text2" << endl;
	csv_file11 << "567;TEST11;\"te\"\"xt3\"" << endl;
	csv_file11.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST11", "TEST11", "TEST11"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2", "te\"xt3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with tab delimiter
	ofstream csv_file12(fs.JoinPath(csv_path, "test.csv"));
	csv_file12 << "123\tTEST12\ttext1" << endl;
	csv_file12 << "345\tTEST12\ttext2" << endl;
	csv_file12 << "567\tTEST12\ttext3" << endl;
	csv_file12.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST12", "TEST12", "TEST12"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with tab delimiter and single quotes
	ofstream csv_file13(fs.JoinPath(csv_path, "test.csv"));
	csv_file13 << "123\tTEST13\ttext1" << endl;
	csv_file13 << "345\tTEST13\t'te\txt2'" << endl;
	csv_file13 << "'567'\tTEST13\ttext3" << endl;
	csv_file13.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST13", "TEST13", "TEST13"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "te\txt2", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with tab delimiter and single quotes without type-hint
	ofstream csv_file14(fs.JoinPath(csv_path, "test.csv"));
	csv_file14 << "123\tTEST14\ttext1" << endl;
	csv_file14 << "345\tTEST14\t'te\txt2'" << endl;
	csv_file14 << "567\tTEST14\ttext3" << endl;
	csv_file14.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345, 567}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST14", "TEST14", "TEST14"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "te\txt2", "text3"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));
}

TEST_CASE("Test csv header detection", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// generate CSV file with two lines, none header
	ofstream csv_file(fs.JoinPath(csv_path, "test.csv"));
	csv_file << "123.0,TEST1,2000-12-12" << endl;
	csv_file << "345.0,TEST1,2000-12-13" << endl;
	csv_file.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT column0, column1, column2 FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123.0, 345.0}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST1", "TEST1"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::DATE(2000, 12, 12), Value::DATE(2000, 12, 13)}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with two lines, one header
	ofstream csv_file2(fs.JoinPath(csv_path, "test.csv"));
	csv_file2 << "number,text,date" << endl;
	csv_file2 << "345.0,TEST2,2000-12-13" << endl;
	csv_file2.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT number, text, date FROM test ORDER BY number;");
	REQUIRE(CHECK_COLUMN(result, 0, {345.0}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST2"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::DATE(2000, 12, 13)}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with three lines, one header, one skip row
	ofstream csv_file3(fs.JoinPath(csv_path, "test.csv"));
	csv_file3 << "some notes..." << endl;
	csv_file3 << "number,text,date" << endl;
	csv_file3 << "345.0,TEST3,2000-12-13" << endl;
	csv_file3.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT number, text, date FROM test ORDER BY number;");
	REQUIRE(CHECK_COLUMN(result, 0, {345.0}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST3"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::DATE(2000, 12, 13)}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with three lines, one header, two skip rows
	ofstream csv_file4(fs.JoinPath(csv_path, "test.csv"));
	csv_file4 << "some notes..." << endl;
	csv_file4 << "more notes,..." << endl;
	csv_file4 << "number,text,date" << endl;
	csv_file4 << "345.0,TEST4,2000-12-13" << endl;
	csv_file4.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT number, text, date FROM test ORDER BY number;");
	REQUIRE(CHECK_COLUMN(result, 0, {345.0}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST4"}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::DATE(2000, 12, 13)}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with two lines both only strings
	ofstream csv_file5(fs.JoinPath(csv_path, "test.csv"));
	csv_file5 << "Alice,StreetA,TEST5" << endl;
	csv_file5 << "Bob,StreetB,TEST5" << endl;
	csv_file5.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Alice", "Bob"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"StreetA", "StreetB"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"TEST5", "TEST5"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with one line, two columsn, only strings
	ofstream csv_file6(fs.JoinPath(csv_path, "test.csv"));
	csv_file6 << "Alice,StreetA" << endl;
	csv_file6.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT column0, column1 FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Alice"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"StreetA"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with one line, two columns - one numeric, one string
	ofstream csv_file7(fs.JoinPath(csv_path, "test.csv"));
	csv_file7 << "1,StreetA" << endl;
	csv_file7.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT column0, column1 FROM test ORDER BY column0;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {"StreetA"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with one line, one string column
	ofstream csv_file8(fs.JoinPath(csv_path, "test.csv"));
	csv_file8 << "Test" << endl;
	csv_file8.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {"Test"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with one line, one numeric column
	ofstream csv_file9(fs.JoinPath(csv_path, "test.csv"));
	csv_file9 << "1" << endl;
	csv_file9.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT * FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));
}

TEST_CASE("Test csv header completion", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	// generate CSV file with one missing header
	ofstream csv_file(fs.JoinPath(csv_path, "test.csv"));
	csv_file << "a,,c" << endl;
	csv_file << "123,TEST1,text1" << endl;
	csv_file << "345,TEST1,text2" << endl;
	csv_file.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT a, column1, c FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST1", "TEST1"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with one duplicate header
	ofstream csv_file2(fs.JoinPath(csv_path, "test.csv"));
	csv_file2 << "a,b,a" << endl;
	csv_file2 << "123,TEST2,text1" << endl;
	csv_file2 << "345,TEST2,text2" << endl;
	csv_file2.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT a_0, b, a_1 FROM test ORDER BY a_0;");
	REQUIRE(CHECK_COLUMN(result, 0, {123, 345}));
	REQUIRE(CHECK_COLUMN(result, 1, {"TEST2", "TEST2"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"text1", "text2"}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with all column names missing
	ofstream csv_file3(fs.JoinPath(csv_path, "test.csv"));
	csv_file3 << ",," << endl;
	csv_file3 << "123,TEST3,text1" << endl;
	csv_file3 << "345,TEST3,text2" << endl;
	csv_file3.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	REQUIRE_NO_FAIL(con.Query("SELECT column0, column1, column2 FROM test ORDER BY column0;"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with 12 columns and all but one column name missing
	ofstream csv_file4(fs.JoinPath(csv_path, "test.csv"));
	csv_file4 << "a,,,,,,,,,,,," << endl;
	csv_file4 << "123,TEST2,text1,,,,,,,,,,value1" << endl;
	csv_file4 << "345,TEST2,text2,,,,,,,,,,value2" << endl;
	csv_file4.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	REQUIRE_NO_FAIL(con.Query("SELECT a, column01, column12 FROM test;"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with 12 equally called columns
	ofstream csv_file5(fs.JoinPath(csv_path, "test.csv"));
	csv_file5 << "a,a,a,a,a,a,a,a,a,a,a,a," << endl;
	csv_file5 << "123,TEST2,text1,,,,,,,,,,value1" << endl;
	csv_file5 << "345,TEST2,text2,,,,,,,,,,value2" << endl;
	csv_file5.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	REQUIRE_NO_FAIL(con.Query("SELECT a_00, a_08, a_09, column12 FROM test;"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));

	// generate CSV file with 10 equally called columns, one named column12 and column 11 and 12 missing
	ofstream csv_file6(fs.JoinPath(csv_path, "test.csv"));
	csv_file6 << "a,a,a,a,a,a,a,a,a,a,column12,," << endl;
	csv_file6 << "123,TEST2,text1,,,,,,,,,,value1" << endl;
	csv_file6 << "345,TEST2,text2,,,,,,,,,,value2" << endl;
	csv_file6.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	REQUIRE_NO_FAIL(con.Query("SELECT a_0, a_8, a_9, column12_0, column11, column12_1 FROM test;"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));
}

TEST_CASE("Test csv type detection with sampling", "[copy]") {
	FileSystem fs;
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	auto csv_path = GetCSVPath();

	idx_t line_count = 9000;

	// generate a CSV file with many strings
	ofstream csv_file(fs.JoinPath(csv_path, "test.csv"));
	csv_file << "linenr|mixed_string|mixed_double" << endl;
	for (idx_t i = 0; i <= line_count; i++) {
		csv_file << i * 3 + 1 << "|1|1" << endl;

		if (i < line_count / 3 || i > line_count * 2 / 3) {
			csv_file << i * 3 + 2 << "|2|2" << endl;
		} else {
			csv_file << i * 3 + 2 << "|TEST|3.5" << endl;
		}

		if (i < line_count / 2) {
			csv_file << i * 3 + 3 << "|3|3" << endl;
		} else {
			csv_file << i * 3 + 3 << "|3|3.5" << endl;
		}
	}
	csv_file.close();

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE test AS SELECT * FROM read_csv_auto ('" + fs.JoinPath(csv_path, "test.csv") + "');"));
	result = con.Query("SELECT linenr, mixed_string, mixed_double FROM test LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {"1", "2", "3"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1.0, 2.0, 3.0}));

	result = con.Query("SELECT linenr, mixed_string, mixed_double FROM test WHERE linenr > 27000 LIMIT 3;");
	REQUIRE(CHECK_COLUMN(result, 0, {27001, 27002, 27003}));
	REQUIRE(CHECK_COLUMN(result, 1, {"1", "2", "3"}));
	REQUIRE(CHECK_COLUMN(result, 2, {1.0, 2.0, 3.5}));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test;"));
}

#endif
