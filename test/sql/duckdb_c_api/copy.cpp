//
// Created by Pedro Holanda on 10/08/2018.
//


#include "catch.hpp"

#include <vector>

#include "duckdb_c_test.hpp"
# include <fstream>

using namespace std;

TEST_CASE("Test Copy statement", "[copystatement]") {
    duckdb_database database;
    duckdb_connection connection;
    duckdb_result result;

    // open and close a database in in-memory mode
    REQUIRE(duckdb_open(NULL, &database) == DuckDBSuccess);
    REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);

    // Generate CSV file
    ofstream csvfile("test.csv");
    csvfile << "1,2.5,test" << endl ;

    // Loading CSV into a Table
    REQUIRE(duckdb_query(connection,
                         "CREATE TABLE test (a INTEGER, b DOUBLE,c VARCHAR(10));",
                         NULL) == DuckDBSuccess);
    REQUIRE(duckdb_query(connection, "COPY test FROM 'test.csv'",
                         NULL) == DuckDBSuccess);
    REQUIRE(duckdb_query(connection,
                         "SELECT * FROM test;",
                         &result) == DuckDBSuccess);

    REQUIRE(CHECK_NUMERIC_COLUMN(result, 0, {1}));
    REQUIRE(CHECK_DECIMAL_COLUMN(result, 1, {2.5}));
    REQUIRE(CHECK_STRING_COLUMN(result, 2, {"test"}));
    csvfile.close();
    duckdb_destroy_result(result);

//    // Generate CSV file with different delimiter.
//    ofstream csvfile2("test.csv");
//    csvfile2 << "2;2.5;test" << endl ;
//
//    // Loading CSV into a Table defining delimiter.
//
//    REQUIRE(duckdb_query(connection, "COPY test FROM 'test.csv' DELIMITER ';'",
//                         NULL) == DuckDBSuccess);
//    REQUIRE(duckdb_query(connection,
//                         "SELECT * FROM test where a = 2;",
//                         &result) == DuckDBSuccess);

//    Generate CSV from Table


    REQUIRE(duckdb_disconnect(connection) == DuckDBSuccess);
    REQUIRE(duckdb_close(database) == DuckDBSuccess);
}
