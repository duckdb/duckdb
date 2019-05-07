#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("ART Index BigInt", "[art-bigint]") {
    unique_ptr<QueryResult> result;
    DuckDB db(nullptr);

    Connection con(db);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT)"));
    REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

    size_t n= 10000;
    int64_t* keys=new int64_t[n];
    for (size_t i=0;i<n;i++)
        keys[i]=i+1;
    std::random_shuffle(keys,keys+n);

    for (size_t i = 0; i < n; i++) {
        REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(keys[i]) + ")"));
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(keys[i])}));
    }
    // Checking non-existing values
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(-1));
    REQUIRE(CHECK_COLUMN(result, 0, {}));
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(10001));
    REQUIRE(CHECK_COLUMN(result, 0, {}));

    // Checking if all elements are still there
    for (size_t i = 0; i < n; i++) {
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(keys[i])}));
    }

    // Checking Duplicates
    REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(1) + ")"));
    result = con.Query("SELECT SUM(i) FROM integers WHERE i="+ to_string(1));
    REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(2)}));

    REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
    REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("ART Index Int", "[art-int]") {
    unique_ptr<QueryResult> result;
    DuckDB db(nullptr);

    Connection con(db);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
    REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

    size_t n= 1000;
    int32_t* keys=new int32_t[n];
    for (size_t i=0;i<n;i++)
        keys[i]=i+1;
    std::random_shuffle(keys,keys+n);

    for (size_t i = 0; i < n; i++) {
        REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(keys[i]) + ")"));
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
    }
    // Checking non-existing values
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(-1));
    REQUIRE(CHECK_COLUMN(result, 0, {}));
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(10001));
    REQUIRE(CHECK_COLUMN(result, 0, {}));

    // Checking if all elements are still there
    for (size_t i = 0; i < n; i++) {
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
    }

    // Checking Duplicates
    REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(1) + ")"));
    result = con.Query("SELECT SUM(i) FROM integers WHERE i="+ to_string(1));
    REQUIRE(CHECK_COLUMN(result, 0, {Value(2)}));

    // successful update
    REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=14 WHERE i=13"));
    result = con.Query("SELECT * FROM integers WHERE i=14");
    REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));

    // rolled back update
    REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
    // update the value
    REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=14 WHERE i=12"));
    // now there are three values with 14
    result = con.Query("SELECT * FROM integers WHERE i=14");
    REQUIRE(CHECK_COLUMN(result, 0, {14, 14, 14}));
    // rollback the value
    REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
    // after the rollback
    result = con.Query("SELECT * FROM integers WHERE i=14");
    REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));

    // roll back insert
    REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
    // update the value
    REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (14)"));
    // now there are three values with 14
    result = con.Query("SELECT * FROM integers WHERE i=14");
    REQUIRE(CHECK_COLUMN(result, 0, {14, 14, 14}));
    // rollback the value
    REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
    // after the rollback
    result = con.Query("SELECT * FROM integers WHERE i=14");
    REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));


    // Now Doing Deletes
    REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=15"));
    // check the value again
    result = con.Query("SELECT * FROM integers WHERE i=15");
    REQUIRE(CHECK_COLUMN(result, 0, {}));


    REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
    REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("ART Index SmallInt", "[art-smallint]") {
    unique_ptr<QueryResult> result;
    DuckDB db(nullptr);

    Connection con(db);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i SMALLINT)"));
    REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

    size_t n= 1000;
    int16_t* keys=new int16_t[n];
    for (size_t i=0;i<n;i++)
        keys[i]=i+1;
    std::random_shuffle(keys,keys+n);

    for (size_t i = 0; i < n; i++) {
        REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(keys[i]) + ")"));
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value::SMALLINT(keys[i])}));
    }
//    // Checking non-existing values
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(-1));
    REQUIRE(CHECK_COLUMN(result, 0, {}));
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(10001));
    REQUIRE(CHECK_COLUMN(result, 0, {}));

    // Checking if all elements are still there
    for (size_t i = 0; i < n; i++) {
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value::SMALLINT(keys[i])}));
    }

    // Checking Duplicates
    REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(1) + ")"));
    result = con.Query("SELECT SUM(i) FROM integers WHERE i="+ to_string(1));
    REQUIRE(CHECK_COLUMN(result, 0, {Value::SMALLINT(2)}));

    REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
    REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}


TEST_CASE("ART Index TinyInt", "[art-tinyint]") {
    unique_ptr<QueryResult> result;
    DuckDB db(nullptr);

    Connection con(db);
    REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i TINYINT)"));
    REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

    size_t n= 100;
    int8_t* keys=new int8_t[n];
    for (size_t i=0;i<n;i++)
        keys[i]=i+1;
    std::random_shuffle(keys,keys+n);

    for (size_t i = 0; i < n; i++) {
        REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(keys[i]) + ")"));
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value::TINYINT(keys[i])}));
    }
//    // Checking non-existing values
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(-1));
    REQUIRE(CHECK_COLUMN(result, 0, {}));
    result = con.Query("SELECT i FROM integers WHERE i="+ to_string(10001));
    REQUIRE(CHECK_COLUMN(result, 0, {}));

    // Checking if all elements are still there
    for (size_t i = 0; i < n; i++) {
        result = con.Query("SELECT i FROM integers WHERE i="+ to_string(keys[i]));
        REQUIRE(CHECK_COLUMN(result, 0, {Value::TINYINT(keys[i])}));
    }

    // Checking Duplicates
    REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(1) + ")"));
    result = con.Query("SELECT SUM(i) FROM integers WHERE i="+ to_string(1));
    REQUIRE(CHECK_COLUMN(result, 0, {Value::TINYINT(2)}));

    REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
    REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}


