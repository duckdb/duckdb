#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;


//FIXME: Rework undo buffer when destroying db
TEST_CASE("Test index creation statements with multiple connections", "[art-index-mult]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
//	Connection con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3)"));
	for (size_t i = 0; i < 3000; i++) {
		REQUIRE_NO_FAIL(
				con.Query("INSERT INTO integers VALUES (" + to_string(i + 10) + ", " + to_string(i + 12) + ")"));
	}

	// both con and con2 start a transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
//	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// con2 updates the integers array before index creation
//	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=4 WHERE i=1"));

	// con creates an index
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	// con should see the old state
	result = con.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// con2 should see the updated state
//	result = con2.Query("SELECT j FROM integers WHERE i=4");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// now we commit con
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// con should still see the old state
	result = con.Query("SELECT j FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

//	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// after commit of con2 - con should see the old state
//	result = con.Query("SELECT j FROM integers WHERE i=4");
//	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// now we update the index again, this time after index creation
//	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=7 WHERE i=4"));
	// the new state should be visible
//	result = con.Query("SELECT j FROM integers WHERE i=7");
//	REQUIRE(CHECK_COLUMN(result, 0, {3}));
}


TEST_CASE("ART Integer Types", "[art-int]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);

    string int_types[4] = {"tinyint", "smallint", "integer", "bigint"};
    int32_t n_sizes[4] = {100,1000,10000,100000};
    for (int idx = 1; idx < 4; idx ++ ){
        REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i "+int_types[idx]+")"));
        REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

        int32_t n = n_sizes[idx];
        int32_t *keys = new int32_t[n];
        for (int32_t i = 0; i < n; i++)
            keys[i] = i + 1;
        std::random_shuffle(keys, keys + n);

        for (int32_t i = 0; i < n; i++) {
            REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(keys[i]) + ")"));
            result = con.Query("SELECT i FROM integers WHERE i=" + to_string(keys[i]));
            REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
        }
        //! Checking non-existing values
        result = con.Query("SELECT i FROM integers WHERE i=" + to_string(-1));
        REQUIRE(CHECK_COLUMN(result, 0, {}));
        result = con.Query("SELECT i FROM integers WHERE i=" + to_string(n_sizes[idx]+1));
        REQUIRE(CHECK_COLUMN(result, 0, {}));

        //! Checking if all elements are still there
        for (size_t i = 0; i < n; i++) {
            result = con.Query("SELECT i FROM integers WHERE i=" + to_string(keys[i]));
            REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
        }

        //! Checking Multiple Range Queries
        int32_t up_range_result = n_sizes[idx]*2 -1;
        result = con.Query("SELECT sum(i) FROM integers WHERE i >=" + to_string(n_sizes[idx]-1));
        REQUIRE(CHECK_COLUMN(result, 0, {Value(up_range_result)}));

        result = con.Query("SELECT sum(i) FROM integers WHERE i >"+to_string(n_sizes[idx]-2));
        REQUIRE(CHECK_COLUMN(result, 0, {Value(up_range_result)}));

        result = con.Query("SELECT sum(i) FROM integers WHERE i >2 AND i <5");
        REQUIRE(CHECK_COLUMN(result, 0, {Value(7)}));

        result = con.Query("SELECT sum(i) FROM integers WHERE i >=2 AND i <5");
        REQUIRE(CHECK_COLUMN(result, 0, {Value(9)}));

        result = con.Query("SELECT sum(i) FROM integers WHERE i >2 AND i <=5");
        REQUIRE(CHECK_COLUMN(result, 0, {Value(12)}));

        result = con.Query("SELECT sum(i) FROM integers WHERE i >=2 AND i <=5");
        REQUIRE(CHECK_COLUMN(result, 0, {Value(14)}));
        result = con.Query("SELECT sum(i) FROM integers WHERE i <=2");
        REQUIRE(CHECK_COLUMN(result, 0, {Value(3)}));

        result = con.Query("SELECT sum(i) FROM integers WHERE i <0");
        REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

        result = con.Query("SELECT sum(i) FROM integers WHERE i >10000000");
        REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

        //! Checking Duplicates
        REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (" + to_string(1) + ")"));
        result = con.Query("SELECT SUM(i) FROM integers WHERE i=" + to_string(1));
        REQUIRE(CHECK_COLUMN(result, 0, {Value(2)}));

//        //! Successful update
//        REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=14 WHERE i=13"));
//        result = con.Query("SELECT * FROM integers WHERE i=14");
//        REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));
//
//        //!Testing rollbacks and commits
//        // rolled back update
//        REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
//        // update the value
//        REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=14 WHERE i=12"));
//        // now there are three values with 14
//        result = con.Query("SELECT * FROM integers WHERE i=14");
//        REQUIRE(CHECK_COLUMN(result, 0, {14, 14, 14}));
//        // rollback the value
//        REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
//        // after the rollback
//        result = con.Query("SELECT * FROM integers WHERE i=14");
//        REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));
//        // roll back insert
//        REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
//        // update the value
//        REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (14)"));
//        // now there are three values with 14
//        result = con.Query("SELECT * FROM integers WHERE i=14");
//        REQUIRE(CHECK_COLUMN(result, 0, {14, 14, 14}));
//        // rollback the value
//        REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
//        // after the rollback
//        result = con.Query("SELECT * FROM integers WHERE i=14");
//        REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));

        //!Testing deletes
        // Delete non-existing element
        REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=0"));
        // Now Deleting all elements
        for (int32_t i = 0; i < n; i++) {
            REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=" + to_string(i)));
            // check the value does not exist
            result = con.Query("SELECT * FROM integers WHERE i=" + to_string(i));
            REQUIRE(CHECK_COLUMN(result, 0, {}));
        }
        // Delete from empty tree
        REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=0"));

        REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
        REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
    }

}