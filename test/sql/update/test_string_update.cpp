#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test update of string columns", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	// scan the table
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// test a delete from the table
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a='hello';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// now test an update of the table
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='hello';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello"}));
}

TEST_CASE("Test update of string columns with NULLs", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	// update a string to NULL
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL where a='world';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello"}));
}

TEST_CASE("Test repeated update of string in same segment", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	// scan the table
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// test a number of repeated updates
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test' WHERE a='hello';"));
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test2' WHERE a='world';"));
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "test2"}));

	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "test2"}));
}

TEST_CASE("Test repeated update of string in same transaction", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));

	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test' WHERE a='hello';"));
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test2' WHERE a='world';"));
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "test2"}));

	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "test2"}));
}

TEST_CASE("Test rollback of string update", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('hello'), ('world')"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));

	// perform an update within the transaction
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test' WHERE a='hello';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// now rollback the update
	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"hello", "world"}));

	// rollback of a value that is updated twice
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test' WHERE a='hello';"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test2' WHERE a='test';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test2", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));

	// test rollback of string update in different part
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='test2' WHERE a='world';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "test2"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
}

TEST_CASE("Test rollback of string update with NULL", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('test'), ('world')"));

	// test rollback of value -> NULL update
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL WHERE a='world';"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "test"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));

	// test rollback of NULL -> value update
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=NULL WHERE a='world';"));
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "test"}));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='world' WHERE a IS NULL;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {"test", "world"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "test"}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "test"}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "test"}));
}

TEST_CASE("Test string updates with many strings", "[update][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('a'), ('b'), ('c'), (NULL)"));

	// insert the same strings many times
	idx_t size = 4 * sizeof(int32_t);
	while (size < Storage::BLOCK_SIZE * 2) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT * FROM test"));
		size *= 4;
	}

	// verify that the distinct values are correct
	result = con.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "a", "b", "c"}));
	result = con2.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "a", "b", "c"}));

	// test update of string column in another transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='aa' WHERE a='a';"));

	// verify that the values were updated
	result = con.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "aa", "b", "c"}));
	result = con2.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "a", "b", "c"}));

	// now roll it back
	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	// the values should be back to normal
	result = con.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "a", "b", "c"}));
	result = con2.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "a", "b", "c"}));

	// this time do the same but commit it
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='aa' WHERE a='a';"));

	// now both connections have the updated value
	result = con.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "aa", "b", "c"}));
	result = con2.Query("SELECT DISTINCT a FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "aa", "b", "c"}));
}

TEST_CASE("Test update of big string", "[update][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('abcdefghijklmnopqrstuvwxyz')"));

	// increase the size of the string until it is bigger than a block
	idx_t size = 26;
	while (size < Storage::BLOCK_SIZE * 2) {
		// concat the string 10x and insert it
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a FROM test"));
		// delete the old value
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE length(a) = (SELECT MIN(length(a)) FROM test)"));
		size *= 10;
	}

	// verify that the string length is correct
	result = con.Query("SELECT LENGTH(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(size)}));

	// now update the big string in a separate transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a='a'"));

	// verify the lengths
	result = con.Query("SELECT LENGTH(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(1)}));
	result = con2.Query("SELECT LENGTH(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(size)}));

	// now commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// the big string is gone now
	result = con.Query("SELECT LENGTH(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(1)}));
	result = con2.Query("SELECT LENGTH(a) FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(1)}));
}
