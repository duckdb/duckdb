#include "catch.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <mutex>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test Sequences", "[sequence]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	// note: query verification is disabled for these queries
	// because running the same query multiple times with a sequence does not result in the same answer

	// create a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// cannot create duplicate sequence
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// ignore errors if sequence already exists
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE IF NOT EXISTS seq;"));

	// generate values from the sequence
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT nextval('seq'), nextval('seq');");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {4}));

	// NULL in nextval
	result = con.Query("SELECT nextval(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	result = con.Query("SELECT nextval(a) FROM (VALUES ('seq'), (NULL), ('seq')) tbl1(a)");
	REQUIRE(CHECK_COLUMN(result, 0, {5, Value(), 6}));

	// can't create a sequence that already exists
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// drop the sequence
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	// can't drop non-existing sequence
	REQUIRE_FAIL(con.Query("DROP SEQUENCE seq;"));
	// but doesn't fail with IF EXISTS
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE IF EXISTS seq;"));

	// INCREMENT BY
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT BY 2;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT nextval('\"seq\"')");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// MINVALUE
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MINVALUE 3;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// MAXVALUE
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MAXVALUE 2;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// max value exceeded
	REQUIRE_FAIL(con.Query("SELECT nextval('seq')"));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// MAXVALUE and CYCLE
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MAXVALUE 2 CYCLE;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	// max value exceeded: cycle back
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// START WITH, MINVALUE, MAXVALUE and CYCLE
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MINVALUE 3 MAXVALUE 5 START WITH 4 CYCLE;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// START WITH defaults to MAXVALUE if increment is negative
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT BY -1 MINVALUE 0 MAXVALUE 2;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE_FAIL(con.Query("SELECT nextval('seq')"));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// START WITH defaults to MINVALUE if increment is positive
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT BY 1 MINVALUE 0 MAXVALUE 2;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE_FAIL(con.Query("SELECT nextval('seq')"));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// for positive increment min_value/start defaults to 1 and max_value defaults to 2^63
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT 1 MAXVALUE 3 START 2 CYCLE;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// for negative increment min_value defaults to -2^63 and max_value/start defaults to -1
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT -1 CYCLE;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {-1}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {-2}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {-3}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT -1 MINVALUE -2 CYCLE;"));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {-1}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {-2}));
	result = con.Query("SELECT nextval('seq')");
	REQUIRE(CHECK_COLUMN(result, 0, {-1}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// min_value defaults to 1, setting start to -1 gives start < min_value
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT 1 START -1 CYCLE;"));
	// max_value defaults to -1, setting start to 1 gives start > max_value
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT -1 START 1 CYCLE;"));

	// sequences in schemas
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA a;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA b;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE a.seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE b.seq;"));
	result = con.Query("SELECT nextval('a.seq'), nextval('b.seq');");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));

	// with quotes
	result = con.Query("SELECT nextval('\"a\".\"seq\"'), nextval('\"b\".seq');");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {2}));

	// unterminated quotes
	REQUIRE_FAIL(con.Query("SELECT nextval('\"a\".\"seq');"));
	// too many separators
	REQUIRE_FAIL(con.Query("SELECT nextval('a.b.c.d');"));

	// start exceeds max value
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq MAXVALUE 5 START WITH 6;"));
	// start preceeds min value
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq MINVALUE 5 START WITH 4;"));
	// min value bigger than max
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq MINVALUE 7 MAXVALUE 5;"));
	// increment must not be 0
	REQUIRE_FAIL(con.Query("CREATE SEQUENCE seq INCREMENT 0;"));

	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq2;"));
	// we can use operations in nextval
	result = con.Query("SELECT nextval('s'||'e'||'q')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// sequences with tables
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('seq'), ('seq2')"));

	// nextval is run once per value
	result = con.Query("SELECT s, nextval('seq') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"seq", "seq2"}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2}));
	// we can also use the strings from the table as input to the sequence
	result = con.Query("SELECT s, nextval(s) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"seq", "seq2"}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 1}));

	// this will also cause an error if the sequence does not exist
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('nonexistant_seq')"));
	REQUIRE_FAIL(con.Query("SELECT s, nextval(s) FROM strings"));
}

TEST_CASE("Test Sequences Are Transactional", "[sequence]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// start a transaction for both nodes
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// create a sequence in node one
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// node one can see it
	REQUIRE_NO_FAIL(con.Query("SELECT nextval('seq');"));
	// node two can't see it
	REQUIRE_FAIL(con2.Query("SELECT nextval('seq');"));

	// we commit the sequence
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	// node two still can't see it
	REQUIRE_FAIL(con2.Query("SELECT nextval('seq');"));
	// now commit node two
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// we can now see the sequence in node two
	REQUIRE_NO_FAIL(con2.Query("SELECT nextval('seq');"));

	// drop sequence seq in a transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// node one can't use it anymore
	REQUIRE_FAIL(con.Query("SELECT nextval('seq');"));
	// node two can still use it
	REQUIRE_NO_FAIL(con2.Query("SELECT nextval('seq');"));

	// rollback cancels the drop sequence
	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	// we can still use it
	REQUIRE_NO_FAIL(con2.Query("SELECT nextval('seq');"));

	// now we drop it for real
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// we can't use it anymore
	REQUIRE_FAIL(con.Query("SELECT nextval('seq');"));
	REQUIRE_FAIL(con2.Query("SELECT nextval('seq');"));
}

struct ConcurrentData {
	DuckDB &db;
	mutex lock;
	vector<int64_t> results;

	ConcurrentData(DuckDB &db) : db(db) {
	}
};

#define THREAD_COUNT 20
#define INSERT_COUNT 100

static void append_values_from_sequence(ConcurrentData *data) {
	Connection con(data->db);
	for (size_t i = 0; i < INSERT_COUNT; i++) {
		auto result = con.Query("SELECT nextval('seq')");
		int64_t res = result->GetValue(0, 0).GetValue<int64_t>();
		lock_guard<mutex> lock(data->lock);
		data->results.push_back(res);
	}
}

TEST_CASE("Test Concurrent Usage of Sequences", "[sequence][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	thread threads[THREAD_COUNT];
	ConcurrentData data(db);
	ConcurrentData seq_data(db);

	// create a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// fetch a number of values sequentially
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		append_values_from_sequence(&seq_data);
	}

	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// now launch threads that all use the sequence in parallel
	// each appends the values to a vector "results"
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(append_values_from_sequence, &data);
	}
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}
	// now we sort the output data
	std::sort(seq_data.results.begin(), seq_data.results.end());
	std::sort(data.results.begin(), data.results.end());
	// the sequential and threaded data should be the same
	REQUIRE(seq_data.results == data.results);

	seq_data.results.clear();
	data.results.clear();
	// now do the same but for a cyclic sequence
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MAXVALUE 10 CYCLE;"));
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		append_values_from_sequence(&seq_data);
	}

	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq MAXVALUE 10 CYCLE;"));
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(append_values_from_sequence, &data);
	}
	for (size_t i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}
	// now we sort the output data
	std::sort(seq_data.results.begin(), seq_data.results.end());
	std::sort(data.results.begin(), data.results.end());
	// the sequential and threaded data should be the same
	REQUIRE(seq_data.results == data.results);
}

TEST_CASE("Test query verification failures", "[sequence]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// create a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));

	// getting the value from a sequence results in a verification failure, but only in debug mode
	con.Query("SELECT nextval('seq')");
	// normal queries still work after that
	REQUIRE_NO_FAIL(con.Query("SELECT 1"));
}
