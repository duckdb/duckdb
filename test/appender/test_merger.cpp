#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <vector>
#include <random>
#include <algorithm>
#ifndef _WIN32
#include <sys/time.h>
#endif
using namespace duckdb;
using namespace std;

TEST_CASE("Basic merger tests", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 16)"));

	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {16}));

	Merger merger(con, "integers");
	merger.BeginRow();
	merger.Append<int32_t>(1);
	merger.Append<int32_t>(18);	
	merger.EndRow();

	merger.Flush();

	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {18}));
}

TEST_CASE("Test multiple AppendRow", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	// append a bunch of values
	{
		Appender appender(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
		  appender.AppendRow((int32_t)(i), (int32_t)(i));
		}
		appender.Close();
	}

	// merge rows
	// change even key value
	duckdb::vector<duckdb::Value> values;
	{
		Merger merger(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
		  if ((i % 2 ) == 0) {
		    merger.AppendRow( (int32_t)(i),  (int32_t)(2*i));
		    values.push_back(duckdb::Value((int32_t)(2*i)));
		  }
		  else {
		    values.push_back(duckdb::Value((int32_t)i));
		  }
		}
		merger.Close();
	}
	
	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, values));
}

TEST_CASE("Test merging out of key order", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i BIGINT, j VARCHAR, b VARCHAR, PRIMARY KEY (i))"));

	auto number_of_values = 2000;
	std::vector<idx_t> key_ids;
	duckdb::vector<duckdb::Value> key_values;
	duckdb::vector<duckdb::Value> j_values;
	duckdb::vector<duckdb::Value> b_values;

	// append a bunch of values
	Appender appender(con, "test");
	for (int32_t i = 0; i < number_of_values; i++) {
		auto index_string = std::to_string(i);
		appender.AppendRow(i, (string_t)("j-" + index_string), (string_t)("b-" + index_string));

		key_ids.push_back(i);
		key_values.push_back(duckdb::Value(i));
		j_values.push_back(duckdb::Value("uj-" + index_string));
		b_values.push_back(duckdb::Value("ub-" + index_string));
	}
	appender.Close();

	auto rng = std::default_random_engine {random_device{}()};
	std::shuffle(std::begin(key_ids), std::end(key_ids), rng);

	// merge rows
	Merger merger(con, "test");
	for (idx_t i = 0; i < key_ids.size(); i++) {
		int32_t key_value = key_ids[i];
		auto j_update_value = j_values[key_value];
		auto b_update_value = b_values[key_value];

		merger.AppendRow(key_value, j_update_value, b_update_value);
	}
	merger.Close();

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, key_values));
	REQUIRE(CHECK_COLUMN(result, 1, j_values));
	REQUIRE(CHECK_COLUMN(result, 2, b_values));
}

TEST_CASE("Partial merge tests", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT, b VARCHAR default 'abc', j int, k int default 9, PRIMARY KEY (i))"));

	// append a bunch of values
	duckdb::vector<idx_t> values;
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 1; i++) {
		auto num = i;
		auto key = "key-" + std::to_string(num);
		appender.AppendRow((int64_t)(num), duckdb::Value(key.c_str()), (int32_t) i, (int32_t) i);
		values.push_back(num);
	}
	appender.Close();

	duckdb::vector<std::string> columns = {"j", "i"};

	Merger merger(con, "integers", columns);
	merger.AppendRow(33, 0);
	merger.Close();

	result = con.Query("SELECT b, j, k FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {"key-0"}));
	REQUIRE(CHECK_COLUMN(result, 1, {33}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));

	duckdb::vector<std::string> columns2 = {"k", "i"};

	Merger merger2(con, "integers", columns2);
	merger2.AppendRow(44, 0);
	merger2.Close();

	result = con.Query("SELECT b, j, k FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {"key-0"}));
	REQUIRE(CHECK_COLUMN(result, 1, {33}));
	REQUIRE(CHECK_COLUMN(result, 2, {44}));
}

TEST_CASE("Merger new rows add defaults", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT, b VARCHAR default 'abc', j int, k int default 9, PRIMARY KEY (i))"));

	// append a bunch of values
	duckdb::vector<idx_t> values;
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 1; i++) {
		auto num = i;
		auto key = "key-" + std::to_string(num);
		appender.AppendRow((int64_t)(num), duckdb::Value(key.c_str()), (int32_t) i, (int32_t) i);
		values.push_back(num);
	}
	appender.Close();

	duckdb::vector<std::string> columns = {"j", "i"};

	Merger merger(con, "integers", columns);
	merger.AppendRow(33, 1);
	merger.Close();

	result = con.Query("SELECT b, j, k FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {"key-0", "abc"}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 33}));
	REQUIRE(CHECK_COLUMN(result, 2, {0, 9}));
}