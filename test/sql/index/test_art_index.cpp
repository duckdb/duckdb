#include "catch.hpp"
#include "duckdb/common/radix.hpp"
#include "test_helpers.hpp"

#include <cfloat>
#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test ART index with rollbacks", "[art][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));
	idx_t count = 0;
	for (int32_t it = 0; it < 10; it++) {
		for (int32_t val = 0; val < 1000; val++) {
			REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
			REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", val));
			if (it + val % 2) {
				count++;
				REQUIRE_NO_FAIL(con.Query("COMMIT"));
			} else {
				REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
			}
		}
	}
	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(count)}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i < 1000000");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(count)}));
}

// If you directly use RAND_MAX:
// > warning: implicit conversion from 'int' to 'float' changes value from 2147483647 to 2147483648
constexpr float RAND_MAX_FLOAT = static_cast<float>(static_cast<double>(RAND_MAX));

float generate_small_float() {
	return static_cast<float>(rand()) / RAND_MAX_FLOAT;
}

float generate_float(float min_float, float max_float) {
	return min_float + static_cast<float>(rand()) / (RAND_MAX_FLOAT / (max_float - min_float));
}

double generate_small_double() {
	return static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
}

double generate_double(double min_double, double max_double) {
	return min_double + static_cast<double>(rand()) / (static_cast<double>(RAND_MAX / (max_double - min_double)));
}

template <class T>
int full_scan(T *keys, idx_t size, T low, T high) {
	int sum = 0;
	for (idx_t i = 0; i < size; i++) {
		if (keys[i] >= low && keys[i] <= high) {
			sum += 1;
		}
	}
	return sum;
}

TEST_CASE("ART Floating Point Small", "[art-float-small]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	duckdb::vector<int64_t> min_values, max_values;
	Connection con(db);
	// Will use 100 keys
	idx_t n = 100;
	auto keys = duckdb::unique_ptr<int64_t[]>(new int64_t[n]);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i BIGINT)"));
	// Generate 10 small floats (0.0 - 1.0)
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Radix::EncodeFloat(generate_small_float());
	}

	// Generate 40 floats (-50/50)
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(-50, 50));
	}
	// Generate 50 floats (min/max)
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	// Insert values and create index
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	// Generate 500 small-small range queries
	for (idx_t i = 0; i < 5; i++) {
		a = Radix::EncodeFloat(generate_small_float());
		b = Radix::EncodeFloat(generate_small_float());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 normal-normal range queries
	for (idx_t i = 0; i < 5; i++) {
		a = Radix::EncodeFloat(generate_float(-50, 50));
		b = Radix::EncodeFloat(generate_float(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 big-big range queries
	for (idx_t i = 0; i < 5; i++) {
		a = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		b = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Radix::EncodeFloat(min_values[i]);
		int64_t high = Radix::EncodeFloat(max_values[i]);
		int answer = full_scan<int64_t>(keys.get(), n, low, high);
		string query =
		    "SELECT COUNT(i) FROM numbers WHERE i >= " + to_string(low) + " and i <= " + to_string(high) + ";";
		result = con.Query(query);
		if (!CHECK_COLUMN(result, 0, {answer})) {
			cout << "Wrong answer on floating point real-small!" << std::endl << "Queries to reproduce:" << std::endl;
			cout << "CREATE TABLE numbers(i BIGINT);" << std::endl;
			for (idx_t k = 0; k < n; k++) {
				cout << "INSERT INTO numbers VALUES (" << keys[k] << ");" << std::endl;
			}
			cout << query << std::endl;
			REQUIRE(false);
		}
	}
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE numbers"));
}

TEST_CASE("ART Floating Point Double Small", "[art-double-small]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	duckdb::vector<int64_t> min_values, max_values;
	Connection con(db);
	// Will use 100 keys
	idx_t n = 100;
	auto keys = duckdb::unique_ptr<int64_t[]>(new int64_t[n]);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i BIGINT)"));
	// Generate 10 small floats (0.0 - 1.0)
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Radix::EncodeFloat(generate_small_float());
	}

	// Generate 40 floats (-50/50)
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(-50, 50));
	}
	// Generate 50 floats (min/max)
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	// Insert values and create index
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	// Generate 500 small-small range queries
	for (idx_t i = 0; i < 5; i++) {
		a = Radix::EncodeDouble(generate_small_double());
		b = Radix::EncodeDouble(generate_small_double());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 normal-normal range queries
	for (idx_t i = 0; i < 5; i++) {
		a = Radix::EncodeDouble(generate_double(-50, 50));
		b = Radix::EncodeDouble(generate_double(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 big-big range queries
	for (idx_t i = 0; i < 5; i++) {
		a = Radix::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		b = Radix::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Radix::EncodeDouble(min_values[i]);
		int64_t high = Radix::EncodeDouble(max_values[i]);
		int answer = full_scan<int64_t>(keys.get(), n, low, high);
		string query =
		    "SELECT COUNT(i) FROM numbers WHERE i >= " + to_string(low) + " and i <= " + to_string(high) + ";";
		result = con.Query(query);
		if (!CHECK_COLUMN(result, 0, {answer})) {
			cout << "Wrong answer on double!" << std::endl << "Queries to reproduce:" << std::endl;
			cout << "CREATE TABLE numbers(i BIGINT);" << std::endl;
			for (idx_t k = 0; k < n; k++) {
				cout << "INSERT INTO numbers VALUES (" << keys[k] << ");" << std::endl;
			}
			cout << query << std::endl;
			REQUIRE(false);
		}
	}
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE numbers"));
}

TEST_CASE("ART Floating Point", "[art-float][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	duckdb::vector<int64_t> min_values, max_values;
	Connection con(db);
	// Will use 10k keys
	idx_t n = 10000;
	auto keys = duckdb::unique_ptr<int64_t[]>(new int64_t[n]);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i BIGINT)"));
	// Generate 1000 small floats (0.0 - 1.0)
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Radix::EncodeFloat(generate_small_float());
	}

	// Generate 4000 floats (-50/50)
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(-50, 50));
	}
	// Generate 5000 floats (min/max)
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	// Insert values and create index
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	// Generate 500 small-small range queries
	for (idx_t i = 0; i < 500; i++) {
		a = Radix::EncodeFloat(generate_small_float());
		b = Radix::EncodeFloat(generate_small_float());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 normal-normal range queries
	for (idx_t i = 0; i < 500; i++) {
		a = Radix::EncodeFloat(generate_float(-50, 50));
		b = Radix::EncodeFloat(generate_float(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 big-big range queries
	for (idx_t i = 0; i < 500; i++) {
		a = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		b = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Radix::EncodeFloat(min_values[i]);
		int64_t high = Radix::EncodeFloat(max_values[i]);
		int answer = full_scan<int64_t>(keys.get(), n, low, high);
		string query =
		    "SELECT COUNT(i) FROM numbers WHERE i >= " + to_string(low) + " and i <= " + to_string(high) + ";";
		result = con.Query(query);
		if (!CHECK_COLUMN(result, 0, {answer})) {
			cout << "Wrong answer on floating point real-small!" << std::endl << "Queries to reproduce:" << std::endl;
			cout << "CREATE TABLE numbers(i BIGINT);" << std::endl;
			for (idx_t k = 0; k < n; k++) {
				cout << "INSERT INTO numbers VALUES (" << keys[k] << ");" << std::endl;
			}
			cout << query << std::endl;
			REQUIRE(false);
		}
	}
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE numbers"));
}

TEST_CASE("ART Floating Point Double", "[art-double][.]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	duckdb::vector<int64_t> min_values, max_values;
	Connection con(db);
	// Will use 10000 keys
	idx_t n = 10000;
	auto keys = duckdb::unique_ptr<int64_t[]>(new int64_t[n]);
	con.Query("CREATE TABLE numbers(i BIGINT)");
	// Generate 1000 small floats (0.0 - 1.0)
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Radix::EncodeFloat(generate_small_float());
	}

	// Generate 4000 floats (-50/50)
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(-50, 50));
	}
	// Generate 5000 floats (min/max)
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Radix::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	// Insert values and create index
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	// Generate 500 small-small range queries
	for (idx_t i = 0; i < 500; i++) {
		a = Radix::EncodeDouble(generate_small_double());
		b = Radix::EncodeDouble(generate_small_double());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 normal-normal range queries
	for (idx_t i = 0; i < 500; i++) {
		a = Radix::EncodeDouble(generate_double(-50, 50));
		b = Radix::EncodeDouble(generate_double(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	// Generate 500 big-big range queries
	for (idx_t i = 0; i < 500; i++) {
		a = Radix::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		b = Radix::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Radix::EncodeDouble(min_values[i]);
		int64_t high = Radix::EncodeDouble(max_values[i]);
		int answer = full_scan<int64_t>(keys.get(), n, low, high);
		string query =
		    "SELECT COUNT(i) FROM numbers WHERE i >= " + to_string(low) + " and i <= " + to_string(high) + ";";
		result = con.Query(query);
		if (!CHECK_COLUMN(result, 0, {answer})) {
			cout << "Wrong answer on floating point real-small!" << std::endl << "Queries to reproduce:" << std::endl;
			cout << "CREATE TABLE numbers(i BIGINT);" << std::endl;
			for (idx_t k = 0; k < n; k++) {
				cout << "INSERT INTO numbers VALUES (" << keys[k] << ");" << std::endl;
			}
			cout << query << std::endl;
			REQUIRE(false);
		}
	}
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE numbers"));
}
