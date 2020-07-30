#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

#include <cfloat>
#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test ART index with rollbacks", "[art][.]") {
	unique_ptr<QueryResult> result;
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

TEST_CASE("Test ART index with the same value multiple times", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));
	for (int32_t val = 0; val < 100; val++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", val));
	}
	for (int32_t val = 0; val < 100; val++) {
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	for (int32_t it = 0; it < 10; it++) {
		for (int32_t val = 0; val < 100; val++) {
			result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
			REQUIRE(CHECK_COLUMN(result, 0, {it + 1}));
		}
		for (int32_t val = 0; val < 100; val++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", val));
			result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
			REQUIRE(CHECK_COLUMN(result, 0, {it + 2}));
		}
		for (int32_t val = 0; val < 100; val++) {
			result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
			REQUIRE(CHECK_COLUMN(result, 0, {it + 2}));
		}
	}
}

TEST_CASE("ART Integer Types", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	string int_types[4] = {"tinyint", "smallint", "integer", "bigint"};
	idx_t n_sizes[4] = {100, 1000, 1000, 1000};
	for (idx_t idx = 0; idx < 4; idx++) {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i " + int_types[idx] + ")"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

		idx_t n = n_sizes[idx];
		auto keys = unique_ptr<int32_t[]>(new int32_t[n]);
		auto key_pointer = keys.get();
		for (idx_t i = 0; i < n; i++) {
			keys[i] = i + 1;
		}
		std::random_shuffle(key_pointer, key_pointer + n);

		for (idx_t i = 0; i < n; i++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", keys[i]));
			result =
			    con.Query("SELECT i FROM integers WHERE i=CAST(" + to_string(keys[i]) + " AS " + int_types[idx] + ")");
			REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
		}
		con.AddComment("Checking non-existing values");
		result = con.Query("SELECT i FROM integers WHERE i=CAST(" + to_string(-1) + " AS " + int_types[idx] + ")");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT i FROM integers WHERE i=CAST(" + to_string(n_sizes[idx] + 1) + " AS " +
		                   int_types[idx] + ")");
		REQUIRE(CHECK_COLUMN(result, 0, {}));

		con.AddComment("Checking if all elements are still there");
		for (idx_t i = 0; i < n; i++) {
			result =
			    con.Query("SELECT i FROM integers WHERE i=CAST(" + to_string(keys[i]) + " AS " + int_types[idx] + ")");
			REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
		}

		con.AddComment("Checking Multiple Range Queries");
		int32_t up_range_result = n_sizes[idx] * 2 - 1;
		result = con.Query("SELECT sum(i) FROM integers WHERE i >= " + to_string(n_sizes[idx] - 1));
		REQUIRE(CHECK_COLUMN(result, 0, {Value(up_range_result)}));

		result = con.Query("SELECT sum(i) FROM integers WHERE i > " + to_string(n_sizes[idx] - 2));
		REQUIRE(CHECK_COLUMN(result, 0, {Value(up_range_result)}));

		result = con.Query("SELECT sum(i) FROM integers WHERE i > 2 AND i < 5");
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

		con.AddComment("Checking Duplicates");
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1)"));
		result = con.Query("SELECT SUM(i) FROM integers WHERE i=1");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(2)}));

		con.AddComment("Successful update");
		REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=14 WHERE i=13"));
		result = con.Query("SELECT * FROM integers WHERE i=14");
		REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));

		con.AddComment("Testing rollbacks and commits");
		con.AddComment("rolled back update");
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		con.AddComment("update the value");
		REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=14 WHERE i=12"));
		con.AddComment("now there are three values with 14");
		result = con.Query("SELECT * FROM integers WHERE i=14");
		REQUIRE(CHECK_COLUMN(result, 0, {14, 14, 14}));
		con.AddComment("rollback the value");
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		con.AddComment("after the rollback");
		result = con.Query("SELECT * FROM integers WHERE i=14");
		REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));
		con.AddComment("roll back insert");
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		con.AddComment("update the value");
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (14)"));
		con.AddComment("now there are three values with 14");
		result = con.Query("SELECT * FROM integers WHERE i=14");
		REQUIRE(CHECK_COLUMN(result, 0, {14, 14, 14}));
		con.AddComment("rollback the value");
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
		con.AddComment("after the rol");
		result = con.Query("SELECT * FROM integers WHERE i=14");
		REQUIRE(CHECK_COLUMN(result, 0, {14, 14}));

		con.AddComment("Testing deletes");
		con.AddComment("Delete non-existing element");
		REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=0"));
		con.AddComment("Now Deleting all elements");
		for (idx_t i = 0; i < n; i++) {
			REQUIRE_NO_FAIL(
			    con.Query("DELETE FROM integers WHERE i=CAST(" + to_string(keys[i]) + " AS " + int_types[idx] + ")"));
			con.AddComment("check the value does not exist");
			result =
			    con.Query("SELECT * FROM integers WHERE i=CAST(" + to_string(keys[i]) + " AS " + int_types[idx] + ")");
			REQUIRE(CHECK_COLUMN(result, 0, {}));
		}
		con.AddComment("Delete from empty tree");
		REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=0"));

		REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
		REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	}
}

TEST_CASE("ART Simple Big Range", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer)"));
	idx_t n = 4;
	auto keys = unique_ptr<int32_t[]>(new int32_t[n + 1]);
	for (idx_t i = 0; i < n + 1; i++) {
		keys[i] = i + 1;
	}

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		for (idx_t j = 0; j < 1500; j++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", keys[i]));
		}
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	result = con.Query("SELECT count(i) FROM integers WHERE i > 1 AND i < 3");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(1500)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i >= 1 AND i < 3");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(3000)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i > 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(4500)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i < 4");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(4500)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i < 5");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(6000)}));
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("ART Big Range with deletions", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	idx_t n = 4;
	auto keys = unique_ptr<int32_t[]>(new int32_t[n + 1]);
	for (idx_t i = 0; i < n + 1; i++) {
		keys[i] = i + 1;
	}

	con.AddComment("now perform a an index creation and scan with deletions with a second transaction");
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer)"));
	for (idx_t j = 0; j < 1500; j++) {
		for (idx_t i = 0; i < n + 1; i++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", keys[i]));
		}
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	con.AddComment("second transaction: begin and verify counts");
	Connection con2(db);
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n + 1; i++) {
		result = con2.Query("SELECT FIRST(i), COUNT(i) FROM integers WHERE i=" + to_string(keys[i]));
		REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(1500)}));
	}
	result = con2.Query("SELECT COUNT(i) FROM integers WHERE i < 10");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(7500)}));

	con.AddComment("now delete entries in the first transaction");
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i = 5"));
	con.AddComment("verify that the counts are still correct in the second transaction");
	for (idx_t i = 0; i < n + 1; i++) {
		result = con2.Query("SELECT FIRST(i), COUNT(i) FROM integers WHERE i=" + to_string(keys[i]));
		REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(1500)}));
	}
	result = con2.Query("SELECT COUNT(i) FROM integers WHERE i < 10");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(7500)}));

	con.AddComment("create an index in the first transaction now");
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	con.AddComment("verify that the counts are still correct for con2");
	for (idx_t i = 0; i < n + 1; i++) {
		result = con2.Query("SELECT FIRST(i), COUNT(i) FROM integers WHERE i=" + to_string(keys[i]));
		REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(1500)}));
	}
	result = con2.Query("SELECT COUNT(i) FROM integers WHERE i<10");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(7500)}));

	con.AddComment("do a bunch of queries in the first transaction");
	result = con.Query("SELECT count(i) FROM integers WHERE i > 1 AND i < 3");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(1500)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i >= 1 AND i < 3");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(3000)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i > 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(4500)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i < 4");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(4500)}));
	result = con.Query("SELECT count(i) FROM integers WHERE i < 5");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(6000)}));

	con.AddComment("verify that the counts are still correct in the second transaction");
	result = con2.Query("SELECT COUNT(i) FROM integers WHERE i<10");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(7500)}));
	result = con2.Query("SELECT COUNT(i) FROM integers WHERE i=5");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(1500)}));
}

TEST_CASE("ART Negative Range", "[art-neg]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer)"));
	idx_t n = 1000;
	auto keys = unique_ptr<int32_t[]>(new int32_t[n]);
	for (idx_t i = 0; i < n; i++) {
		keys[i] = i - 500;
	}

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", keys[i]));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));

	result = con.Query("SELECT sum(i) FROM integers WHERE i >= -500 AND i <= -498");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(-1497)}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i >= -10 AND i <= 5");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(-40)}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i >= 10 AND i <= 15");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(75)}));
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

float generate_small_float() {
	return static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
}

float generate_float(float min_float, float max_float) {
	return min_float + static_cast<float>(rand()) / (static_cast<float>(RAND_MAX / (max_float - min_float)));
}

double generate_small_double() {
	return static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
}

double generate_double(double min_double, double max_double) {
	return min_double + static_cast<double>(rand()) / (static_cast<double>(RAND_MAX / (max_double - min_double)));
}

template <class T> int full_scan(T *keys, idx_t size, T low, T high) {
	int sum = 0;
	for (idx_t i = 0; i < size; i++) {
		if (keys[i] >= low && keys[i] <= high) {
			sum += 1;
		}
	}
	return sum;
}

TEST_CASE("ART Floating Point Small", "[art-float-small]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	vector<int64_t> min_values, max_values;
	Connection con(db);
	con.AddComment("Will use 100 keys");
	idx_t n = 100;
	auto keys = unique_ptr<int64_t[]>(new int64_t[n]);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i BIGINT)"));
	con.AddComment("Generate 10 small floats (0.0 - 1.0)");
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Key::EncodeFloat(generate_small_float());
	}

	con.AddComment("Generate 40 floats (-50/50)");
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Key::EncodeFloat(generate_float(-50, 50));
	}
	con.AddComment("Generate 50 floats (min/max)");
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	con.AddComment("Insert values and create index");
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	con.AddComment("Generate 500 small-small range queries");
	for (idx_t i = 0; i < 5; i++) {
		a = Key::EncodeFloat(generate_small_float());
		b = Key::EncodeFloat(generate_small_float());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 normal-normal range queries");
	for (idx_t i = 0; i < 5; i++) {
		a = Key::EncodeFloat(generate_float(-50, 50));
		b = Key::EncodeFloat(generate_float(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 big-big range queries");
	for (idx_t i = 0; i < 5; i++) {
		a = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		b = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Key::EncodeFloat(min_values[i]);
		int64_t high = Key::EncodeFloat(max_values[i]);
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
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	vector<int64_t> min_values, max_values;
	Connection con(db);
	con.AddComment("Will use 100 keys");
	idx_t n = 100;
	auto keys = unique_ptr<int64_t[]>(new int64_t[n]);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i BIGINT)"));
	con.AddComment("Generate 10 small floats (0.0 - 1.0)");
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Key::EncodeFloat(generate_small_float());
	}

	con.AddComment("Generate 40 floats (-50/50)");
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Key::EncodeFloat(generate_float(-50, 50));
	}
	con.AddComment("Generate 50 floats (min/max)");
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	con.AddComment("Insert values and create index");
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	con.AddComment("Generate 500 small-small range queries");
	for (idx_t i = 0; i < 5; i++) {
		a = Key::EncodeDouble(generate_small_double());
		b = Key::EncodeDouble(generate_small_double());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 normal-normal range queries");
	for (idx_t i = 0; i < 5; i++) {
		a = Key::EncodeDouble(generate_double(-50, 50));
		b = Key::EncodeDouble(generate_double(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 big-big range queries");
	for (idx_t i = 0; i < 5; i++) {
		a = Key::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		b = Key::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Key::EncodeDouble(min_values[i]);
		int64_t high = Key::EncodeDouble(max_values[i]);
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
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	vector<int64_t> min_values, max_values;
	Connection con(db);
	con.AddComment("Will use 10k keys");
	idx_t n = 10000;
	auto keys = unique_ptr<int64_t[]>(new int64_t[n]);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i BIGINT)"));
	con.AddComment("Generate 1000 small floats (0.0 - 1.0)");
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Key::EncodeFloat(generate_small_float());
	}

	con.AddComment("Generate 4000 floats (-50/50)");
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Key::EncodeFloat(generate_float(-50, 50));
	}
	con.AddComment("Generate 5000 floats (min/max)");
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	con.AddComment("Insert values and create index");
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	con.AddComment("Generate 500 small-small range queries");
	for (idx_t i = 0; i < 500; i++) {
		a = Key::EncodeFloat(generate_small_float());
		b = Key::EncodeFloat(generate_small_float());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 normal-normal range queries");
	for (idx_t i = 0; i < 500; i++) {
		a = Key::EncodeFloat(generate_float(-50, 50));
		b = Key::EncodeFloat(generate_float(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 big-big range queries");
	for (idx_t i = 0; i < 500; i++) {
		a = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		b = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Key::EncodeFloat(min_values[i]);
		int64_t high = Key::EncodeFloat(max_values[i]);
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
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	int64_t a, b;
	vector<int64_t> min_values, max_values;
	Connection con(db);
	con.AddComment("Will use 10000 keys");
	idx_t n = 10000;
	auto keys = unique_ptr<int64_t[]>(new int64_t[n]);
	con.Query("CREATE TABLE numbers(i BIGINT)");
	con.AddComment("Generate 1000 small floats (0.0 - 1.0)");
	for (idx_t i = 0; i < n / 10; i++) {
		keys[i] = Key::EncodeFloat(generate_small_float());
	}

	con.AddComment("Generate 4000 floats (-50/50)");
	for (idx_t i = n / 10; i < n / 2; i++) {
		keys[i] = Key::EncodeFloat(generate_float(-50, 50));
	}
	con.AddComment("Generate 5000 floats (min/max)");
	for (idx_t i = n / 2; i < n; i++) {
		keys[i] = Key::EncodeFloat(generate_float(FLT_MIN, FLT_MAX));
	}
	con.AddComment("Insert values and create index");
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (" + to_string(keys[i]) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	con.AddComment("Generate 500 small-small range queries");
	for (idx_t i = 0; i < 500; i++) {
		a = Key::EncodeDouble(generate_small_double());
		b = Key::EncodeDouble(generate_small_double());
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 normal-normal range queries");
	for (idx_t i = 0; i < 500; i++) {
		a = Key::EncodeDouble(generate_double(-50, 50));
		b = Key::EncodeDouble(generate_double(-50, 50));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	con.AddComment("Generate 500 big-big range queries");
	for (idx_t i = 0; i < 500; i++) {
		a = Key::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		b = Key::EncodeDouble(generate_double(FLT_MIN, FLT_MAX));
		min_values.push_back(min(a, b));
		max_values.push_back(max(a, b));
	}
	for (idx_t i = 0; i < min_values.size(); i++) {
		int64_t low = Key::EncodeDouble(min_values[i]);
		int64_t high = Key::EncodeDouble(max_values[i]);
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
