#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

#include <cfloat>
#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test ART index creation with many versions", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection r1(db), r2(db), r3(db);
	int64_t expected_sum_r1 = 0, expected_sum_r2 = 0, expected_sum_r3 = 0, total_sum = 0;

	con.AddComment("insert the values [0...20000]");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	for (idx_t i = 0; i < 20000; i++) {
		int32_t val = i + 1;
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", val));
		expected_sum_r1 += val;
		expected_sum_r2 += val + 1;
		expected_sum_r3 += val + 2;
		total_sum += val + 3;
	}
	con.AddComment("now start a transaction in r1");
	REQUIRE_NO_FAIL(r1.Query("BEGIN TRANSACTION"));
	con.AddComment("increment values by 1");
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=i+1"));
	con.AddComment("now start a transaction in r2");
	REQUIRE_NO_FAIL(r2.Query("BEGIN TRANSACTION"));
	con.AddComment("increment values by 1 again");
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=i+1"));
	con.AddComment("now start a transaction in r3");
	REQUIRE_NO_FAIL(r3.Query("BEGIN TRANSACTION"));
	con.AddComment("increment values by 1 again");
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=i+1"));
	con.AddComment("create an index, this fails because we have outstanding updates");
	REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	con.AddComment("r1");
	result = r1.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_r1)}));
	result = r1.Query("SELECT SUM(i) FROM integers WHERE i > 0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_r1)}));
	con.AddComment("r2");
	result = r2.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_r2)}));
	result = r2.Query("SELECT SUM(i) FROM integers WHERE i > 0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_r2)}));
	con.AddComment("r3");
	result = r3.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_r3)}));
	result = r3.Query("SELECT SUM(i) FROM integers WHERE i > 0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_r3)}));
	con.AddComment("total sum");
	result = con.Query("SELECT SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(total_sum)}));
	result = con.Query("SELECT SUM(i) FROM integers WHERE i > 0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(total_sum)}));
}

TEST_CASE("Test ART index with many matches", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	for (idx_t i = 0; i < 1024; i++) {
		for (idx_t val = 0; val < 2; val++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", (int32_t)val));
		}
	}
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i<1");
	REQUIRE(CHECK_COLUMN(result, 0, {1024}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i<=1");
	REQUIRE(CHECK_COLUMN(result, 0, {2048}));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=0");
	REQUIRE(CHECK_COLUMN(result, 0, {1024}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1024}));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i>0");
	REQUIRE(CHECK_COLUMN(result, 0, {1024}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i>=0");
	REQUIRE(CHECK_COLUMN(result, 0, {2048}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	for (idx_t i = 0; i < 2048; i++) {
		for (idx_t val = 0; val < 2; val++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", (int32_t)val));
		}
	}

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i<1");
	REQUIRE(CHECK_COLUMN(result, 0, {2048}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i<=1");
	REQUIRE(CHECK_COLUMN(result, 0, {4096}));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=0");
	REQUIRE(CHECK_COLUMN(result, 0, {2048}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {2048}));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i>0");
	REQUIRE(CHECK_COLUMN(result, 0, {2048}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i>=0");
	REQUIRE(CHECK_COLUMN(result, 0, {4096}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Test ART index with non-linear insertion", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));
	idx_t count = 0;
	for (int32_t it = 0; it < 10; it++) {
		for (int32_t val = 0; val < 1000; val++) {
			if (it + val % 2) {
				count++;
				REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", val));
			}
		}
	}
	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(count)}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i < 1000000");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(count)}));
}

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

TEST_CASE("Test ART index with negative values and big values", "[art]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i BIGINT)"));
	vector<int64_t> values = {-4611686018427387906, -4611686018427387904, -2305843009213693952, 0,
	                          2305843009213693952,  4611686018427387904,  4611686018427387906};
	for (auto val : values) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", val));
	}

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers using art(i)"));

	result = con.Query("SELECT COUNT(*) FROM integers WHERE i > $1", 0);
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i < $1", 0);
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i < $1", (int64_t)4611686018427387906);
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("SELECT COUNT(*) FROM integers WHERE i <= $1", (int64_t)4611686018427387906);
	REQUIRE(CHECK_COLUMN(result, 0, {7}));
}

TEST_CASE("Test Drop Index", "[drop-index]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE A (A1 INTEGER,A2 VARCHAR, A3 INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO A VALUES (1, 1, 1)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO A VALUES (2, 2, 2)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE B (B1 INTEGER,B2 INTEGER, B3 INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO B VALUES (1, 1, 1)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO B VALUES (2, 2, 2)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE C (C1 VARCHAR, C2 INTEGER, C3 INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO C VALUES ('t1', 1, 1)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO C VALUES ('t2', 2, 2)"));

	REQUIRE_NO_FAIL(con.Query("SELECT A2 FROM A WHERE A1=1"));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX A_index ON A (A1)"));
	REQUIRE_NO_FAIL(con.Query("SELECT A2 FROM A WHERE A1=1"));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX B_index ON B (B1)"));
	REQUIRE_NO_FAIL(con.Query("SELECT A2 FROM A WHERE A1=1"));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX C_index ON C (C2)"));
	REQUIRE_NO_FAIL(con.Query("SELECT A2 FROM A WHERE A1=1"));

	REQUIRE_NO_FAIL(con.Query("DROP INDEX IF EXISTS A_index"));
	REQUIRE_NO_FAIL(con.Query("SELECT A2 FROM A WHERE A1=1"));
}

TEST_CASE("Test ART with different Integer Types", "[art]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i TINYINT, j SMALLINT, k INTEGER, l BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index1 ON integers(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index2 ON integers(j)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index3 ON integers(k)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index4 ON integers(l)"));

	con.AddComment("query the empty indices first");
	result = con.Query("SELECT i FROM integers WHERE i > 0");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT j FROM integers WHERE j < 0");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT k FROM integers WHERE k >= 0");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT l FROM integers WHERE l <= 0");
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	con.AddComment("now insert the values [1..5] in all columns");
	auto prepare = con.Prepare("INSERT INTO integers VALUES ($1, $2, $3, $4)");
	for (int32_t i = 1; i <= 5; i++) {
		REQUIRE_NO_FAIL(prepare->Execute(i, i, i, i));
	}
	prepare.reset();

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, 3, 4, 5}));
	REQUIRE(CHECK_COLUMN(result, 3, {1, 2, 3, 4, 5}));

	result = con.Query("SELECT i FROM integers WHERE i > 0::TINYINT ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	result = con.Query("SELECT j FROM integers WHERE j <= 2::SMALLINT ORDER BY j");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	result = con.Query("SELECT k FROM integers WHERE k >= -100000::INTEGER ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	result = con.Query("SELECT k FROM integers WHERE k >= 100000::INTEGER ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT k FROM integers WHERE k >= 100000::INTEGER AND k <= 100001::INTEGER ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	result = con.Query("SELECT l FROM integers WHERE l <= 1000000000::BIGINT ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5}));
	result = con.Query("SELECT l FROM integers WHERE l <= -1000000000::BIGINT ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
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

TEST_CASE("ART Strings", "[art-string]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(i varchar)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON strings(i)"));

	con.AddComment("Insert values and create index");
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('test')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('test1')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('vest1')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('somesuperbigstring')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('somesuperbigstring1')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('somesuperbigstring2')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('somesuperbigstring')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('maybesomesuperbigstring')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES "
	                          "('"
	                          "maybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigst"
	                          "ringmaybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstring')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES "
	                          "('"
	                          "maybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigst"
	                          "ringmaybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstring2')"));

	result = con.Query("SELECT COUNT(i) FROM strings WHERE i = 'test'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT COUNT(i) FROM strings WHERE i = 'somesuperbigstring'");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT COUNT(i) FROM strings WHERE i = "
	                   "'maybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstringma"
	                   "ybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstring'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT COUNT(i) FROM strings WHERE i = "
	                   "'maybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstringma"
	                   "ybesomesuperbigstringmaybesomesuperbigstringmaybesomesuperbigstring2'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT COUNT(i) FROM strings WHERE i >= 'somesuperbigstring' and i <='somesuperbigstringz'");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE strings"));
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

TEST_CASE("ART FP Unique Constraint", "[art-float-unique]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i REAL PRIMARY KEY, j INTEGER)"));

	con.AddComment("insert two conflicting pairs at the same time");
	REQUIRE_FAIL(con.Query("INSERT INTO numbers VALUES (3.45, 4), (3.45, 5)"));

	con.AddComment("insert unique values");
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (3.45, 4), (2.2, 5)"));

	result = con.Query("SELECT * FROM numbers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::FLOAT(3.45f), Value::FLOAT(2.2f)}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5}));

	con.AddComment("insert a duplicate value as part of a chain of values");
	REQUIRE_FAIL(con.Query("INSERT INTO numbers VALUES (6, 6), (3.45, 4);"));

	con.AddComment("now insert just the first value");
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (6, 6);"));

	result = con.Query("SELECT * FROM numbers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::FLOAT(3.45f), Value::FLOAT(2.2f), Value::FLOAT(6.0f)}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6}));

	con.AddComment("insert NULL value in PRIMARY KEY is not allowed");
	REQUIRE_FAIL(con.Query("INSERT INTO numbers VALUES (NULL, 4);"));

	con.AddComment("update NULL is also not allowed");
	REQUIRE_FAIL(con.Query("UPDATE numbers SET i=NULL;"));
}

TEST_CASE("ART FP Special Cases", "[art-fp-special]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i REAL)"));
	con.AddComment("+0");
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (CAST(0 AS REAL))"));
	con.AddComment("-0");
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (CAST(-0 AS REAL))"));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	con.AddComment("+0");
	result = con.Query("SELECT COUNT(i) FROM numbers WHERE i = CAST(0 AS REAL)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	con.AddComment("-0");
	result = con.Query("SELECT COUNT(i) FROM numbers WHERE i = CAST(-0 AS REAL)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}

TEST_CASE("ART Double Special Cases", "[art-double-special]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(i DOUBLE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (CAST(0 AS DOUBLE))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (CAST(-0 AS DOUBLE))"));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON numbers(i)"));
	result = con.Query("SELECT COUNT(i) FROM numbers WHERE i = CAST(0 AS DOUBLE)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT COUNT(i) FROM numbers WHERE i = CAST(-0 AS DOUBLE)");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
}
TEST_CASE("Test updates resulting from big index scans", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	int64_t sum = 0;
	int64_t count = 0;

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	for (idx_t i = 0; i < 25000; i++) {
		int32_t value = i + 1;

		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", value));

		sum += value;
		count++;
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	con.AddComment("check the sum and the count");
	result = con.Query("SELECT SUM(i), COUNT(i) FROM integers WHERE i>0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count)}));

	con.AddComment("update the data with an index scan");
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=i+1 WHERE i>0"));
	sum += count;

	con.AddComment("now check the sum and the count again");
	result = con.Query("SELECT SUM(i), COUNT(i) FROM integers WHERE i>0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(sum)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(count)}));

	con.AddComment("now delete from the table with an index scan");
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i>0"));

	result = con.Query("SELECT SUM(i), COUNT(i) FROM integers WHERE i>0");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(0)}));
}

TEST_CASE("ART Node 4", "[art]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer)"));
	idx_t n = 4;
	auto keys = unique_ptr<int32_t[]>(new int32_t[n]);
	for (idx_t i = 0; i < n; i++) {
		keys[i] = i + 1;
	}

	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", keys[i]));
	}

	for (idx_t i = 0; i < n; i++) {
		result = con.Query("SELECT i FROM integers WHERE i=$1", keys[i]);
		REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	result = con.Query("SELECT sum(i) FROM integers WHERE i <= 2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(3)}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i > 1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(2 + 3 + 4)}));
	con.AddComment("Now Deleting all elements");
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=$1", keys[i]));
	}
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i = 0"));
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("ART Node 16", "[art]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer)"));
	idx_t n = 6;
	auto keys = unique_ptr<int32_t[]>(new int32_t[n]);
	for (idx_t i = 0; i < n; i++) {
		keys[i] = i + 1;
	}

	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", keys[i]));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	for (idx_t i = 0; i < n; i++) {
		result = con.Query("SELECT i FROM integers WHERE i=$1", keys[i]);
		REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
	}
	result = con.Query("SELECT sum(i) FROM integers WHERE i <=2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(3)}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i > 4");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(5 + 6)}));
	con.AddComment("Now Deleting all elements");
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=$1", keys[i]));
	}
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("ART Node 48", "[art]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer)"));
	idx_t n = 20;
	auto keys = unique_ptr<int32_t[]>(new int32_t[n]);
	for (idx_t i = 0; i < n; i++) {
		keys[i] = i + 1;
	}
	int64_t expected_sum = 0;
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", keys[i]));
		expected_sum += keys[i];
	}
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i)"));
	for (idx_t i = 0; i < n; i++) {
		result = con.Query("SELECT i FROM integers WHERE i=$1", keys[i]);
		REQUIRE(CHECK_COLUMN(result, 0, {Value(keys[i])}));
	}
	result = con.Query("SELECT sum(i) FROM integers WHERE i <=2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(3)}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i > 15");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(16 + 17 + 18 + 19 + 20)}));

	con.AddComment("delete an element and reinsert it");
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=16"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (16)"));

	con.AddComment("query again");
	result = con.Query("SELECT sum(i) FROM integers WHERE i <=2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(3)}));
	result = con.Query("SELECT sum(i) FROM integers WHERE i > 15");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(16 + 17 + 18 + 19 + 20)}));

	con.AddComment("Now delete all elements");
	for (idx_t i = 0; i < n; i++) {
		REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=$1", keys[i]));
		expected_sum -= keys[i];
		con.AddComment("verify the sum");
		result = con.Query("SELECT sum(i) FROM integers WHERE i > 0");
		REQUIRE(CHECK_COLUMN(result, 0, {expected_sum == 0 ? Value() : Value::BIGINT(expected_sum)}));
	}
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("Index Exceptions", "[art]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i integer, j integer, k BOOLEAN)"));

	REQUIRE_FAIL(con.Query("CREATE INDEX ON integers(i)"));
	REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON integers(i COLLATE \"de_DE\")"));
	REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON integers using blabla(i)"));
	REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON integers(f)"));
}

TEST_CASE("More Index Exceptions (#496)", "[art]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);

	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOLEAN, c1 INT)"));
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i0 ON t0(c1, c0)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1) VALUES (0)"));
	auto res = con.Query("SELECT * FROM t0");
	REQUIRE(res->success);

	REQUIRE(CHECK_COLUMN(res, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(res, 1, {0}));
}
