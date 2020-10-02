#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include <random>
#include <vector>
#include <cfloat>

using namespace duckdb;
using namespace std;


TEST_CASE("Test Inner Join with Art Index", "[art][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2(i INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON t1 using art(i)"));
	vector<int32_t> data;
	data.reserve(100);
    for (int32_t val = 0; val < 100; val++) {
		data.push_back(val);
	}
	shuffle(data.begin(),data.end(), std::mt19937(std::random_device()()));
	for (auto& d: data){
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES ($1)", d));
	}
	shuffle(data.begin(),data.end(), std::mt19937(std::random_device()()));
	for (auto& d: data){
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t2 VALUES ($1)", d));
	}

	con.Query("EXPLAIN select sum(t1.i) from t1 inner join t2 on (t1.i = t2.i)")->Print();

}