#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

using namespace duckdb;

namespace {

unique_ptr<QueryResult> RunSingletonQuery(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	result->Complete();
	result->ThrowIfError();
	return result;
}

const string singleton_join =
    "SELECT count(*), sum(l.z + r.z) "
    "FROM (SELECT first(x ORDER BY id) k, max(id) z FROM singleton_left GROUP BY id::BIGINT) l "
    "JOIN (SELECT first(x ORDER BY id) k, max(id) z FROM singleton_right GROUP BY id::BIGINT) r USING (k)";

idx_t SingletonJoinEstimate(Connection &connection) {
	auto result = RunSingletonQuery(connection, "EXPLAIN (FORMAT JSON) " + singleton_join);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	auto plan = chunk->GetValue(1, 0).ToString();
	INFO(plan);
	auto join = plan.find("\"Join Type\"");
	REQUIRE(join != string::npos);
	const string prefix = "\"Estimated Cardinality\": \"";
	auto cardinality = plan.find(prefix, join);
	REQUIRE(cardinality != string::npos);
	cardinality += prefix.size();
	auto end = plan.find('"', cardinality);
	REQUIRE(end != string::npos);
	return std::stoull(plan.substr(cardinality, end - cardinality));
}

bool SingletonCollapsingCast(Vector &source, Vector &result, idx_t count, CastParameters &) {
	UnaryExecutor::Execute<int32_t, int64_t>(source, result, count, [](int32_t) { return int64_t(0); });
	return true;
}

void CheckSingletonResult(Connection &connection, const string &count) {
	auto result = RunSingletonQuery(connection, singleton_join);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	CHECK(chunk->GetValue(0, 0).ToString() == count);
	CHECK(chunk->GetValue(1, 0).ToString() == "4");
}

} // namespace

TEST_CASE("Singleton aggregate domains reject overridden casts", "[api][singleton_group_statistics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	for (auto &sql : {"SET threads=1", "SET explain_output='optimized_only'",
	                  "CREATE TABLE singleton_left(id INTEGER PRIMARY KEY, x INTEGER)",
	                  "CREATE TABLE singleton_right(id INTEGER PRIMARY KEY, x INTEGER)",
	                  "INSERT INTO singleton_left VALUES (0, 0), (2, 1)",
	                  "INSERT INTO singleton_right SELECT * FROM singleton_left", "ANALYZE"}) {
		RunSingletonQuery(connection, sql);
	}
	REQUIRE(SingletonJoinEstimate(connection) == 2);
	CheckSingletonResult(connection, "2");

	CastFunctionSet::Get(*connection.context)
	    .RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT, BoundCastInfo(SingletonCollapsingCast));
	REQUIRE(SingletonJoinEstimate(connection) == 1);
	CheckSingletonResult(connection, "1");
}
