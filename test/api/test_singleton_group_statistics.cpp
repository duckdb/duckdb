#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

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

optional_ptr<const BoundAggregateExpression> FindSingletonAggregate(const LogicalOperator &op) {
	for (auto &expression : op.expressions) {
		if (expression->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE) {
			return expression->Cast<BoundAggregateExpression>();
		}
	}
	for (auto &child : op.children) {
		auto aggregate = FindSingletonAggregate(*child);
		if (aggregate) {
			return aggregate;
		}
	}
	return nullptr;
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

TEST_CASE("Ordered singleton aggregate identity survives serialization",
          "[api][serialization][singleton_group_statistics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	RunSingletonQuery(connection, "CREATE TABLE roundtrip_keys(id INTEGER PRIMARY KEY, x INTEGER, p INTEGER)");
	RunSingletonQuery(connection, "INSERT INTO roundtrip_keys VALUES (0, 0, NULL), (1, 1, 3)");
	connection.BeginTransaction();

	const pair<string, bool> cases[] = {{"first(x ORDER BY p)", true},     {"last(x ORDER BY p)", true},
	                                    {"any_value(x ORDER BY p)", true}, {"arg_min(x, p)", false},
	                                    {"arg_min_null(x, p)", false},     {"arg_max_null(x, p)", false}};
	for (const auto &entry : cases) {
		CAPTURE(entry.first);
		auto plan = connection.ExtractPlan("SELECT " + entry.first + " FROM roundtrip_keys GROUP BY id");
		auto aggregate = FindSingletonAggregate(*plan);
		REQUIRE(aggregate);
		REQUIRE(aggregate->Function().HasSingleValueIdentity() == entry.second);
		for (const auto &compatibility :
		     {StorageCompatibility::Latest(), StorageCompatibility::FromIndex(StorageVersion::V2_0_0)}) {
			const bool preserves_identity = compatibility.Compare(StorageVersion::V2_1_0);
			CAPTURE(preserves_identity);
			MemoryStream stream;
			SerializationOptions options;
			options.storage_compatibility = compatibility;
			BinarySerializer::Serialize(*aggregate, stream, options);
			stream.Rewind();
			bound_parameter_map_t parameters;
			auto copy = BinaryDeserializer::Deserialize<Expression>(stream, *connection.context, parameters);
			REQUIRE(copy->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &rebound = copy->Cast<BoundAggregateExpression>();
			CHECK(rebound.Function().GetName() == aggregate->Function().GetName());
			CHECK(rebound.Function().HasSingleValueIdentity() == (entry.second && preserves_identity));
		}
	}
	connection.Rollback();
}
