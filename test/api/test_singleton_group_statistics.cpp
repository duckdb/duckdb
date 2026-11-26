#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/distinctness.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/planner.hpp"

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

void RegisterPayloadAggregate(Connection &connection, const Identifier &source) {
	auto &context = *connection.context;
	context.RunFunctionInTransaction([&]() {
		auto &entry =
		    Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, QualifiedName("system", "main", source));
		auto functions = entry.functions;
		functions.SetName("test_singleton_payload");
		functions.ApplyToFunctions([](AggregateFunction &function) { function.SetName("test_singleton_payload"); });
		CreateAggregateFunctionInfo info(std::move(functions));
		Catalog::GetSystemCatalog(context).CreateFunction(context, info);
	});
}

} // namespace

TEST_CASE("Distinctness forwards primary keys through Top N", "[api][singleton_group_statistics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	RunSingletonQuery(connection, "CREATE TABLE singleton_keys(id INTEGER PRIMARY KEY)");
	connection.BeginTransaction();

	Parser parser(*connection.context);
	parser.ParseQuery("SELECT id, id AS alias_id FROM singleton_keys");
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	const auto bindings = planner.plan->GetColumnBindings();
	REQUIRE(bindings.size() == 2);

	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, bindings[0]));
	LogicalTopN top_n(std::move(orders), 1, 0);
	top_n.children.push_back(std::move(planner.plan));

	Distinctness distinctness(DistinctnessPropagation::UNARY_OPERATORS);
	column_binding_set_t primary_key {bindings[0]};
	CHECK(distinctness.IsDistinctOn(top_n, primary_key));

	vector<unique_ptr<Expression>> aliased_key;
	aliased_key.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, bindings[1]));
	CHECK(distinctness.IsDistinctOn(top_n, aliased_key));
	connection.Rollback();
}

TEST_CASE("Distinctness uses the same cast proof for projected keys", "[api][singleton_group_statistics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	RunSingletonQuery(connection, "CREATE TABLE projection_keys(id INTEGER PRIMARY KEY)");

	for (bool registered_override : {false, true}) {
		CAPTURE(registered_override);
		if (registered_override) {
			CastFunctionSet::Get(*connection.context)
			    .RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT,
			                          BoundCastInfo(SingletonCollapsingCast));
		}
		connection.BeginTransaction();
		Parser parser(*connection.context);
		parser.ParseQuery("SELECT id::BIGINT AS first_key, id::BIGINT AS second_key, id AS plain_key, "
		                  "TRY_CAST(id AS TINYINT) AS try_key FROM projection_keys");
		Planner planner(*connection.context);
		planner.CreatePlan(std::move(parser.statements[0]));
		auto &projection = *planner.plan;
		const auto bindings = projection.GetColumnBindings();
		REQUIRE(bindings.size() == 4);

		const column_binding_set_t first_key {bindings[0]};
		const column_binding_set_t plain_key {bindings[2]};
		const column_binding_set_t try_key {bindings[3]};
		vector<unique_ptr<Expression>> second_key;
		second_key.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, bindings[1]));

		Distinctness without_context(DistinctnessPropagation::UNARY_OPERATORS);
		CHECK_FALSE(without_context.IsDistinctOn(projection, first_key));
		CHECK_FALSE(without_context.IsDistinctOn(projection, second_key));
		CHECK(without_context.IsDistinctOn(projection, plain_key));

		Distinctness with_context(DistinctnessPropagation::UNARY_OPERATORS, *connection.context);
		CHECK(with_context.GetUniqueSets(projection).size() == 1);
		CHECK(with_context.IsDistinctOn(projection, first_key) == !registered_override);
		CHECK(with_context.IsDistinctOn(projection, second_key) == !registered_override);
		CHECK_FALSE(with_context.IsDistinctOn(projection, try_key));
		connection.Rollback();
	}
}

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

TEST_CASE("Ordered singleton aggregate domain proof survives rebinding",
          "[api][serialization][singleton_group_statistics]") {
	DuckDB database(nullptr);
	Connection connection(database);
	RunSingletonQuery(connection, "CREATE TABLE roundtrip_keys(id INTEGER PRIMARY KEY, x INTEGER, p INTEGER, "
	                              "items INTEGER[], amount DECIMAL(12,2))");
	RunSingletonQuery(connection, "INSERT INTO roundtrip_keys VALUES (0, 0, NULL, [0], 1.23), (1, 1, 3, [1], 4.56)");
	connection.BeginTransaction();

	const pair<string, bool> cases[] = {{"first(x ORDER BY p)", true},     {"last(x ORDER BY p)", true},
	                                    {"any_value(x ORDER BY p)", true}, {"arg_min(x, p)", false},
	                                    {"arg_min_null(x, p)", false},     {"arg_max_null(x, p)", false},
	                                    {"first(items ORDER BY p)", true}, {"first(amount ORDER BY p)", true}};
	for (const auto &entry : cases) {
		CAPTURE(entry.first);
		auto plan = TestExtractPlan(connection, "SELECT " + entry.first + " FROM roundtrip_keys GROUP BY id");
		auto aggregate = FindSingletonAggregate(*plan);
		REQUIRE(aggregate);
		REQUIRE(aggregate->CanForwardSingletonInputDomain() == entry.second);
		CHECK_FALSE(aggregate->Function().HasSingleValueIdentity());
		for (const auto &compatibility :
		     {StorageCompatibility::Latest(), StorageCompatibility::FromIndex(StorageVersion::V2_0_0)}) {
			CAPTURE(compatibility.duckdb_version);
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
			CHECK(rebound.CanForwardSingletonInputDomain() == entry.second);
			CHECK_FALSE(rebound.Function().HasSingleValueIdentity());
		}
	}
	connection.Rollback();
}

TEST_CASE("Rebound recursive payload aggregates use the reader's singleton identity",
          "[api][serialization][singleton_group_statistics]") {
	DuckDB writer_database(nullptr);
	DuckDB reader_database(nullptr);
	Connection writer(writer_database);
	Connection reader(reader_database);
	RegisterPayloadAggregate(writer, "max");
	RegisterPayloadAggregate(reader, "count");

	const string sql = "WITH RECURSIVE t(k, v) USING KEY(k, test_singleton_payload(v)) AS ("
	                   "SELECT 0, 11::BIGINT UNION SELECT k + 1, v + 9 FROM t WHERE k < 2"
	                   ") SELECT list(v ORDER BY k) FROM t";
	auto original = RunSingletonQuery(writer, sql)->Fetch();
	REQUIRE(original);
	CHECK(original->GetValue(0, 0).ToString() == "[11, 20, 29]");
	auto fresh = RunSingletonQuery(reader, sql)->Fetch();
	REQUIRE(fresh);
	REQUIRE(fresh->GetValue(0, 0).ToString() == "[1, 1, 1]");

	writer.BeginTransaction();
	reader.BeginTransaction();
	Parser parser(*writer.context);
	parser.ParseQuery(sql);
	Planner planner(*writer.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	MemoryStream stream;
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::Latest();
	BinarySerializer::Serialize(*planner.plan, stream, options);
	stream.Rewind();
	bound_parameter_map_t parameters;
	auto rebound = BinaryDeserializer::Deserialize<LogicalOperator>(stream, *reader.context, parameters);
	rebound->ResolveOperatorTypes();
	auto result = reader.Query(make_uniq<LogicalPlanStatement>(std::move(rebound)));
	result->Complete();
	result->ThrowIfError();
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	CHECK(chunk->GetValue(0, 0) == fresh->GetValue(0, 0));
	reader.Rollback();
	writer.Rollback();
}
