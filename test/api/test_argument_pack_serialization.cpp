#include "catch.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/planner.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

unique_ptr<LogicalOperator> PlanQuery(Connection &con, const string &query) {
	Parser parser;
	parser.ParseQuery(query);
	Planner planner(*con.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	return std::move(planner.plan);
}

//! Serialize a plan at the given storage version, returning the bytes it was written as
vector<data_t> Serialize(LogicalOperator &plan, const StorageCompatibility &compatibility) {
	MemoryStream stream(Allocator::DefaultAllocator());

	SerializationOptions options;
	options.storage_compatibility = compatibility;
	BinarySerializer serializer(stream, options);
	serializer.Begin();
	plan.Serialize(serializer);
	serializer.End();

	vector<data_t> result(stream.GetPosition());
	memcpy(result.data(), stream.GetData(), stream.GetPosition());
	return result;
}

unique_ptr<LogicalOperator> Deserialize(Connection &con, const vector<data_t> &bytes) {
	MemoryStream stream(Allocator::DefaultAllocator());
	stream.WriteData(bytes.data(), bytes.size());
	stream.Rewind();

	BinaryDeserializer deserializer(stream);
	deserializer.Set<ClientContext &>(*con.context);
	deserializer.Begin();
	auto result = LogicalOperator::Deserialize(deserializer);
	deserializer.End();
	result->ResolveOperatorTypes();
	return result;
}

//! The "hash" function takes (value ANY, *values ANY), so a call to it always has exactly two arguments once bound:
//! the first value and the pack holding the rest.
void RequirePackedHashCall(LogicalOperator &plan) {
	auto &expr = *plan.expressions[0];
	REQUIRE(expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION);
	auto &function = expr.Cast<BoundFunctionExpression>();
	REQUIRE(function.Function().GetName() == "hash");
	REQUIRE(function.GetChildren().size() == 2);
	REQUIRE(ArgumentPack::IsPackType(function.GetChildren()[1]->GetReturnType()));
}

} // namespace

TEST_CASE("Argument packs survive plan serialization", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();

	const string query = "SELECT hash(i, i + 1, 'x') FROM range(3) t(i)";
	auto expected = con.Query(query);
	REQUIRE_NO_FAIL(*expected);

	auto plan = PlanQuery(con, query);
	RequirePackedHashCall(*plan);

	auto latest_bytes = Serialize(*plan, StorageCompatibility::Latest());
	auto legacy_bytes = Serialize(*plan, StorageCompatibility::FromString("v1.2.0"));

	// a storage version that predates argument packs gets a different encoding of the same call: the pack is
	// unrolled into the plain argument list the call was written with
	REQUIRE(latest_bytes != legacy_bytes);

	SECTION("current storage version keeps the call packed") {
		auto deserialized = Deserialize(con, latest_bytes);
		RequirePackedHashCall(*deserialized);

		auto result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized)), false);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Equals(*expected, false));
	}

	SECTION("an unrolled call binds its trailing arguments back into a pack") {
		auto deserialized = Deserialize(con, legacy_bytes);

		// this is also what happens when a plan written before argument packs existed is read back
		RequirePackedHashCall(*deserialized);

		auto result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized)), false);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Equals(*expected, false));
	}

	con.Rollback();
}
