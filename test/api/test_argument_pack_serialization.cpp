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

//! "struct_pack" takes (**fields ANY), so a bound call always has exactly one argument: the keyword pack, whose
//! field names are the names the arguments were passed under
void RequirePackedStructPackCall(LogicalOperator &plan) {
	auto &expr = *plan.expressions[0];
	REQUIRE(expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION);
	auto &function = expr.Cast<BoundFunctionExpression>();
	REQUIRE(function.Function().GetName() == "struct_pack");
	REQUIRE(function.GetChildren().size() == 1);

	auto &pack_type = function.GetChildren()[0]->GetReturnType();
	REQUIRE(ArgumentPack::IsPackType(pack_type));
	REQUIRE(StructType::GetChildCount(pack_type) == 2);
	REQUIRE(StructType::GetChildName(pack_type, 0) == "a");
	REQUIRE(StructType::GetChildName(pack_type, 1) == "b");
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

	SECTION("the current storage version writes the call unrolled and reads it back packed") {
		auto deserialized = Deserialize(con, latest_bytes);
		RequirePackedHashCall(*deserialized);

		auto result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized)), false);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Equals(*expected, false));
	}

	SECTION("so does a storage version that predates argument packs") {
		auto deserialized = Deserialize(con, legacy_bytes);

		// this is also what happens when a plan written before argument packs existed is read back
		RequirePackedHashCall(*deserialized);

		auto result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized)), false);
		REQUIRE_NO_FAIL(*result);
		fprintf(stderr, "GOT:\n%s\nEXPECTED:\n%s\n", result->ToString().c_str(), expected->ToString().c_str());
		REQUIRE(result->Equals(*expected, false));
	}

	con.Rollback();
}

TEST_CASE("Keyword argument packs survive plan serialization", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();

	const string query = "SELECT struct_pack(a := i, b := i + 1) FROM range(3) t(i)";
	auto plan = PlanQuery(con, query);
	RequirePackedStructPackCall(*plan);

	// the keyword pack is unrolled with its field names in the argument aliases, which is how a call to a function
	// that derived its names from argument aliases looked before argument packs existed
	for (auto &compatibility : {StorageCompatibility::Latest(), StorageCompatibility::FromString("v1.2.0")}) {
		auto expected = con.Query(query);
		REQUIRE_NO_FAIL(*expected);

		auto deserialized = Deserialize(con, Serialize(*plan, compatibility));
		RequirePackedStructPackCall(*deserialized);

		auto result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized)), false);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Equals(*expected, false));
	}

	con.Rollback();
}
