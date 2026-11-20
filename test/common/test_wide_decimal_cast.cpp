#include "catch.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("Wide decimal results survive reuse of the same cast state", "[decimal]") {
	const auto source_type = LogicalType::DECIMAL(1, 0);
	const auto target_type = LogicalType::DECIMAL(40, 38);
	CastFunctionSet functions;
	auto input = GetCastFunctionInput::Default();
	auto cast = functions.GetCastFunction(source_type, target_type, input);
	REQUIRE(cast.HasInitLocalState());
	CastLocalStateParameters local_parameters(nullptr, cast.GetCastData());
	auto local_state = cast.InitLocalState(local_parameters);
	CastParameters parameters(cast.GetCastData(), false, nullptr, local_state.get());
	Vector source(source_type), first(target_type), next(target_type);
	const duckdb::vector<Value> values {Value("1"),  Value("2"), Value("-1"),
	                                    Value("-2"), Value("0"), Value(LogicalType::VARCHAR)};
	const auto count = MinValue<idx_t>(values.size(), STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		source.SetValue(i, values[i].DefaultCastAs(source_type));
	}
	FlatVector::SetSize(source, count);
	REQUIRE(cast.Cast(source, first, count, parameters));
	for (idx_t batch = 1; batch < 4; batch++) {
		for (idx_t i = 0; i < count; i++) {
			source.SetValue(i, values[(i + batch) % values.size()].DefaultCastAs(source_type));
		}
		REQUIRE(cast.Cast(source, next, count, parameters));
		for (idx_t i = 0; i < count; i++) {
			REQUIRE(Value::NotDistinctFrom(first.GetValue(i), values[i].DefaultCastAs(target_type)));
		}
	}
}
