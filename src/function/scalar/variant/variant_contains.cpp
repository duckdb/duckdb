#include "duckdb/common/radix.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/types/variant_comparison.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

//! Deepest nesting the comparison follows before it gives up, so a deeply nested VARIANT cannot overflow the stack
static constexpr idx_t MAX_VARIANT_DEPTH = 1000;

static void CheckDepth(idx_t depth) {
	if (depth > MAX_VARIANT_DEPTH) {
		throw InvalidInputException("VARIANT is nested more than %llu levels deep, which is more than variant_contains "
		                            "supports",
		                            MAX_VARIANT_DEPTH);
	}
}

static bool IsEqual(const VariantNode &haystack, const VariantNode &needle, idx_t depth);

static bool IsObjectEqual(const VariantNode &haystack, const VariantNode &needle, const idx_t depth) {
	// for every needle we need to find an occurrence in the haystack
	// elements in the haystack are not consumed, so multiple needles can be satisfied by one haystack element
	for (const auto &needle_child : needle.GetObjectChildren()) {
		auto found = false;
		for (const auto &haystack_child : haystack.GetObjectChildren()) {
			if (needle_child.key != haystack_child.key) {
				continue;
			}
			if (IsEqual(haystack_child.value, needle_child.value, depth + 1)) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

static bool IsArrayEqual(const VariantNode &haystack, const VariantNode &needle, const idx_t depth) {
	for (const auto &needle_child : needle.GetArrayChildren()) {
		auto found = false;
		for (const auto &haystack_child : haystack.GetArrayChildren()) {
			if (IsEqual(haystack_child, needle_child, depth + 1)) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

static bool IsEqual(const VariantNode &haystack, const VariantNode &needle, const idx_t depth) {
	CheckDepth(depth);
	const auto haystack_type = haystack.GetTypeId();
	const auto needle_type = needle.GetTypeId();
	const auto haystack_category = GetVariantComparisonType(haystack_type);
	if (haystack_category != GetVariantComparisonType(needle_type)) {
		return false;
	}

	switch (haystack_category) {
	case VariantComparisonType::ARRAY:
		return IsArrayEqual(haystack, needle, depth);
	case VariantComparisonType::OBJECT:
		return IsObjectEqual(haystack, needle, depth);
	case VariantComparisonType::NULL_VALUE:
		return true;
	case VariantComparisonType::BOOLEAN:
		return haystack.GetTypeId() == needle.GetTypeId();
	case VariantComparisonType::UUID:
		return haystack.GetData<hugeint_t>() == needle.GetData<hugeint_t>();
	case VariantComparisonType::REAL:
		// we need to encode so NaN == NaN, -0.0 == 0.0, and infinity == infinity
		return Radix::EncodeDouble(VariantGetRealValue(haystack_type, haystack)) ==
		       Radix::EncodeDouble(VariantGetRealValue(needle_type, needle));
	case VariantComparisonType::NUMBER:
		return VariantGetNumberKey(haystack_type, haystack) == VariantGetNumberKey(needle_type, needle);
	case VariantComparisonType::TIME:
		return VariantGetTimeValue(haystack_type, haystack) == VariantGetTimeValue(needle_type, needle);
	case VariantComparisonType::TIME_TZ:
		return haystack.GetData<dtime_tz_t>() == needle.GetData<dtime_tz_t>();
	case VariantComparisonType::TIMESTAMP:
		return VariantGetTimestampValue(haystack_type, haystack) == VariantGetTimestampValue(needle_type, needle);
	case VariantComparisonType::TIMESTAMP_TZ:
		return VariantGetTimestampTZValue(haystack_type, haystack) == VariantGetTimestampTZValue(needle_type, needle);
	case VariantComparisonType::INTERVAL:
		return haystack.GetData<interval_t>() == needle.GetData<interval_t>();
	case VariantComparisonType::BLOB:
	case VariantComparisonType::BITSTRING:
	case VariantComparisonType::GEOMETRY:
	case VariantComparisonType::VARCHAR:
		return haystack.GetString() == needle.GetString();
	}

	return false;
}

static bool RecursiveHaystackWalk(const VariantNode &haystack, const VariantNode &needle, const idx_t depth) {
	CheckDepth(depth);
	if (IsEqual(haystack, needle, depth)) {
		return true;
	}

	switch (haystack.GetTypeId()) {
	case VariantLogicalType::ARRAY:
		for (const auto &child : haystack.GetArrayChildren()) {
			if (RecursiveHaystackWalk(child, needle, depth + 1)) {
				return true;
			}
		}
		break;
	case VariantLogicalType::OBJECT:
		for (const auto &child : haystack.GetObjectChildren()) {
			if (RecursiveHaystackWalk(child.value, needle, depth + 1)) {
				return true;
			}
		}
		break;
	default:
		return false;
	}

	return false;
}

static void VariantContainsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 2);
	(void)state;

	const auto count = input.size();
	const VectorIterator<VectorVariantType> haystacks(input.data[0]);
	const VectorIterator<VectorVariantType> needles(input.data[1]);

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto result_writer = FlatVector::Writer<bool>(result, count);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!haystacks.RowIsValid(row_idx) || !needles.RowIsValid(row_idx)) {
			result_writer.WriteNull();
			continue;
		}
		result_writer.WriteValue(RecursiveHaystackWalk(haystacks[row_idx], needles[row_idx], 0));
	}
}

ScalarFunctionSet VariantContainsFun::GetFunctions() {
	ScalarFunction function("variant_contains", {}, LogicalType::BOOLEAN, VariantContainsFunction);
	function.GetSignature()
	    .AddParameter("variant_haystack", LogicalType::VARIANT())
	    .AddParameter("variant_needle", LogicalType::VARIANT());
	return ScalarFunctionSet(function);
}

} // namespace duckdb
