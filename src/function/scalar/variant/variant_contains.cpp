#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"

namespace duckdb {

enum class VariantComparisonType : data_t {
	BOOLEAN = 1,
	NUMBER,
	REAL,
	VARCHAR,
	BLOB,
	UUID,
	TIMESTAMP,    // DATE and all non-tz TIMESTAMP precisions, compared as nanoseconds since the epoch
	TIMESTAMP_TZ, // TIMESTAMP WITH TIME ZONE (all precisions), compared as nanoseconds since the epoch (UTC)
	TIME,         // TIME (all precisions), compared as nanoseconds since midnight
	TIME_TZ,      // TIME WITH TIME ZONE
	INTERVAL,
	GEOMETRY,
	BITSTRING,
	ARRAY,
	OBJECT,
	NULL_VALUE
};

static VariantComparisonType GetComparisonType(const VariantLogicalType &type) {
	switch (type) {
	case VariantLogicalType::ARRAY:
		return VariantComparisonType::ARRAY;
	case VariantLogicalType::VARIANT_NULL:
		return VariantComparisonType::NULL_VALUE;
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		return VariantComparisonType::BOOLEAN;
	case VariantLogicalType::INT8:
	case VariantLogicalType::INT16:
	case VariantLogicalType::INT32:
	case VariantLogicalType::INT64:
	case VariantLogicalType::INT128:
	case VariantLogicalType::UINT8:
	case VariantLogicalType::UINT16:
	case VariantLogicalType::UINT32:
	case VariantLogicalType::UINT64:
	case VariantLogicalType::UINT128:
	case VariantLogicalType::DECIMAL:
	case VariantLogicalType::BIGNUM:
		return VariantComparisonType::NUMBER;
	case VariantLogicalType::FLOAT:
	case VariantLogicalType::DOUBLE:
		return VariantComparisonType::REAL;
	case VariantLogicalType::VARCHAR:
		return VariantComparisonType::VARCHAR;
	case VariantLogicalType::BLOB:
		return VariantComparisonType::BLOB;
	case VariantLogicalType::UUID:
		return VariantComparisonType::UUID;
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIME_NANOS:
		return VariantComparisonType::TIME;
	case VariantLogicalType::DATE:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
		return VariantComparisonType::TIMESTAMP;
	case VariantLogicalType::TIME_MICROS_TZ:
		return VariantComparisonType::TIME_TZ;
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return VariantComparisonType::TIMESTAMP_TZ;
	case VariantLogicalType::INTERVAL:
		return VariantComparisonType::INTERVAL;
	case VariantLogicalType::OBJECT:
		return VariantComparisonType::OBJECT;
	case VariantLogicalType::BITSTRING:
		return VariantComparisonType::BITSTRING;
	case VariantLogicalType::GEOMETRY:
		return VariantComparisonType::GEOMETRY;
	default:
		throw NotImplementedException("Variant type %s is not supported in variant_contains", EnumUtil::ToString(type));
	}
}

static bool IsContainedAt(const VariantNode &haystack, const VariantNode &needle);

static bool IsContainedObject(const VariantNode &haystack, const VariantNode &needle) {
	// for every needle we need to find an occurrence in the haystack
	// elements in the haystack are not consumed, so multiple needles can be satisfied by one haystack element
	for (const auto &needle_child : needle.GetObjectChildren()) {
		auto found = false;
		for (const auto &haystack_child : haystack.GetObjectChildren()) {
			if (needle_child.key != haystack_child.key) {
				continue;
			}
			if (IsContainedAt(haystack_child.value, needle_child.value)) {
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

static bool IsContainedArray(const VariantNode &haystack, const VariantNode &needle) {
	for (const auto &needle_child : needle.GetArrayChildren()) {
		auto found = false;
		for (const auto &haystack_child : haystack.GetArrayChildren()) {
			if (IsContainedAt(haystack_child, needle_child)) {
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

static bool IsContainedAt(const VariantNode &haystack, const VariantNode &needle) {
	const auto &haystack_type = GetComparisonType(haystack.GetTypeId());
	const auto &needle_type = GetComparisonType(needle.GetTypeId());
	if (haystack_type != needle_type) {
		return false;
	}

	switch (haystack_type) {
	case VariantComparisonType::ARRAY:
		return IsContainedArray(haystack, needle);
	case VariantComparisonType::OBJECT:
		return IsContainedObject(haystack, needle);
	case VariantComparisonType::NULL_VALUE:
		return true;
	case VariantComparisonType::BOOLEAN:
		return haystack.GetTypeId() == needle.GetTypeId();
	case VariantComparisonType::UUID:
		return haystack.GetData<hugeint_t>() == needle.GetData<hugeint_t>();
	case VariantComparisonType::REAL:
		break;
	case VariantComparisonType::NUMBER:
		break;
	case VariantComparisonType::TIME:
		break;
	case VariantComparisonType::TIME_TZ:
		break;
	case VariantComparisonType::TIMESTAMP:
		break;
	case VariantComparisonType::TIMESTAMP_TZ:
		break;
	case VariantComparisonType::INTERVAL:
		return haystack.GetData<interval_t>() == needle.GetData<interval_t>();
	case VariantComparisonType::BLOB:
	case VariantComparisonType::BITSTRING:
	case VariantComparisonType::GEOMETRY:
	case VariantComparisonType::VARCHAR:
		return haystack.GetString() == needle.GetString();
	}
}

static bool RecursiveContainSearch(const VariantNode &haystack, const VariantNode &needle) {
	if (IsContainedAt(haystack, needle)) {
		return true;
	}

	switch (haystack.GetTypeId()) {
	case VariantLogicalType::ARRAY:
		for (const auto &child : haystack.GetArrayChildren()) {
			if (RecursiveContainSearch(child, needle)) {
				return true;
			}
		}
		break;
	case VariantLogicalType::OBJECT:
		for (const auto &child : haystack.GetObjectChildren()) {
			if (RecursiveContainSearch(child.value, needle)) {
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
		result_writer.WriteValue(RecursiveContainSearch(haystacks[row_idx], needles[row_idx]));
	}
}

ScalarFunctionSet VariantContainsFun::GetFunctions() {
	ScalarFunction function("variant_contains", {LogicalType::VARIANT(), LogicalType::VARIANT()}, LogicalType::BOOLEAN,
	                        VariantContainsFunction);
	return ScalarFunctionSet(function);
}

} // namespace duckdb
