#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/types/variant_comparison.hpp"

namespace duckdb {

namespace {
constexpr int64_t NANOS_PER_MICRO = 1000;
constexpr int64_t NANOS_PER_MILLI = 1000000;
constexpr int64_t NANOS_PER_SEC = 1000000000;
constexpr int64_t NANOS_PER_DAY = 86400LL * NANOS_PER_SEC;

//! Build a number key from a sign, the decimal digits of the magnitude (no leading zeros, as e.g.
//! produced by NumericHelper::FormatUnsigned) and a base-10 scale (value = magnitude / 10^scale)
VariantNumberKey BuildNumberKey(bool negative, const char *digits, idx_t count, int64_t scale) {
	VariantNumberKey key;
	// find the first non-zero digit (magnitude digits have no leading zeros, but be defensive)
	idx_t first = 0;
	while (first < count && digits[first] == '0') {
		first++;
	}
	if (first == count) {
		// the value is zero
		key.number_class = 1;
		key.adjusted_exponent = 0;
		key.digit_count = 0;
		return key;
	}
	key.number_class = negative ? 0 : 2;
	// position of the leading digit: (#digits after leading zeros - 1) - scale
	key.adjusted_exponent = NumericCast<int64_t>(count - first) - 1 - scale;
	// strip trailing zeros (keep at least the leading digit)
	idx_t last = count;
	while (last > first + 1 && digits[last - 1] == '0') {
		last--;
	}
	key.SetDigits(digits + first, last - first);
	return key;
}

//! Build a number key from a 128-bit magnitude
VariantNumberKey BuildNumberKey(bool negative, uhugeint_t magnitude, int64_t scale) {
	char buffer[NumericLimits<uhugeint_t>::Digits()];
	auto end = buffer + sizeof(buffer);
	auto start = NumericHelper::FormatUnsigned(magnitude, end);
	return BuildNumberKey(negative, start, NumericCast<idx_t>(end - start), scale);
}

//! Decompose a 128-bit signed value (hugeint) into (sign, magnitude)
uhugeint_t HugeintMagnitude(const hugeint_t value, bool &negative) {
	negative = value.upper < 0;
	const uhugeint_t magnitude(static_cast<uint64_t>(value.upper), value.lower);
	return negative ? -magnitude : magnitude;
}

//! Build a number key from a native (<= 64-bit) integer value, computing the magnitude and its
//! decimal digits in 64-bit arithmetic instead of widening every value to uhugeint_t.
template <class T>
VariantNumberKey IntegerNumberKey(T value) {
	bool negative = false;
	auto magnitude = static_cast<uint64_t>(value);
	if constexpr (std::is_signed_v<T>) {
		if (value < 0) {
			negative = true;
			magnitude = static_cast<uint64_t>(0) - magnitude; // unsigned negate -> |value| (also handles the minimum)
		}
	}
	char buffer[NumericLimits<uint64_t>::Digits()];
	auto end = buffer + sizeof(buffer);
	auto start = NumericHelper::FormatUnsigned(magnitude, end);
	return BuildNumberKey(negative, start, NumericCast<idx_t>(end - start), 0);
}
} // namespace

VariantComparisonType GetVariantComparisonType(VariantLogicalType type) {
	switch (type) {
	case VariantLogicalType::VARIANT_NULL:
		return VariantComparisonType::NULL_VALUE;
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		return VariantComparisonType::BOOLEAN;
		// all integers, decimals and bignums fold into a single NUMBER rank (compared by numeric value)
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
		// FLOAT and DOUBLE fold into a single REAL rank
	case VariantLogicalType::FLOAT:
	case VariantLogicalType::DOUBLE:
		return VariantComparisonType::REAL;
	case VariantLogicalType::VARCHAR:
		return VariantComparisonType::VARCHAR;
	case VariantLogicalType::BLOB:
		return VariantComparisonType::BLOB;
	case VariantLogicalType::UUID:
		return VariantComparisonType::UUID;
		// DATE folds into the TIMESTAMP rank (a date compares as midnight of that day), together with all
		// non-tz TIMESTAMP precisions (SEC / MILIS / MICROS / NANOS)
	case VariantLogicalType::DATE:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
		return VariantComparisonType::TIMESTAMP;
		// TIMESTAMP WITH TIME ZONE precisions fold into a single TIMESTAMP_TZ rank
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return VariantComparisonType::TIMESTAMP_TZ;
		// TIME precisions fold into a single TIME rank
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIME_NANOS:
		return VariantComparisonType::TIME;
	case VariantLogicalType::TIME_MICROS_TZ:
		return VariantComparisonType::TIME_TZ;
	case VariantLogicalType::INTERVAL:
		return VariantComparisonType::INTERVAL;
	case VariantLogicalType::BITSTRING:
		return VariantComparisonType::BITSTRING;
	case VariantLogicalType::GEOMETRY:
		return VariantComparisonType::GEOMETRY;
	case VariantLogicalType::GEOGRAPHY:
		return VariantComparisonType::GEOGRAPHY;
	case VariantLogicalType::ARRAY:
		return VariantComparisonType::ARRAY;
	case VariantLogicalType::OBJECT:
		return VariantComparisonType::OBJECT;
	default:
		throw NotImplementedException("Variant type %s is not supported", EnumUtil::ToString(type));
	}
}
double VariantGetRealValue(VariantLogicalType type_id, const VariantNode &node) {
	switch (type_id) {
	case VariantLogicalType::FLOAT:
		// fold FLOAT into the REAL rank by widening to double (lossless)
		return static_cast<double>(node.GetData<float>());
	case VariantLogicalType::DOUBLE:
		return node.GetData<double>();
	default:
		throw InternalException("VariantGetRealValue called on non-REAL type");
	}
}

VariantNumberKey VariantGetNumberKey(VariantLogicalType type_id, const VariantNode &node) {
	switch (type_id) {
	case VariantLogicalType::DECIMAL: {
		const auto decimal_data = node.GetDecimal();
		hugeint_t unscaled;
		switch (decimal_data.GetPhysicalType()) {
		case PhysicalType::VARCHAR: {
			// the BIGNUM blob of a decimal wider than 38 digits
			const auto decimal_string = Bignum::BignumToVarchar(bignum_t(node.GetWideDecimal()));
			const bool negative = !decimal_string.empty() && decimal_string[0] == '-';
			const idx_t offset = negative ? 1 : 0;
			return BuildNumberKey(negative, decimal_string.data() + offset, decimal_string.size() - offset,
			                      decimal_data.scale);
		}
		case PhysicalType::INT16:
			unscaled = hugeint_t(Load<int16_t>(decimal_data.value_ptr));
			break;
		case PhysicalType::INT32:
			unscaled = hugeint_t(Load<int32_t>(decimal_data.value_ptr));
			break;
		case PhysicalType::INT64:
			unscaled = hugeint_t(Load<int64_t>(decimal_data.value_ptr));
			break;
		default:
			unscaled = Load<hugeint_t>(decimal_data.value_ptr);
			break;
		}
		bool negative;
		const auto magnitude = HugeintMagnitude(unscaled, negative);
		return BuildNumberKey(negative, magnitude, decimal_data.scale);
	}
	case VariantLogicalType::BIGNUM: {
		const auto bignum_blob = node.GetString();
		const auto decimal_string = Bignum::BignumToVarchar(bignum_t(bignum_blob));
		const bool negative = !decimal_string.empty() && decimal_string[0] == '-';
		const idx_t offset = negative ? 1 : 0;
		return BuildNumberKey(negative, decimal_string.data() + offset, decimal_string.size() - offset, 0);
	}
	// integer types - use the native width for the magnitude, only widening to 128-bit when necessary
	case VariantLogicalType::INT8:
		return IntegerNumberKey(Load<int8_t>(node.GetDataPointer()));
	case VariantLogicalType::INT16:
		return IntegerNumberKey(Load<int16_t>(node.GetDataPointer()));
	case VariantLogicalType::INT32:
		return IntegerNumberKey(Load<int32_t>(node.GetDataPointer()));
	case VariantLogicalType::INT64:
		return IntegerNumberKey(Load<int64_t>(node.GetDataPointer()));
	case VariantLogicalType::UINT8:
		return IntegerNumberKey(Load<uint8_t>(node.GetDataPointer()));
	case VariantLogicalType::UINT16:
		return IntegerNumberKey(Load<uint16_t>(node.GetDataPointer()));
	case VariantLogicalType::UINT32:
		return IntegerNumberKey(Load<uint32_t>(node.GetDataPointer()));
	case VariantLogicalType::UINT64:
		return IntegerNumberKey(Load<uint64_t>(node.GetDataPointer()));
	case VariantLogicalType::INT128: {
		bool negative;
		const auto magnitude = HugeintMagnitude(Load<hugeint_t>(node.GetDataPointer()), negative);
		return BuildNumberKey(negative, magnitude, 0);
	}
	case VariantLogicalType::UINT128:
		return BuildNumberKey(false, Load<uhugeint_t>(node.GetDataPointer()), 0);
	default:
		throw InternalException("VariantGetNumberKey called on non-number type %s", EnumUtil::ToString(type_id));
	}
}

hugeint_t VariantGetTimestampValue(VariantLogicalType type_id, const VariantNode &node) {
	switch (type_id) {
	case VariantLogicalType::DATE:
		return hugeint_t(node.GetData<int32_t>()) * hugeint_t(NANOS_PER_DAY);
	case VariantLogicalType::TIMESTAMP_SEC:
		return hugeint_t(node.GetData<int64_t>()) * hugeint_t(NANOS_PER_SEC);
	case VariantLogicalType::TIMESTAMP_MILIS:
		return hugeint_t(node.GetData<int64_t>()) * hugeint_t(NANOS_PER_MILLI);
	case VariantLogicalType::TIMESTAMP_MICROS:
		return hugeint_t(node.GetData<int64_t>()) * hugeint_t(NANOS_PER_MICRO);
	case VariantLogicalType::TIMESTAMP_NANOS:
		return hugeint_t(node.GetData<int64_t>());
	default:
		throw InternalException("VariantGetTimestampValue called on non-TIMESTAMP type");
	}
}

hugeint_t VariantGetTimestampTZValue(VariantLogicalType type_id, const VariantNode &node) {
	switch (type_id) {
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return hugeint_t(node.GetData<int64_t>()) * hugeint_t(NANOS_PER_MICRO);
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return hugeint_t(node.GetData<int64_t>());
	default:
		throw InternalException("VariantGetTimestampTZValue called on non-TIMESTAMP_TZ type");
	}
}

int64_t VariantGetTimeValue(VariantLogicalType type_id, const VariantNode &node) {
	switch (type_id) {
	case VariantLogicalType::TIME_MICROS:
		return node.GetData<int64_t>() * NANOS_PER_MICRO;
	case VariantLogicalType::TIME_NANOS:
		return node.GetData<int64_t>();
	default:
		throw InternalException("VariantGetTimeValue called on non-TIME type");
	}
}

} // namespace duckdb
