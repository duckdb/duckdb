#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Variant Comparator Encoding
//===--------------------------------------------------------------------===//
// VARIANT is physically a STRUCT (keys/children/values/data), but ordering it by that physical
// representation does not match the logical ordering of the values it stores. Instead we traverse
// the variant and emit a sort key per value as [type-rank][value]. Sorting is performed by type
// first and value second. Object keys are processed in string-sorted order so that objects that
// only differ in key order (e.g. {'b': 10, 'a': 1} and {'a': 1, 'b': 10}) compare equal.
//
// Types are grouped into "ranks" the way a semi-structured system (e.g. Snowflake) groups them:
// all integers, decimals and bignums fold into a single NUMBER rank (compared by numeric value) and
// FLOAT/DOUBLE fold into a single REAL rank. Everything else keeps its own rank.
//
// The produced key is byte-comparable and self-contained - it is only ever compared against other
// variant_comparator outputs, so the byte constants below only need to be internally consistent
// (they mirror the conventions used by create_sort_key).

//! byte constants for the comparator encoding
constexpr data_t NULL_FIRST_BYTE = 1;
constexpr data_t NULL_LAST_BYTE = 2;
constexpr data_t STRING_DELIMITER = 0;
constexpr data_t LIST_DELIMITER = 0;
constexpr data_t BLOB_ESCAPE_CHARACTER = 1;

//! nanoseconds-per-unit scale factors used to fold the DATE / TIME / TIMESTAMP precisions into a
//! common unit (nanoseconds) so that values of different precision compare by their actual instant
constexpr int64_t NANOS_PER_MICRO = 1000;
constexpr int64_t NANOS_PER_MILLI = 1000000;
constexpr int64_t NANOS_PER_SEC = 1000000000;
constexpr int64_t NANOS_PER_DAY = 86400LL * NANOS_PER_SEC;

//! The sort-key rank of a variant value - determines the cross-type ordering (type-first)
enum class VariantSortRank : data_t {
	NULL_VALUE = 0,
	BOOLEAN,
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
	OBJECT
};

data_t GetVariantTypeRank(VariantLogicalType type_id) {
	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
		return static_cast<data_t>(VariantSortRank::NULL_VALUE);
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		return static_cast<data_t>(VariantSortRank::BOOLEAN);
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
		return static_cast<data_t>(VariantSortRank::NUMBER);
	// FLOAT and DOUBLE fold into a single REAL rank
	case VariantLogicalType::FLOAT:
	case VariantLogicalType::DOUBLE:
		return static_cast<data_t>(VariantSortRank::REAL);
	case VariantLogicalType::VARCHAR:
		return static_cast<data_t>(VariantSortRank::VARCHAR);
	case VariantLogicalType::BLOB:
		return static_cast<data_t>(VariantSortRank::BLOB);
	case VariantLogicalType::UUID:
		return static_cast<data_t>(VariantSortRank::UUID);
	// DATE folds into the TIMESTAMP rank (a date compares as midnight of that day), together with all
	// non-tz TIMESTAMP precisions (SEC / MILIS / MICROS / NANOS)
	case VariantLogicalType::DATE:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP);
	// TIMESTAMP WITH TIME ZONE precisions fold into a single TIMESTAMP_TZ rank
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP_TZ);
	// TIME precisions fold into a single TIME rank
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIME_NANOS:
		return static_cast<data_t>(VariantSortRank::TIME);
	case VariantLogicalType::TIME_MICROS_TZ:
		return static_cast<data_t>(VariantSortRank::TIME_TZ);
	case VariantLogicalType::INTERVAL:
		return static_cast<data_t>(VariantSortRank::INTERVAL);
	case VariantLogicalType::GEOMETRY:
		return static_cast<data_t>(VariantSortRank::GEOMETRY);
	case VariantLogicalType::BITSTRING:
		return static_cast<data_t>(VariantSortRank::BITSTRING);
	case VariantLogicalType::ARRAY:
		return static_cast<data_t>(VariantSortRank::ARRAY);
	case VariantLogicalType::OBJECT:
		return static_cast<data_t>(VariantSortRank::OBJECT);
	default:
		throw NotImplementedException("Variant type %s is not supported in variant_comparator",
		                              EnumUtil::ToString(type_id));
	}
}

//===--------------------------------------------------------------------===//
// NUMBER rank encoding
//===--------------------------------------------------------------------===//
// All integers, decimals and bignums fold into a single NUMBER rank and are compared by numeric
// value. To make values of different scales/widths comparable (and equal when numerically equal),
// each value is reduced to an order-preserving "decimal-scientific" form:
//
//     value = sign * 0.d1 d2 ... dk * 10^adjexp     (d1 != 0, trailing zeros stripped)
//
// and encoded as [class][adjexp][digits][terminator], where everything after the class byte is
// complemented for negatives. This makes e.g. 1::TINYINT, 1::BIGINT, 1.00::DECIMAL and 1::BIGNUM all
// encode identically, and orders them numerically (so -100.5 < 0 < 1.5 < 15 < ...).
struct VariantNumberKey {
	//! 0 = negative, 1 = zero, 2 = positive (so negatives sort before zero before positives)
	data_t number_class;
	int64_t adjusted_exponent;
	//! significant decimal digits ('0'..'9'), leading digit non-zero, trailing zeros stripped
	string digits;
};

//! Build a number key from a sign, the decimal digits of the magnitude (no leading zeros, as e.g.
//! produced by Uhugeint::ToString) and a base-10 scale (value = magnitude / 10^scale)
VariantNumberKey BuildNumberKey(bool negative, const string &magnitude_digits, int64_t scale) {
	VariantNumberKey key;
	// find the first non-zero digit (magnitude strings have no leading zeros, but be defensive)
	idx_t first = 0;
	while (first < magnitude_digits.size() && magnitude_digits[first] == '0') {
		first++;
	}
	if (first == magnitude_digits.size()) {
		// the value is zero
		key.number_class = 1;
		key.adjusted_exponent = 0;
		return key;
	}
	key.number_class = negative ? 0 : 2;
	// position of the leading digit: (#digits after leading zeros - 1) - scale
	key.adjusted_exponent = static_cast<int64_t>(magnitude_digits.size() - first) - 1 - scale;
	// strip trailing zeros (keep at least the leading digit)
	idx_t last = magnitude_digits.size();
	while (last > first + 1 && magnitude_digits[last - 1] == '0') {
		last--;
	}
	key.digits = magnitude_digits.substr(first, last - first);
	return key;
}

template <class SINK>
void VariantEncodeNumber(SINK &sink, const VariantNumberKey &key) {
	// the class byte is the primary discriminator and is never complemented
	sink.Write(key.number_class);
	if (key.number_class == 1) {
		// zero - the class fully describes the value
		return;
	}
	const bool negative = key.number_class == 0;
	// for negatives we complement every payload byte so that larger magnitudes sort earlier
	auto emit = [&](data_t b) {
		sink.Write(negative ? static_cast<data_t>(~b) : b);
	};

	// adjusted exponent (order-preserving signed encoding) orders magnitude
	data_t exp_buffer[sizeof(int64_t)];
	Radix::EncodeData<int64_t>(exp_buffer, key.adjusted_exponent);
	for (idx_t i = 0; i < sizeof(int64_t); i++) {
		emit(exp_buffer[i]);
	}
	// significant digits (1..10 so the 0x00 terminator is smaller than any digit -> prefix sorts first)
	for (auto c : key.digits) {
		emit(static_cast<data_t>((c - '0') + 1));
	}
	emit(0); // terminator
}

//! Decompose a 128-bit signed value (hugeint) into (sign, magnitude)
uhugeint_t HugeintMagnitude(hugeint_t value, bool &negative) {
	negative = value.upper < 0;
	uint64_t hi = static_cast<uint64_t>(value.upper);
	uint64_t lo = value.lower;
	if (negative) {
		// two's complement negate to get the magnitude
		hi = ~hi;
		lo = ~lo;
		if (++lo == 0) {
			++hi;
		}
	}
	return uhugeint_t(hi, lo);
}

//! Decompose a variant integer value into (sign, 128-bit magnitude)
uhugeint_t VariantIntegralMagnitude(VariantLogicalType type_id, const_data_ptr_t ptr, bool &negative) {
	negative = false;
	switch (type_id) {
	case VariantLogicalType::INT8:
		return HugeintMagnitude(hugeint_t(Load<int8_t>(ptr)), negative);
	case VariantLogicalType::INT16:
		return HugeintMagnitude(hugeint_t(Load<int16_t>(ptr)), negative);
	case VariantLogicalType::INT32:
		return HugeintMagnitude(hugeint_t(Load<int32_t>(ptr)), negative);
	case VariantLogicalType::INT64:
		return HugeintMagnitude(hugeint_t(Load<int64_t>(ptr)), negative);
	case VariantLogicalType::INT128:
		return HugeintMagnitude(Load<hugeint_t>(ptr), negative);
	case VariantLogicalType::UINT8:
		return uhugeint_t(0, Load<uint8_t>(ptr));
	case VariantLogicalType::UINT16:
		return uhugeint_t(0, Load<uint16_t>(ptr));
	case VariantLogicalType::UINT32:
		return uhugeint_t(0, Load<uint32_t>(ptr));
	case VariantLogicalType::UINT64:
		return uhugeint_t(0, Load<uint64_t>(ptr));
	case VariantLogicalType::UINT128:
		return Load<uhugeint_t>(ptr);
	default:
		throw InternalException("VariantIntegralMagnitude called on non-integer type");
	}
}

//! Compute the number key for any value in the NUMBER rank (integer, decimal or bignum)
VariantNumberKey VariantGetNumberKey(VariantLogicalType type_id, const VariantIterator &it) {
	switch (type_id) {
	case VariantLogicalType::DECIMAL: {
		auto decimal_data = it.GetDecimal();
		hugeint_t unscaled;
		switch (decimal_data.GetPhysicalType()) {
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
		auto magnitude = HugeintMagnitude(unscaled, negative);
		return BuildNumberKey(negative, Uhugeint::ToString(magnitude), decimal_data.scale);
	}
	case VariantLogicalType::BIGNUM: {
		auto bignum_blob = it.GetString();
		auto decimal_string = Bignum::BignumToVarchar(bignum_t(bignum_blob));
		bool negative = !decimal_string.empty() && decimal_string[0] == '-';
		idx_t offset = negative ? 1 : 0;
		return BuildNumberKey(negative, decimal_string.substr(offset), 0);
	}
	default: {
		// integer types
		bool negative;
		auto magnitude = VariantIntegralMagnitude(type_id, it.GetData(), negative);
		return BuildNumberKey(negative, Uhugeint::ToString(magnitude), 0);
	}
	}
}

//! Sink that only computes the encoded length of a variant value
struct VariantComparatorLengthSink {
	idx_t length = 0;

	inline void Write(data_t) {
		length++;
	}
	inline void WriteBytes(const_data_ptr_t, idx_t count) {
		length += count;
	}
};

//! Sink that writes the encoded bytes of a variant value (flipping bytes for DESCENDING order)
struct VariantComparatorWriteSink {
	VariantComparatorWriteSink(data_ptr_t result_ptr, idx_t offset, bool flip_bytes)
	    : result_ptr(result_ptr), offset(offset), flip_bytes(flip_bytes) {
	}

	data_ptr_t result_ptr;
	idx_t offset;
	bool flip_bytes;

	inline void Write(data_t b) {
		result_ptr[offset++] = flip_bytes ? static_cast<data_t>(~b) : b;
	}
	inline void WriteBytes(const_data_ptr_t src, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Write(src[i]);
		}
	}
};

template <class T, class SINK>
void VariantEncodeFixed(SINK &sink, T value) {
	data_t buffer[sizeof(T)];
	Radix::EncodeData<T>(buffer, value);
	sink.WriteBytes(buffer, sizeof(T));
}

template <class SINK>
void VariantEncodeString(SINK &sink, const string_t &str, bool is_varchar) {
	auto input_data = const_data_ptr_cast(str.GetData());
	auto input_size = str.GetSize();
	if (is_varchar) {
		// VARCHAR is valid UTF-8 (never contains 0xFF) so we use the +1 / null-delimiter scheme
		for (idx_t i = 0; i < input_size; i++) {
			sink.Write(static_cast<data_t>(input_data[i] + 1));
		}
	} else {
		// arbitrary bytes - escape \x00 and \x01 so they cannot collide with the delimiter
		for (idx_t i = 0; i < input_size; i++) {
			if (input_data[i] <= 1) {
				sink.Write(BLOB_ESCAPE_CHARACTER);
			}
			sink.Write(input_data[i]);
		}
	}
	sink.Write(STRING_DELIMITER);
}

template <class SINK>
void EncodeVariantValue(const VariantIterator &it, SINK &sink) {
	auto type_id = it.GetTypeId();
	// write the type rank - this guarantees values are ordered by type first
	sink.Write(GetVariantTypeRank(type_id));

	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
		// no payload - the rank fully describes the value
		break;
	case VariantLogicalType::BOOL_TRUE:
		sink.Write(1);
		break;
	case VariantLogicalType::BOOL_FALSE:
		sink.Write(0);
		break;
	// all integers, decimals and bignums fold into the NUMBER rank and compare by numeric value
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
		VariantEncodeNumber(sink, VariantGetNumberKey(type_id, it));
		break;
	case VariantLogicalType::FLOAT:
		// fold FLOAT into the REAL rank by widening to double (lossless)
		VariantEncodeFixed<double>(sink, static_cast<double>(Load<float>(it.GetData())));
		break;
	case VariantLogicalType::DOUBLE:
		VariantEncodeFixed<double>(sink, Load<double>(it.GetData()));
		break;
	case VariantLogicalType::UUID:
		VariantEncodeFixed<hugeint_t>(sink, Load<hugeint_t>(it.GetData()));
		break;
	// DATE and all (non-tz) TIMESTAMP precisions fold into the TIMESTAMP rank and compare as the number
	// of nanoseconds since the epoch (DATE compares as midnight of that day, TIMESTAMP WITH TIME ZONE
	// as its UTC instant). int128 is used so that the conversion to nanoseconds is lossless across the
	// full range of each type. The type rank already separates the tz / non-tz groups.
	case VariantLogicalType::DATE:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(Load<int32_t>(it.GetData())) * hugeint_t(NANOS_PER_DAY));
		break;
	case VariantLogicalType::TIMESTAMP_SEC:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(Load<int64_t>(it.GetData())) * hugeint_t(NANOS_PER_SEC));
		break;
	case VariantLogicalType::TIMESTAMP_MILIS:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(Load<int64_t>(it.GetData())) * hugeint_t(NANOS_PER_MILLI));
		break;
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(Load<int64_t>(it.GetData())) * hugeint_t(NANOS_PER_MICRO));
		break;
	case VariantLogicalType::TIMESTAMP_NANOS:
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(Load<int64_t>(it.GetData())));
		break;
	// TIME precisions fold into the TIME rank and compare as nanoseconds since midnight (fits in int64)
	case VariantLogicalType::TIME_MICROS:
		VariantEncodeFixed<int64_t>(sink, Load<int64_t>(it.GetData()) * NANOS_PER_MICRO);
		break;
	case VariantLogicalType::TIME_NANOS:
		VariantEncodeFixed<int64_t>(sink, Load<int64_t>(it.GetData()));
		break;
	case VariantLogicalType::TIME_MICROS_TZ:
		// TIME WITH TIME ZONE requires a dedicated byte-comparable transform
		VariantEncodeFixed<uint64_t>(sink, Load<dtime_tz_t>(it.GetData()).sort_key());
		break;
	case VariantLogicalType::INTERVAL:
		// normalize the interval so that equal intervals encode identically
		VariantEncodeFixed<interval_t>(sink, Load<interval_t>(it.GetData()).Normalize());
		break;
	case VariantLogicalType::VARCHAR:
		VariantEncodeString(sink, it.GetString(), true);
		break;
	case VariantLogicalType::BLOB:
	case VariantLogicalType::GEOMETRY:
	case VariantLogicalType::BITSTRING:
		VariantEncodeString(sink, it.GetString(), false);
		break;
	case VariantLogicalType::ARRAY: {
		auto children = it.GetArrayChildren();
		for (auto &child : children) {
			EncodeVariantValue(child, sink);
		}
		sink.Write(LIST_DELIMITER);
		break;
	}
	case VariantLogicalType::OBJECT: {
		// gather (key, value) pairs and process them in string-sorted key order so that objects
		// that only differ in key order compare equal
		auto entries = it.GetObjectChildren();
		std::sort(entries.begin(), entries.end(),
		          [](const std::pair<string_t, VariantIterator> &a, const std::pair<string_t, VariantIterator> &b) {
			          return a.first < b.first;
		          });
		for (auto &entry : entries) {
			VariantEncodeString(sink, entry.first, true);
			EncodeVariantValue(entry.second, sink);
		}
		sink.Write(LIST_DELIMITER);
		break;
	}
	default:
		throw NotImplementedException("Variant type %s is not supported in variant_comparator",
		                              EnumUtil::ToString(type_id));
	}
}

//! Encodes the *logical* value of a VARIANT into a byte-comparable sort key. VARIANT is physically a
//! STRUCT, but the generic create_sort_key encodes that physical representation (which is reversible,
//! so it can be used by aggregates to store/decode values). For comparison / ordering we instead
//! encode the *logical* value of the variant - this encoding is intentionally not reversible (e.g.
//! all integer widths fold together). NULLs are propagated into the result validity.
void CreateVariantComparator(const Vector &input, idx_t count, OrderModifiers modifiers, Vector &result) {
	VariantIteratorState variant(input, count);

	data_t null_byte = NULL_FIRST_BYTE;
	data_t valid_byte = NULL_LAST_BYTE;
	if (modifiers.null_type == OrderByNullType::NULLS_LAST) {
		std::swap(null_byte, valid_byte);
	}
	const bool flip_bytes = modifiers.order_type == OrderType::DESCENDING;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);

	for (idx_t r = 0; r < count; r++) {
		auto root = variant.Root(r);
		// a VARIANT is only NULL at the root via a genuine SQL NULL (never a VARIANT_NULL value)
		const bool valid = !root.IsNull();
		// phase 1 - compute the encoded length
		idx_t length = 1; // validity byte
		if (valid) {
			VariantComparatorLengthSink length_sink;
			EncodeVariantValue(root, length_sink);
			length += length_sink.length;
		}
		// phase 2 - allocate and construct
		result_data[r] = StringVector::EmptyString(result, length);
		auto ptr = data_ptr_cast(result_data[r].GetDataWriteable());
		if (!valid) {
			ptr[0] = null_byte;
			result_data[r].Finalize();
			// propagate NULL so that NULL = NULL stays NULL and ORDER BY ... NULLS FIRST/LAST is honored
			result_validity.SetInvalid(r);
			continue;
		}
		ptr[0] = valid_byte;
		VariantComparatorWriteSink write_sink(ptr, 1, flip_bytes);
		EncodeVariantValue(root, write_sink);
		result_data[r].Finalize();
	}
}

// variant_comparator produces a binary, byte-comparable sort key for a VARIANT value.
// It is used as a collation for VARIANT so that comparison / ordering / grouping all operate on the
// logical ordering of variant values (see EncodeVariantValue above).
// Unlike the create_sort_key scalar (which uses SPECIAL_HANDLING and encodes NULLs into the key), this
// function propagates NULL - this is required so that NULL = NULL stays NULL and ORDER BY ... NULLS
// FIRST/LAST is handled by the surrounding operator instead of being baked into the key.
void VariantComparatorFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);
	OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
	CreateVariantComparator(input.data[0], input.size(), modifiers, result);
}

} // namespace

ScalarFunction VariantComparatorFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("variant_comparator", {variant_type}, LogicalType::BLOB, VariantComparatorFunction);
}

} // namespace duckdb
