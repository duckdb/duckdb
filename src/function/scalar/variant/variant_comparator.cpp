#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"

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
constexpr data_t STRING_DELIMITER = 0;
constexpr data_t LIST_DELIMITER = 0;
constexpr data_t BLOB_ESCAPE_CHARACTER = 1;

//! BIT/BITSTRING sort-key bytes (mirrors CreateBitStringSortKey in the core_functions bitstring code):
//! each bit is expanded to one byte, kept above the STRING_DELIMITER (0) so a shorter bitstring (a
//! prefix) sorts first, while preserving 0 < 1
constexpr data_t BIT_SORT_KEY_ZERO = 2;
constexpr data_t BIT_SORT_KEY_ONE = 3;

//! nanoseconds-per-unit scale factors used to fold the DATE / TIME / TIMESTAMP precisions into a
//! common unit (nanoseconds) so that values of different precision compare by their actual instant
constexpr int64_t NANOS_PER_MICRO = 1000;
constexpr int64_t NANOS_PER_MILLI = 1000000;
constexpr int64_t NANOS_PER_SEC = 1000000000;
constexpr int64_t NANOS_PER_DAY = 86400LL * NANOS_PER_SEC;

//! The sort-key rank of a variant value - determines the cross-type ordering (type-first).
//! Ranks start at 1 so that every rank is strictly greater than the LIST/STRING delimiter (0); this
//! guarantees a shorter list/object sorts before a longer one (the delimiter is smaller than any
//! following element's rank byte) and that an element can never be confused with an end-of-list marker.
enum class VariantSortRank : data_t {
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
	//! VARIANT_NULL ranks last so a nested NULL element orders after all non-null values, matching the
	//! native "NULLS LAST" ordering of nested NULLs under ASC. For DESC the ORDER BY operator reverses
	//! the (always-ascending) comparator key, which then places nested NULLs first - exactly as native
	//! nested ordering does (where the null position within nested types follows ASC/DESC).
	NULL_VALUE
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

//! Decimal-digit string of a 128-bit magnitude. Uses a cheap 64-bit conversion when the value fits in
//! 64 bits, avoiding the slower 128-bit Uhugeint::ToString.
string MagnitudeToString(uhugeint_t magnitude) {
	if (magnitude.upper == 0) {
		return to_string(magnitude.lower);
	}
	return Uhugeint::ToString(magnitude);
}

//! Build a number key from a native (<= 64-bit) integer value, computing the magnitude in 64-bit
//! arithmetic instead of widening every value to uhugeint_t.
template <class T>
VariantNumberKey IntegerNumberKey(T value) {
	bool negative = false;
	auto magnitude = static_cast<uint64_t>(value);
	if constexpr (std::is_signed<T>::value) {
		if (value < 0) {
			negative = true;
			magnitude = uint64_t(0) - magnitude; // unsigned negate -> |value| (also handles the minimum)
		}
	}
	return BuildNumberKey(negative, to_string(magnitude), 0);
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
		return BuildNumberKey(negative, MagnitudeToString(magnitude), decimal_data.scale);
	}
	case VariantLogicalType::BIGNUM: {
		auto bignum_blob = it.GetString();
		auto decimal_string = Bignum::BignumToVarchar(bignum_t(bignum_blob));
		bool negative = !decimal_string.empty() && decimal_string[0] == '-';
		idx_t offset = negative ? 1 : 0;
		return BuildNumberKey(negative, decimal_string.substr(offset), 0);
	}
	// integer types - use the native width for the magnitude, only widening to 128-bit when necessary
	case VariantLogicalType::INT8:
		return IntegerNumberKey(Load<int8_t>(it.GetDataPointer()));
	case VariantLogicalType::INT16:
		return IntegerNumberKey(Load<int16_t>(it.GetDataPointer()));
	case VariantLogicalType::INT32:
		return IntegerNumberKey(Load<int32_t>(it.GetDataPointer()));
	case VariantLogicalType::INT64:
		return IntegerNumberKey(Load<int64_t>(it.GetDataPointer()));
	case VariantLogicalType::UINT8:
		return IntegerNumberKey(Load<uint8_t>(it.GetDataPointer()));
	case VariantLogicalType::UINT16:
		return IntegerNumberKey(Load<uint16_t>(it.GetDataPointer()));
	case VariantLogicalType::UINT32:
		return IntegerNumberKey(Load<uint32_t>(it.GetDataPointer()));
	case VariantLogicalType::UINT64:
		return IntegerNumberKey(Load<uint64_t>(it.GetDataPointer()));
	case VariantLogicalType::INT128: {
		bool negative;
		auto magnitude = HugeintMagnitude(Load<hugeint_t>(it.GetDataPointer()), negative);
		return BuildNumberKey(negative, MagnitudeToString(magnitude), 0);
	}
	case VariantLogicalType::UINT128:
		return BuildNumberKey(false, MagnitudeToString(Load<uhugeint_t>(it.GetDataPointer())), 0);
	default:
		throw InternalException("VariantGetNumberKey called on non-number type %s", EnumUtil::ToString(type_id));
	}
}

//! Sink that appends the encoded bytes of a variant value to a (growable) buffer
struct VariantComparatorBufferSink {
	explicit VariantComparatorBufferSink(string &buffer) : buffer(buffer) {
	}

	string &buffer;

	inline void Write(data_t b) {
		buffer.push_back(static_cast<char>(b));
	}
	inline void WriteBytes(const_data_ptr_t src, idx_t count) {
		buffer.append(const_char_ptr_cast(src), count);
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
		VariantEncodeFixed<double>(sink, static_cast<double>(it.GetData<float>()));
		break;
	case VariantLogicalType::DOUBLE:
		VariantEncodeFixed<double>(sink, it.GetData<double>());
		break;
	case VariantLogicalType::UUID:
		VariantEncodeFixed<hugeint_t>(sink, it.GetData<hugeint_t>());
		break;
	// DATE and all (non-tz) TIMESTAMP precisions fold into the TIMESTAMP rank and compare as the number
	// of nanoseconds since the epoch (DATE compares as midnight of that day, TIMESTAMP WITH TIME ZONE
	// as its UTC instant). int128 is used so that the conversion to nanoseconds is lossless across the
	// full range of each type. The type rank already separates the tz / non-tz groups.
	case VariantLogicalType::DATE:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(it.GetData<int32_t>()) * hugeint_t(NANOS_PER_DAY));
		break;
	case VariantLogicalType::TIMESTAMP_SEC:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(it.GetData<int64_t>()) * hugeint_t(NANOS_PER_SEC));
		break;
	case VariantLogicalType::TIMESTAMP_MILIS:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(it.GetData<int64_t>()) * hugeint_t(NANOS_PER_MILLI));
		break;
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(it.GetData<int64_t>()) * hugeint_t(NANOS_PER_MICRO));
		break;
	case VariantLogicalType::TIMESTAMP_NANOS:
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		VariantEncodeFixed<hugeint_t>(sink, hugeint_t(it.GetData<int64_t>()));
		break;
	// TIME precisions fold into the TIME rank and compare as nanoseconds since midnight (fits in int64)
	case VariantLogicalType::TIME_MICROS:
		VariantEncodeFixed<int64_t>(sink, it.GetData<int64_t>() * NANOS_PER_MICRO);
		break;
	case VariantLogicalType::TIME_NANOS:
		VariantEncodeFixed<int64_t>(sink, it.GetData<int64_t>());
		break;
	case VariantLogicalType::TIME_MICROS_TZ:
		// TIME WITH TIME ZONE requires a dedicated byte-comparable transform
		VariantEncodeFixed<uint64_t>(sink, it.GetData<dtime_tz_t>().sort_key());
		break;
	case VariantLogicalType::INTERVAL:
		// normalize the interval so that equal intervals encode identically
		VariantEncodeFixed<interval_t>(sink, it.GetData<interval_t>().Normalize());
		break;
	case VariantLogicalType::VARCHAR:
		VariantEncodeString(sink, it.GetString(), true);
		break;
	case VariantLogicalType::BLOB:
	case VariantLogicalType::GEOMETRY:
		VariantEncodeString(sink, it.GetString(), false);
		break;
	case VariantLogicalType::BITSTRING: {
		// BIT is ordered by its logical bit sequence, not by its raw bytes - expand each bit to a sort
		// key byte (matching the bitstring_byte_comparable collation), terminated so a shorter bitstring
		// (a prefix) sorts before a longer one
		auto bitstring = it.GetString();
		auto bit_length = Bit::BitLength(bitstring);
		for (idx_t bit_idx = 0; bit_idx < bit_length; bit_idx++) {
			sink.Write(Bit::GetBit(bitstring, bit_idx) ? BIT_SORT_KEY_ONE : BIT_SORT_KEY_ZERO);
		}
		sink.Write(STRING_DELIMITER);
		break;
	}
	case VariantLogicalType::ARRAY: {
		for (auto child : it.GetArrayChildren()) {
			EncodeVariantValue(child, sink);
		}
		sink.Write(LIST_DELIMITER);
		break;
	}
	case VariantLogicalType::OBJECT: {
		// process the children in string-sorted key order so that objects that only differ in key order
		// compare equal
		for (auto &entry : it.GetObjectChildren(VariantIterationOrder::LEXICOGRAPHIC)) {
			VariantEncodeString(sink, entry.key, true);
			EncodeVariantValue(entry.value, sink);
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
void CreateVariantComparator(const Vector &input, idx_t count, Vector &result) {
	VariantIteratorState variant(input);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto writer = FlatVector::Writer<string_t>(result, count);

	//! reused growable buffer - the key is encoded once and then copied into the result vector
	string buffer;
	for (idx_t r = 0; r < count; r++) {
		auto root = variant.Root(r);
		// a VARIANT is only NULL at the root via a genuine SQL NULL (never a VARIANT_NULL value)
		if (root.IsNull()) {
			// propagate NULL so that NULL = NULL stays NULL and ORDER BY ... NULLS FIRST/LAST is honored
			writer.WriteNull();
			continue;
		}
		// encode the key once into the (reused) buffer, then hand it to the writer (which copies it).
		// NULLs are handled via validity above, so no NULL/validity prefix byte is needed in the key.
		buffer.clear();
		VariantComparatorBufferSink sink(buffer);
		EncodeVariantValue(root, sink);
		writer.WriteValue(string_t(buffer.data(), NumericCast<uint32_t>(buffer.size())));
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
	CreateVariantComparator(input.data[0], input.size(), result);
}

} // namespace

ScalarFunction VariantComparatorFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("variant_comparator", {variant_type}, LogicalType::BLOB, VariantComparatorFunction);
}

} // namespace duckdb
