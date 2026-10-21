#include "duckdb/common/types/numeric_blob.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

static constexpr data_t NUMERIC_CLASS_NEGATIVE = 0x00;
static constexpr data_t NUMERIC_CLASS_ZERO = 0x40;
static constexpr data_t NUMERIC_CLASS_POSITIVE = 0x80;
static constexpr int64_t NUMERIC_EXPONENT_BIAS = 32768;
static constexpr idx_t NUMERIC_MAX_BLOB_SIZE = NumericBlob::MAX_BLOB_SIZE;
//! The digits of a 128-bit unscaled value
static constexpr idx_t NUMERIC_MAX_INT128_DIGITS = 39;

//===--------------------------------------------------------------------===//
// Blob layout
//===--------------------------------------------------------------------===//
namespace {

//! The header of a blob: the class byte and the biased adjusted exponent, complemented for negative values
struct BlobHeader {
	static idx_t Write(data_ptr_t buffer, bool negative, int64_t adjusted) {
		buffer[0] = negative ? NUMERIC_CLASS_NEGATIVE : NUMERIC_CLASS_POSITIVE;
		auto biased = uint16_t(adjusted + NUMERIC_EXPONENT_BIAS);
		buffer[1] = data_t(biased >> 8);
		buffer[2] = data_t(biased & 0xFF);
		if (negative) {
			buffer[1] = data_t(~buffer[1]);
			buffer[2] = data_t(~buffer[2]);
		}
		return 3;
	}
};

//! A parsed blob: the digits are read in place (stored as digit + 1, complemented for negative values)
struct BlobView {
	bool zero = false;
	bool negative = false;
	int64_t adjusted = 0;
	const_data_ptr_t digits = nullptr;
	idx_t digit_count = 0;

	inline data_t Digit(idx_t i) const {
		auto b = digits[i];
		return data_t((negative ? data_t(~b) : b) - 1);
	}
};

BlobView ParseBlob(const string_t &blob) {
	BlobView view;
	auto data = const_data_ptr_cast(blob.GetData());
	auto size = blob.GetSize();
	if (size == 0) {
		throw InternalException("Empty NUMERIC blob");
	}
	if (data[0] == NUMERIC_CLASS_ZERO) {
		view.zero = true;
		return view;
	}
	view.negative = data[0] == NUMERIC_CLASS_NEGATIVE;
	if (size < 4) {
		throw InternalException("Truncated NUMERIC blob");
	}
	auto high = view.negative ? data_t(~data[1]) : data[1];
	auto low = view.negative ? data_t(~data[2]) : data[2];
	auto biased = uint16_t((uint16_t(high) << 8) | low);
	view.adjusted = int64_t(biased) - NUMERIC_EXPONENT_BIAS;
	view.digits = data + 3;
	auto terminator = view.negative ? data_t(0xFF) : data_t(0);
	idx_t count = 0;
	while (3 + count < size && data[3 + count] != terminator) {
		count++;
	}
	view.digit_count = count;
	return view;
}

//! The powers of ten that fit an unsigned 128-bit integer
struct UnsignedPowersOfTen {
	UnsignedPowersOfTen() {
		for (idx_t i = 0; i < Hugeint::CACHED_POWERS_OF_TEN; i++) {
			auto &power = Hugeint::POWERS_OF_TEN[i];
			values[i] = uhugeint_t(uint64_t(power.upper), power.lower);
		}
	}
	uhugeint_t values[Hugeint::CACHED_POWERS_OF_TEN];
};

const uhugeint_t &PowerOfTen(idx_t exponent) {
	static const UnsignedPowersOfTen powers;
	D_ASSERT(exponent < Hugeint::CACHED_POWERS_OF_TEN);
	return powers.values[exponent];
}

//! Writes the decimal digits of a 64-bit value in front of position `pos` of `digits`, two at a time
inline void WriteDigits(uint64_t value, data_t *digits, idx_t &pos) {
	while (value >= 100) {
		auto quotient = value / 100;
		auto pair = uint32_t(value - quotient * 100);
		digits[--pos] = data_t(pair % 10);
		digits[--pos] = data_t(pair / 10);
		value = quotient;
	}
	if (value >= 10) {
		digits[--pos] = data_t(value % 10);
		digits[--pos] = data_t(value / 10);
	} else {
		digits[--pos] = data_t(value);
	}
}

//! Writes the blob of magnitude * 10^-scale into the buffer and returns its length
idx_t EncodeMagnitude(uhugeint_t magnitude, bool negative, uint8_t scale, data_ptr_t buffer) {
	if (magnitude.upper == 0 && magnitude.lower == 0) {
		buffer[0] = NUMERIC_CLASS_ZERO;
		return 1;
	}
	// the decimal digits of the magnitude, filled from the back
	data_t digits[NUMERIC_MAX_INT128_DIGITS];
	idx_t pos = NUMERIC_MAX_INT128_DIGITS;
	if (magnitude.upper == 0) {
		WriteDigits(magnitude.lower, digits, pos);
	} else {
		static constexpr uint64_t TEN18 = 1000000000000000000ULL;
		while (magnitude.upper != 0) {
			auto quotient = magnitude / uhugeint_t(0, TEN18);
			auto remainder = (magnitude - quotient * uhugeint_t(0, TEN18)).lower;
			// 18 digits, zero padded
			auto group_end = pos;
			WriteDigits(remainder, digits, pos);
			while (group_end - pos < 18) {
				digits[--pos] = 0;
			}
			magnitude = quotient;
		}
		WriteDigits(magnitude.lower, digits, pos);
		// the magnitude is not zero: skip the leading zeros of the highest group
		while (digits[pos] == 0) {
			pos++;
		}
	}
	idx_t digit_count = NUMERIC_MAX_INT128_DIGITS - pos;
	int64_t adjusted = int64_t(digit_count) - 1 - int64_t(scale);
	idx_t last = NUMERIC_MAX_INT128_DIGITS;
	while (digits[last - 1] == 0) {
		last--;
	}
	auto length = BlobHeader::Write(buffer, negative, adjusted);
	if (negative) {
		for (idx_t i = pos; i < last; i++) {
			buffer[length++] = data_t(~data_t(digits[i] + 1));
		}
		buffer[length++] = data_t(0xFF);
	} else {
		for (idx_t i = pos; i < last; i++) {
			buffer[length++] = data_t(digits[i] + 1);
		}
		buffer[length++] = data_t(0);
	}
	return length;
}

template <class T>
struct Magnitude {
	static inline uhugeint_t Get(T value, bool &negative) {
		negative = value < 0;
		auto wide = uint64_t(value);
		return uhugeint_t(0, negative ? uint64_t(0) - wide : wide);
	}
};

template <>
struct Magnitude<hugeint_t> {
	static inline uhugeint_t Get(hugeint_t value, bool &negative) {
		negative = value.upper < 0;
		uhugeint_t bits(uint64_t(value.upper), value.lower);
		return negative ? uhugeint_t(0, 0) - bits : bits;
	}
};

template <>
struct Magnitude<uhugeint_t> {
	static inline uhugeint_t Get(uhugeint_t value, bool &negative) {
		negative = false;
		return value;
	}
};

//! The number of decimal digits of a 64-bit value (one for zero)
inline idx_t DigitCount(uint64_t value) {
	idx_t digits = 1;
	for (;;) {
		if (value < 10) {
			return digits;
		}
		if (value < 100) {
			return digits + 1;
		}
		if (value < 1000) {
			return digits + 2;
		}
		if (value < 10000) {
			return digits + 3;
		}
		value /= 10000;
		digits += 4;
	}
}

//! The two decimal digits of every value below 100, as digit + 1
struct DigitPairs {
	DigitPairs() {
		for (idx_t i = 0; i < 100; i++) {
			high[i] = data_t(i / 10 + 1);
			low[i] = data_t(i % 10 + 1);
		}
	}
	data_t high[100];
	data_t low[100];
};

//! Writes the digits of a 64-bit magnitude (digit + 1, complemented for negatives) and the terminator; the magnitude
//! has `digit_count` digits
template <bool NEGATIVE>
inline void WriteSmallDigits(uint64_t magnitude, idx_t digit_count, data_ptr_t digits) {
	static const DigitPairs pairs;
	idx_t pos = digit_count;
	while (pos >= 2) {
		auto quotient = magnitude / 100;
		auto pair = magnitude - quotient * 100;
		digits[pos - 1] = NEGATIVE ? data_t(~pairs.low[pair]) : pairs.low[pair];
		digits[pos - 2] = NEGATIVE ? data_t(~pairs.high[pair]) : pairs.high[pair];
		magnitude = quotient;
		pos -= 2;
	}
	if (pos == 1) {
		auto digit = data_t(magnitude + 1);
		digits[0] = NEGATIVE ? data_t(~digit) : digit;
	}
	digits[digit_count] = NEGATIVE ? data_t(0xFF) : data_t(0);
}

//! Encodes the values of a fixed-size representation: values below 2^64 are written straight into the string slots
template <class T>
void EncodeTyped(Vector &typed, idx_t count, uint8_t scale, Vector &result) {
	UnifiedVectorFormat vdata;
	typed.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	auto writer = FlatVector::Writer<string_t>(result, count);
	data_t buffer[NUMERIC_MAX_BLOB_SIZE];
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			writer.WriteNull();
			continue;
		}
		bool negative;
		auto magnitude = Magnitude<T>::Get(data[idx], negative);
		if (magnitude.upper != 0) {
			auto length = EncodeMagnitude(magnitude, negative, scale, buffer);
			auto &blob = writer.WriteEmptyString(length);
			memcpy(blob.GetDataWriteable(), buffer, length);
			blob.Finalize();
			continue;
		}
		// the common case: an inlined string_t, built in place
		alignas(8) data_t block[sizeof(string_t)] = {0};
		auto blob = block + sizeof(uint32_t);
		idx_t length;
		auto small = magnitude.lower;
		if (small == 0) {
			blob[0] = NUMERIC_CLASS_ZERO;
			length = 1;
		} else {
			auto digit_count = DigitCount(small);
			int64_t adjusted = int64_t(digit_count) - 1 - int64_t(scale);
			while (small % 10 == 0) {
				small /= 10;
				digit_count--;
			}
			length = 3 + digit_count + 1;
			if (length > string_t::INLINE_LENGTH) {
				auto &heap_blob = writer.WriteEmptyString(length);
				auto target = data_ptr_cast(heap_blob.GetDataWriteable());
				BlobHeader::Write(target, negative, adjusted);
				if (negative) {
					WriteSmallDigits<true>(small, digit_count, target + 3);
				} else {
					WriteSmallDigits<false>(small, digit_count, target + 3);
				}
				heap_blob.Finalize();
				continue;
			}
			BlobHeader::Write(blob, negative, adjusted);
			if (negative) {
				WriteSmallDigits<true>(small, digit_count, blob + 3);
			} else {
				WriteSmallDigits<false>(small, digit_count, blob + 3);
			}
		}
		Store<uint32_t>(UnsafeNumericCast<uint32_t>(length), block);
		writer.WriteStringRef(Load<string_t>(block));
	}
}

//! The digits of a blob as an unscaled 128-bit value at the given scale; false when the value does not fit the scale
//! or 128 bits
bool TryGetMagnitude(const BlobView &view, uint8_t scale, uhugeint_t &magnitude) {
	// the digits form an integer that is scaled by 10^(adjusted + 1 - digit_count): bring it to the target scale
	int64_t shift = view.adjusted + 1 - int64_t(view.digit_count) + int64_t(scale);
	if (shift < 0 || view.digit_count + idx_t(shift) > NUMERIC_MAX_INT128_DIGITS) {
		return false;
	}
	idx_t i = 0;
	uint64_t low = 0;
	for (; i < view.digit_count && i < 19; i++) {
		low = low * 10 + view.Digit(i);
	}
	magnitude = uhugeint_t(0, low);
	for (; i < view.digit_count; i++) {
		magnitude = magnitude * uhugeint_t(0, 10) + uhugeint_t(0, view.Digit(i));
		if (magnitude.upper > uint64_t(INT64_MAX)) {
			return false;
		}
	}
	if (shift == 0) {
		return true;
	}
	auto &power = PowerOfTen(idx_t(shift));
	// the magnitude does not exceed 10^39 by the digit check; the product may still exceed 128 bits
	uhugeint_t product;
	if (!Uhugeint::TryMultiply(magnitude, power, product)) {
		return false;
	}
	magnitude = product;
	return true;
}

template <class T>
struct FromMagnitude {
	static inline bool Get(uhugeint_t magnitude, bool negative, T &result) {
		if (magnitude.upper > uint64_t(INT64_MAX)) {
			return false;
		}
		hugeint_t signed_value(int64_t(magnitude.upper), magnitude.lower);
		if (negative) {
			signed_value = -signed_value;
		}
		return Hugeint::TryCast(signed_value, result);
	}
};

template <>
struct FromMagnitude<uhugeint_t> {
	static inline bool Get(uhugeint_t magnitude, bool negative, uhugeint_t &result) {
		if (negative) {
			return false;
		}
		result = magnitude;
		return true;
	}
};

template <class T>
bool TryDecodeTyped(Vector &canonical, idx_t count, uint8_t scale, Vector &typed) {
	UnifiedVectorFormat vdata;
	canonical.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	typed.SetVectorType(VectorType::FLAT_VECTOR);
	auto writer = FlatVector::Writer<T>(typed, count);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			writer.WriteNull();
			continue;
		}
		auto view = ParseBlob(data[idx]);
		if (view.zero) {
			writer.WriteValue(T(0));
			continue;
		}
		uhugeint_t magnitude;
		T value;
		if (!TryGetMagnitude(view, scale, magnitude) || !FromMagnitude<T>::Get(magnitude, view.negative, value)) {
			return false;
		}
		writer.WriteValue(value);
	}
	return true;
}

} // namespace

//===--------------------------------------------------------------------===//
// Text
//===--------------------------------------------------------------------===//
bool NumericBlob::TryEncode(const char *text, idx_t size, string &result) {
	idx_t begin = 0;
	idx_t end = size;
	while (begin < end && StringUtil::CharacterIsSpace(text[begin])) {
		begin++;
	}
	while (end > begin && StringUtil::CharacterIsSpace(text[end - 1])) {
		end--;
	}
	bool negative = false;
	if (begin < end && (text[begin] == '-' || text[begin] == '+')) {
		negative = text[begin] == '-';
		begin++;
	}
	string all_digits;
	idx_t integer_digits = 0;
	bool seen_point = false;
	for (idx_t i = begin; i < end; i++) {
		auto c = text[i];
		if (StringUtil::CharacterIsDigit(c)) {
			all_digits.push_back(c);
			if (!seen_point) {
				integer_digits++;
			}
		} else if (c == '.' && !seen_point) {
			seen_point = true;
		} else {
			return false;
		}
	}
	if (all_digits.empty()) {
		return false;
	}
	idx_t first = 0;
	while (first < all_digits.size() && all_digits[first] == '0') {
		first++;
	}
	if (first == all_digits.size()) {
		result.assign(1, char(NUMERIC_CLASS_ZERO));
		return true;
	}
	idx_t last = all_digits.size();
	while (last > first && all_digits[last - 1] == '0') {
		last--;
	}
	// the decimal exponent of the first significant digit: "123.45" -> 2, "0.0012" -> -3
	int64_t adjusted = int64_t(integer_digits) - 1 - int64_t(first);
	if (adjusted < -NUMERIC_EXPONENT_BIAS || adjusted >= NUMERIC_EXPONENT_BIAS) {
		return false;
	}
	data_t header[3];
	BlobHeader::Write(header, negative, adjusted);
	result.clear();
	result.reserve(4 + (last - first));
	result.append(const_char_ptr_cast(header), 3);
	for (idx_t i = first; i < last; i++) {
		auto b = data_t(all_digits[i] - '0' + 1);
		result.push_back(char(negative ? data_t(~b) : b));
	}
	result.push_back(char(negative ? data_t(0xFF) : data_t(0)));
	return true;
}

string NumericBlob::Encode(const string &text) {
	string result;
	if (!TryEncode(text.c_str(), text.size(), result)) {
		throw ConversionException("Cannot convert '%s' to NUMERIC: not an exact number", text);
	}
	return result;
}

string NumericBlob::ToText(const string_t &blob) {
	auto view = ParseBlob(blob);
	if (view.zero) {
		return "0";
	}
	string text;
	if (view.negative) {
		text.push_back('-');
	}
	auto digit = [&](idx_t i) {
		return char('0' + view.Digit(i));
	};
	if (view.adjusted >= 0) {
		auto integer_length = idx_t(view.adjusted) + 1;
		for (idx_t i = 0; i < view.digit_count && i < integer_length; i++) {
			text.push_back(digit(i));
		}
		if (view.digit_count <= integer_length) {
			text.append(integer_length - view.digit_count, '0');
		} else {
			text.push_back('.');
			for (idx_t i = integer_length; i < view.digit_count; i++) {
				text.push_back(digit(i));
			}
		}
	} else {
		text += "0.";
		text.append(idx_t(-view.adjusted) - 1, '0');
		for (idx_t i = 0; i < view.digit_count; i++) {
			text.push_back(digit(i));
		}
	}
	return text;
}

void NumericBlob::Digits(const string_t &blob, idx_t &integer_digits, idx_t &fractional_digits) {
	auto view = ParseBlob(blob);
	if (view.zero) {
		integer_digits = 0;
		fractional_digits = 0;
		return;
	}
	if (view.adjusted >= 0) {
		integer_digits = idx_t(view.adjusted) + 1;
		fractional_digits = view.digit_count > integer_digits ? view.digit_count - integer_digits : 0;
	} else {
		integer_digits = 0;
		fractional_digits = idx_t(-view.adjusted) - 1 + view.digit_count;
	}
}

bool NumericBlob::TryDecodeUnscaled(const string_t &blob, hugeint_t &unscaled, uint8_t &scale) {
	auto view = ParseBlob(blob);
	if (view.zero) {
		unscaled = 0;
		scale = 0;
		return true;
	}
	idx_t integer_digits, fractional_digits;
	Digits(blob, integer_digits, fractional_digits);
	if (fractional_digits > Decimal::MAX_WIDTH_INT128) {
		return false;
	}
	scale = NumericCast<uint8_t>(fractional_digits);
	uhugeint_t magnitude;
	if (!TryGetMagnitude(view, scale, magnitude)) {
		return false;
	}
	return FromMagnitude<hugeint_t>::Get(magnitude, view.negative, unscaled);
}

idx_t NumericBlob::EncodeUnscaled(hugeint_t unscaled, uint8_t scale, data_ptr_t buffer) {
	bool negative;
	auto magnitude = Magnitude<hugeint_t>::Get(unscaled, negative);
	return EncodeMagnitude(magnitude, negative, scale, buffer);
}

string_t NumericBlob::AddUnscaled(Vector &result, hugeint_t unscaled, uint8_t scale) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::NUMERIC);
	data_t buffer[NUMERIC_MAX_BLOB_SIZE];
	auto length = EncodeUnscaled(unscaled, scale, buffer);
	return StringVector::AddStringOrBlob(result, const_char_ptr_cast(buffer), length);
}

LogicalType NumericBlob::RangeType(Vector &canonical, idx_t count) {
	UnifiedVectorFormat vdata;
	canonical.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	idx_t integer_digits = 0;
	idx_t fractional_digits = 0;
	bool found = false;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		idx_t value_integer_digits, value_fractional_digits;
		Digits(data[idx], value_integer_digits, value_fractional_digits);
		integer_digits = MaxValue(integer_digits, value_integer_digits);
		fractional_digits = MaxValue(fractional_digits, value_fractional_digits);
		found = true;
	}
	if (!found) {
		return NumericType::DefaultType();
	}
	if (fractional_digits > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException("NUMERIC value needs %d digits, more than the maximum of %d",
		                          integer_digits + fractional_digits, Decimal::MAX_WIDTH_DECIMAL);
	}
	return NumericType::ForDigits(integer_digits, NumericCast<uint8_t>(fractional_digits));
}

string NumericBlob::NormalizeText(const string &text) {
	auto point = text.find('.');
	if (point == string::npos) {
		return text;
	}
	idx_t end = text.size();
	while (end > point + 1 && text[end - 1] == '0') {
		end--;
	}
	if (end == point + 1) {
		end = point;
	}
	auto result = text.substr(0, end);
	if (result == "-0") {
		return "0";
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Vectors
//===--------------------------------------------------------------------===//
static uint8_t ScaleOf(const LogicalType &type) {
	return type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0;
}

//! Wide decimals (a BIGNUM blob per value) go through their decimal text
static void EncodeThroughText(Vector &typed, idx_t count, Vector &result) {
	Vector text(LogicalType::VARCHAR, count);
	VectorOperations::DefaultCast(typed, text, count, true);
	UnifiedVectorFormat vdata;
	text.ToUnifiedFormat(vdata);
	auto text_data = UnifiedVectorFormat::GetData<string_t>(vdata);
	auto writer = FlatVector::Writer<string_t>(result, count);
	string blob;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			writer.WriteNull();
			continue;
		}
		if (!NumericBlob::TryEncode(text_data[idx].GetData(), text_data[idx].GetSize(), blob)) {
			throw InternalException("Exact value '%s' does not encode as NUMERIC", text_data[idx].GetString());
		}
		writer.WriteValue(StringVector::AddStringOrBlob(result, blob.c_str(), blob.size()));
	}
}

void NumericBlob::Encode(Vector &typed, idx_t count, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::NUMERIC);
	D_ASSERT(NumericType::IsExact(typed.GetType()));
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto scale = ScaleOf(typed.GetType());
	switch (typed.GetType().InternalType()) {
	case PhysicalType::INT8:
		return EncodeTyped<int8_t>(typed, count, scale, result);
	case PhysicalType::INT16:
		return EncodeTyped<int16_t>(typed, count, scale, result);
	case PhysicalType::INT32:
		return EncodeTyped<int32_t>(typed, count, scale, result);
	case PhysicalType::INT64:
		return EncodeTyped<int64_t>(typed, count, scale, result);
	case PhysicalType::INT128:
		return EncodeTyped<hugeint_t>(typed, count, scale, result);
	case PhysicalType::UINT8:
		return EncodeTyped<uint8_t>(typed, count, scale, result);
	case PhysicalType::UINT16:
		return EncodeTyped<uint16_t>(typed, count, scale, result);
	case PhysicalType::UINT32:
		return EncodeTyped<uint32_t>(typed, count, scale, result);
	case PhysicalType::UINT64:
		return EncodeTyped<uint64_t>(typed, count, scale, result);
	case PhysicalType::UINT128:
		return EncodeTyped<uhugeint_t>(typed, count, scale, result);
	default:
		return EncodeThroughText(typed, count, result);
	}
}

static void DecodeThroughText(Vector &canonical, idx_t count, Vector &typed) {
	UnifiedVectorFormat vdata;
	canonical.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	Vector text(LogicalType::VARCHAR, count);
	{
		auto writer = FlatVector::Writer<string_t>(text, count);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				writer.WriteNull();
				continue;
			}
			auto value_text = NumericBlob::ToText(data[idx]);
			writer.WriteValue(StringVector::AddString(text, value_text.c_str(), value_text.size()));
		}
	}
	VectorOperations::DefaultCast(text, typed, count, false);
}

void NumericBlob::Decode(Vector &canonical, idx_t count, Vector &typed) {
	D_ASSERT(canonical.GetType().id() == LogicalTypeId::NUMERIC);
	auto scale = ScaleOf(typed.GetType());
	bool decoded;
	switch (typed.GetType().InternalType()) {
	case PhysicalType::INT8:
		decoded = TryDecodeTyped<int8_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::INT16:
		decoded = TryDecodeTyped<int16_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::INT32:
		decoded = TryDecodeTyped<int32_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::INT64:
		decoded = TryDecodeTyped<int64_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::INT128:
		decoded = TryDecodeTyped<hugeint_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::UINT8:
		decoded = TryDecodeTyped<uint8_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::UINT16:
		decoded = TryDecodeTyped<uint16_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::UINT32:
		decoded = TryDecodeTyped<uint32_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::UINT64:
		decoded = TryDecodeTyped<uint64_t>(canonical, count, scale, typed);
		break;
	case PhysicalType::UINT128:
		decoded = TryDecodeTyped<uhugeint_t>(canonical, count, scale, typed);
		break;
	default:
		decoded = false;
		break;
	}
	if (!decoded) {
		// values that do not fit the representation (or wide decimals) take the checked text cast
		DecodeThroughText(canonical, count, typed);
	}
}

} // namespace duckdb
