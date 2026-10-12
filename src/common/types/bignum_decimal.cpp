#include "duckdb/common/types/bignum_decimal.hpp"

#include "duckdb/common/bignum.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/uhugeint.hpp"

#include <cmath>
#include <cstdio>

namespace duckdb {

namespace {

//! A BIGNUM blob that owns its bytes together with an intermediate pointing into it
struct OwnedBignum {
	explicit OwnedBignum(string blob_p) : blob(std::move(blob_p)), value(string_t(blob)), intermediate(value) {
	}
	OwnedBignum(const OwnedBignum &) = delete;
	OwnedBignum &operator=(const OwnedBignum &) = delete;

	string blob;
	bignum_t value;
	BignumIntermediate intermediate;
};

//! An intermediate over a blob that lives elsewhere (the bignum_t must stay alive as long as the intermediate is used)
struct BorrowedBignum {
	explicit BorrowedBignum(const string_t &blob) : value(blob), intermediate(value) {
	}
	BorrowedBignum(const BorrowedBignum &) = delete;
	BorrowedBignum &operator=(const BorrowedBignum &) = delete;

	bignum_t value;
	BignumIntermediate intermediate;
};

void IncrementDigits(string &digits) {
	int carry = 1;
	for (idx_t i = digits.size(); i > 0 && carry; i--) {
		auto digit = digits[i - 1] - '0' + carry;
		digits[i - 1] = static_cast<char>('0' + digit % 10);
		carry = digit / 10;
	}
	if (carry) {
		digits.insert(digits.begin(), '1');
	}
}

bool FitsWidthInternal(const BignumIntermediate &value, uint8_t width) {
	BorrowedBignum limit(string_t(BignumDecimal::PowerOfTen(width)));
	return value.IsAbsoluteBigger(limit.intermediate) == SMALLER;
}

} // namespace

const string &BignumDecimal::PowerOfTen(idx_t exponent) {
	static const vector<string> POWERS = []() {
		vector<string> result;
		result.reserve(idx_t(Decimal::MAX_WIDTH_DECIMAL) + 1);
		for (idx_t i = 0; i <= Decimal::MAX_WIDTH_DECIMAL; i++) {
			string digits = "1" + string(i, '0');
			result.push_back(Bignum::VarcharToBignum(string_t(digits)));
		}
		return result;
	}();
	D_ASSERT(exponent <= Decimal::MAX_WIDTH_DECIMAL);
	return POWERS[exponent];
}

bool BignumDecimal::FitsWidth(const string_t &value, uint8_t width) {
	BorrowedBignum borrowed(value);
	return FitsWidthInternal(borrowed.intermediate, width);
}

bool BignumDecimal::FitsWidth(const BignumIntermediate &value, uint8_t width) {
	return FitsWidthInternal(value, width);
}

bool BignumDecimal::IsNegative(const string_t &value) {
	return (value.GetData()[0] & 0x80) == 0;
}

bool BignumDecimal::IsZero(const string_t &value) {
	return value.GetSize() == Bignum::BIGNUM_HEADER_SIZE + 1 && !IsNegative(value) &&
	       value.GetData()[Bignum::BIGNUM_HEADER_SIZE] == 0;
}

string BignumDecimal::ToString(const string_t &value, uint8_t scale) {
	const bignum_t bignum(value);
	auto digits = Bignum::BignumToVarchar(bignum);
	if (scale == 0) {
		return digits;
	}
	bool negative = false;
	if (!digits.empty() && digits[0] == '-') {
		negative = true;
		digits.erase(digits.begin());
	}
	// make sure there is at least one digit in front of the decimal point
	if (digits.size() <= scale) {
		digits.insert(digits.begin(), scale + 1 - digits.size(), '0');
	}
	digits.insert(digits.begin() + static_cast<int64_t>(digits.size() - scale), '.');
	if (negative) {
		digits.insert(digits.begin(), '-');
	}
	return digits;
}

bool BignumDecimal::TryParse(const char *data, idx_t size, uint8_t width, uint8_t scale, string &result) {
	idx_t pos = 0;
	// skip surrounding whitespace
	while (pos < size && StringUtil::CharacterIsSpace(data[pos])) {
		pos++;
	}
	while (size > pos && StringUtil::CharacterIsSpace(data[size - 1])) {
		size--;
	}
	if (pos >= size) {
		return false;
	}
	bool negative = false;
	if (data[pos] == '-' || data[pos] == '+') {
		negative = data[pos] == '-';
		pos++;
	}
	string digits;
	idx_t integer_digits = 0;
	bool any_digit = false;
	while (pos < size && StringUtil::CharacterIsDigit(data[pos])) {
		digits.push_back(data[pos++]);
		integer_digits++;
		any_digit = true;
	}
	if (pos < size && data[pos] == '.') {
		pos++;
		while (pos < size && StringUtil::CharacterIsDigit(data[pos])) {
			digits.push_back(data[pos++]);
			any_digit = true;
		}
	}
	if (!any_digit) {
		return false;
	}
	int64_t exponent = 0;
	if (pos < size && (data[pos] == 'e' || data[pos] == 'E')) {
		pos++;
		bool exponent_negative = false;
		if (pos < size && (data[pos] == '-' || data[pos] == '+')) {
			exponent_negative = data[pos] == '-';
			pos++;
		}
		if (pos >= size || !StringUtil::CharacterIsDigit(data[pos])) {
			return false;
		}
		while (pos < size && StringUtil::CharacterIsDigit(data[pos])) {
			if (exponent < 100000) {
				exponent = exponent * 10 + (data[pos] - '0');
			}
			pos++;
		}
		if (exponent_negative) {
			exponent = -exponent;
		}
	}
	if (pos != size) {
		return false;
	}
	// the decimal point sits after "point" digits of the digit string; we keep "point + scale" digits as the
	// unscaled value and use the digit after that for rounding (half away from zero)
	const int64_t keep = static_cast<int64_t>(integer_digits) + exponent + scale;
	string unscaled;
	bool round_up = false;
	if (keep >= 0) {
		const auto keep_count = static_cast<idx_t>(keep);
		if (keep_count > idx_t(width) + digits.size() + 1) {
			// far more digits than the width allows, unless every digit is zero
			for (auto c : digits) {
				if (c != '0') {
					return false;
				}
			}
			unscaled = "0";
		} else {
			unscaled = digits.substr(0, MinValue<idx_t>(keep_count, digits.size()));
			if (keep_count > digits.size()) {
				unscaled.append(keep_count - digits.size(), '0');
			} else if (keep_count < digits.size()) {
				round_up = digits[keep_count] >= '5';
			}
		}
	}
	// strip leading zeros
	idx_t first_non_zero = 0;
	while (first_non_zero < unscaled.size() && unscaled[first_non_zero] == '0') {
		first_non_zero++;
	}
	unscaled.erase(0, first_non_zero);
	if (round_up) {
		IncrementDigits(unscaled);
	}
	if (unscaled.size() > width) {
		return false;
	}
	if (unscaled.empty()) {
		unscaled = "0";
		negative = false;
	}
	if (negative) {
		unscaled.insert(unscaled.begin(), '-');
	}
	result = Bignum::VarcharToBignum(string_t(unscaled));
	return true;
}

bool BignumDecimal::TryRescale(const string_t &value, uint8_t source_scale, uint8_t target_width, uint8_t target_scale,
                               string &result) {
	BorrowedBignum source(value);
	if (target_scale == source_scale) {
		if (!FitsWidthInternal(source.intermediate, target_width)) {
			return false;
		}
		result = value.GetString();
		return true;
	}
	StringHeap heap;
	if (target_scale > source_scale) {
		BorrowedBignum factor(string_t(PowerOfTen(target_scale - source_scale)));
		auto scaled = BignumIntermediate::Multiply(heap, source.intermediate, factor.intermediate);
		if (!FitsWidth(scaled, target_width)) {
			return false;
		}
		result = scaled.GetString();
		return true;
	}
	// scale down: divide by the power of ten and round half away from zero
	BorrowedBignum divisor(string_t(PowerOfTen(source_scale - target_scale)));
	auto quotient = BignumIntermediate::Divide(heap, source.intermediate, divisor.intermediate);
	auto remainder = BignumIntermediate::Modulo(heap, source.intermediate, divisor.intermediate);
	BorrowedBignum remainder_value(remainder);
	BorrowedBignum two(string_t(Bignum::VarcharToBignum(string_t("2"))));
	auto twice_remainder = BignumIntermediate::Multiply(heap, remainder_value.intermediate, two.intermediate);
	BorrowedBignum twice_remainder_value(twice_remainder);
	if (twice_remainder_value.intermediate.IsAbsoluteBigger(divisor.intermediate) != SMALLER) {
		// |remainder| >= divisor / 2: round away from zero
		BorrowedBignum one(string_t(Bignum::VarcharToBignum(string_t(source.intermediate.is_negative ? "-1" : "1"))));
		BorrowedBignum quotient_value(quotient);
		quotient = BignumIntermediate::Add(heap, quotient_value.intermediate, one.intermediate);
	}
	if (!FitsWidth(quotient, target_width)) {
		return false;
	}
	result = quotient.GetString();
	return true;
}

string BignumDecimal::FromInteger(uhugeint_t value) {
	uint8_t bytes[16];
	idx_t size = 0;
	for (idx_t i = 0; i < 16; i++) {
		// big-endian: the most significant byte first
		auto shift = (15 - i) * 8;
		uint8_t byte = static_cast<uint8_t>((shift >= 64 ? value.upper >> (shift - 64) : value.lower >> shift) & 0xFF);
		if (size == 0 && byte == 0 && i < 15) {
			continue;
		}
		bytes[size++] = byte;
	}
	return Bignum::FromByteArray(bytes, size, false);
}

string BignumDecimal::FromInteger(hugeint_t value) {
	if (value.upper >= 0) {
		return FromInteger(uhugeint_t(static_cast<uint64_t>(value.upper), value.lower));
	}
	// the magnitude of the minimum hugeint (2^127) fits a uhugeint, so negate in two's complement in uhugeint space
	// (~x + 1 cannot overflow because x is not zero)
	uhugeint_t bits(static_cast<uint64_t>(value.upper), value.lower);
	uhugeint_t magnitude = ~bits + uhugeint_t(1);
	auto blob = FromInteger(magnitude);
	// flip the sign: complement header and data bytes
	for (auto &c : blob) {
		c = static_cast<char>(~c);
	}
	return blob;
}

bool BignumDecimal::TryToUhugeint(const string_t &value, uhugeint_t &result) {
	if (IsNegative(value)) {
		return false;
	}
	BorrowedBignum borrowed(value);
	auto &intermediate = borrowed.intermediate;
	const idx_t start = intermediate.GetStartDataPos();
	if (intermediate.size - start > 16) {
		return false;
	}
	result = 0;
	for (idx_t i = start; i < intermediate.size; i++) {
		result = (result << 8) | uhugeint_t(intermediate.GetAbsoluteByte(static_cast<int64_t>(i)));
	}
	return true;
}

bool BignumDecimal::TryToHugeint(const string_t &value, hugeint_t &result) {
	BorrowedBignum borrowed(value);
	auto &intermediate = borrowed.intermediate;
	const idx_t start = intermediate.GetStartDataPos();
	if (intermediate.size - start > 16) {
		return false;
	}
	uhugeint_t magnitude = 0;
	for (idx_t i = start; i < intermediate.size; i++) {
		magnitude = (magnitude << 8) | uhugeint_t(intermediate.GetAbsoluteByte(static_cast<int64_t>(i)));
	}
	const uhugeint_t limit = uhugeint_t(0x8000000000000000ULL, 0);
	if (intermediate.is_negative) {
		if (magnitude > limit) {
			return false;
		}
		if (magnitude == limit) {
			result = NumericLimits<hugeint_t>::Minimum();
			return true;
		}
		result = hugeint_t(0) - hugeint_t(static_cast<int64_t>(magnitude.upper), magnitude.lower);
		return true;
	}
	if (magnitude >= limit) {
		return false;
	}
	result = hugeint_t(static_cast<int64_t>(magnitude.upper), magnitude.lower);
	return true;
}

double BignumDecimal::ToDouble(const string_t &value, uint8_t scale) {
	auto text = ToString(value, scale);
	double result;
	if (!TryCast::Operation(string_t(text), result, false)) {
		throw InternalException("Could not convert wide decimal '%s' to DOUBLE", text);
	}
	return result;
}

string BignumDecimal::FromTwosComplement(const_data_ptr_t data, idx_t size, bool big_endian) {
	if (size == 0) {
		return Bignum::InitializeBignumZero();
	}
	// normalize to big-endian magnitude bytes
	vector<uint8_t> magnitude(size);
	for (idx_t i = 0; i < size; i++) {
		magnitude[i] = big_endian ? data[i] : data[size - 1 - i];
	}
	const bool negative = (magnitude[0] & 0x80) != 0;
	if (negative) {
		// the magnitude of a two's complement number: complement every byte and add one
		for (idx_t i = 0; i < size; i++) {
			magnitude[i] = static_cast<uint8_t>(~magnitude[i]);
		}
		for (idx_t i = size; i > 0; i--) {
			if (++magnitude[i - 1] != 0) {
				break;
			}
		}
	}
	// strip leading zero bytes
	idx_t start = 0;
	while (start + 1 < size && magnitude[start] == 0) {
		start++;
	}
	if (size - start == 1 && magnitude[start] == 0) {
		return Bignum::InitializeBignumZero();
	}
	return Bignum::FromByteArray(magnitude.data() + start, size - start, negative);
}

void BignumDecimal::ToTwosComplement(const string_t &value, data_ptr_t result, idx_t size, bool big_endian) {
	BorrowedBignum borrowed(value);
	auto &intermediate = borrowed.intermediate;
	const idx_t start = intermediate.GetStartDataPos();
	const idx_t magnitude_bytes = intermediate.size - start;
	if (magnitude_bytes > size) {
		throw OutOfRangeException("Decimal value does not fit in %d bytes", size);
	}
	// big-endian magnitude, right-aligned
	vector<uint8_t> bytes(size, 0);
	for (idx_t i = 0; i < magnitude_bytes; i++) {
		bytes[size - magnitude_bytes + i] = intermediate.GetAbsoluteByte(static_cast<int64_t>(start + i));
	}
	if (intermediate.is_negative) {
		// two's complement: complement every byte and add one
		for (idx_t i = 0; i < size; i++) {
			bytes[i] = static_cast<uint8_t>(~bytes[i]);
		}
		for (idx_t i = size; i > 0; i--) {
			if (++bytes[i - 1] != 0) {
				break;
			}
		}
		// the only negative number whose magnitude has the top bit set is -2^(8 * size - 1)
		if (magnitude_bytes == size && (bytes[0] & 0x80) == 0) {
			throw OutOfRangeException("Decimal value does not fit in %d bytes", size);
		}
	} else if (magnitude_bytes == size && (bytes[0] & 0x80) != 0) {
		throw OutOfRangeException("Decimal value does not fit in %d bytes", size);
	}
	for (idx_t i = 0; i < size; i++) {
		result[i] = big_endian ? bytes[i] : bytes[size - 1 - i];
	}
}

bool BignumDecimal::TryFromDouble(double value, uint8_t width, uint8_t scale, string &result) {
	if (!std::isfinite(value)) {
		return false;
	}
	// render the double with exactly "scale" fraction digits, then parse the digits exactly
	char buffer[1024];
	auto length = snprintf(buffer, sizeof(buffer), "%.*f", static_cast<int>(scale), value);
	if (length <= 0 || static_cast<size_t>(length) >= sizeof(buffer)) {
		return false;
	}
	return TryParse(buffer, static_cast<idx_t>(length), width, scale, result);
}

} // namespace duckdb
