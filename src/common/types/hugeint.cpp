#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/algorithm.hpp"

#include <limits>
#include <cmath>

using namespace std;

namespace duckdb {

bool Hugeint::FromString(string str, hugeint_t &result) {
	return Hugeint::FromCString(str.c_str(), str.size(), result);
}

bool Hugeint::FromCString(const char *buf, idx_t len, hugeint_t &result) {
	if (len == 0) {
		return false;
	}
	result.lower = 0;
	result.upper = 0;
	bool negative = buf[0] == '-';
	
	idx_t start_pos = negative || buf[0] == '+' ? 1 : 0;
	idx_t pos = start_pos;
	while(pos < len) {
		if (!std::isdigit((unsigned char)buf[pos])) {
			// not a digit!
			if (std::isspace((unsigned char)buf[pos])) {
				// skip any trailing spaces
				while(++pos < len) {
					if (!std::isspace((unsigned char)buf[pos])) {
						return false;
					}
				}
				break;
			}
			return false;
		}
		uint8_t digit = buf[pos++] - '0';
		Hugeint::MultiplyPositiveInPlace(result, 10);
		Hugeint::AddInPlace(result, digit);
	}
	if (negative) {
		NegateInPlace(result);
	}
	return pos > start_pos;
}

static uint8_t positive_hugeint_highest_bit(hugeint_t bits) {
	uint8_t out = 0;
	if (bits.upper){
		out = 64;
		uint64_t up = bits.upper;
		while (up){
			up >>= 1;
			out++;
		}
	}
	else{
		uint64_t low = bits.lower;
		while (low){
			low >>= 1;
			out++;
		}
	}
	return out;
}

static bool positive_hugeint_is_bit_set(hugeint_t lhs, uint8_t bit_position) {
	if (bit_position < 64) {
		return lhs.lower & (uint64_t(1) << uint64_t(bit_position));
	} else {
		return lhs.upper & (uint64_t(1) << uint64_t(bit_position - 64));
	}
}

hugeint_t positive_hugeint_leftshift(hugeint_t lhs, uint32_t amount) {
	assert(amount > 0 && amount < 64);
	hugeint_t result;
	result.lower = lhs.lower << amount;
	result.upper = (lhs.upper << amount) + (lhs.lower >> (64 - amount));
	return result;
}

static hugeint_t positive_hugeint_divmod(hugeint_t lhs, uint32_t rhs, uint32_t &remainder) {
	assert(lhs.upper >= 0);
	// DivMod code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// initialize the result and remainder to 0
	hugeint_t div_result;
	div_result.lower = 0;
	div_result.upper = 0;
	remainder = 0;

	uint8_t highest_bit_set = positive_hugeint_highest_bit(lhs);
	// now iterate over the amount of bits that are set in the LHS
	for(uint8_t x = highest_bit_set; x > 0; x--) {
		// left-shift the current result and remainder by 1
		div_result = positive_hugeint_leftshift(div_result, 1);
		remainder <<= 1;
		// we get the value of the bit at position X, where position 0 is the least-significant bit
		if (positive_hugeint_is_bit_set(lhs, x - 1)) {
			// increment the remainder
			remainder++;
		}
		if (remainder >= rhs) {
			// the remainder has passed the division multiplier: add one to the divide result
			remainder -= rhs;
			div_result.lower++;
			if (div_result.lower == 0) {
				// overflow
				div_result.upper++;
			}
		}
	}
	return div_result;
}

static hugeint_t hugeint_divmod(hugeint_t lhs, hugeint_t rhs, hugeint_t &remainder) {
	// division by zero not allowed
	assert(!(rhs.upper == 0 && rhs.lower == 0));

	bool lhs_negative = lhs.upper < 0;
	bool rhs_negative = rhs.upper < 0;
	if (lhs_negative) {
		Hugeint::NegateInPlace(lhs);
	}
	if (rhs_negative) {
		Hugeint::NegateInPlace(rhs);
	}
	// DivMod code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// initialize the result and remainder to 0
	hugeint_t div_result;
	div_result.lower = 0;
	div_result.upper = 0;
	remainder.lower = 0;
	remainder.upper = 0;

	uint8_t highest_bit_set = positive_hugeint_highest_bit(lhs);
	// now iterate over the amount of bits that are set in the LHS
	for(uint8_t x = highest_bit_set; x > 0; x--) {
		// left-shift the current result and remainder by 1
		div_result = positive_hugeint_leftshift(div_result, 1);
		remainder = positive_hugeint_leftshift(remainder, 1);
		
		// we get the value of the bit at position X, where position 0 is the least-significant bit
		if (positive_hugeint_is_bit_set(lhs, x - 1)) {
			// increment the remainder
			Hugeint::AddInPlace(remainder, 1);
		}
		if (Hugeint::GreaterThanEquals(remainder, rhs)) {
			// the remainder has passed the division multiplier: add one to the divide result
			remainder = Hugeint::Subtract(remainder, rhs);
			Hugeint::AddInPlace(div_result, 1);
		}
	}
	if (lhs_negative ^ rhs_negative) {
		Hugeint::NegateInPlace(div_result);
	}
	if (lhs_negative) {
		Hugeint::NegateInPlace(remainder);
	}
	return div_result;
}

hugeint_t Hugeint::Divide(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t remainder;
	return hugeint_divmod(lhs, rhs, remainder);
}

hugeint_t Hugeint::Modulo(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t remainder;
	hugeint_divmod(lhs, rhs, remainder);
	return remainder;
}

string Hugeint::ToString(hugeint_t input) {
	uint32_t remainder;
	string result;
	bool negative = input.upper < 0;
	if (negative) {
		NegateInPlace(input);
	}
	while(true) {
		if (!input.lower && !input.upper) {
			break;
		}
		input = positive_hugeint_divmod(input, 10, remainder);
		result = string(1, '0' + remainder) + result;
	}
	if (result.empty()) {
		// value is zero
		return "0";
	}
	return negative ? "-" + result : result;
}

hugeint_t Hugeint::Multiply(hugeint_t lhs, hugeint_t rhs) {
	// Multiply code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	bool lhs_negative = lhs.upper < 0;
	bool rhs_negative = rhs.upper < 0;
	if (lhs_negative) {
		NegateInPlace(lhs);
	}
	if (rhs_negative) {
		NegateInPlace(rhs);
	}
	// split values into 4 32-bit parts
	uint64_t top[4] = {uint64_t(lhs.upper) >> 32, uint64_t(lhs.upper) & 0xffffffff, lhs.lower >> 32, lhs.lower & 0xffffffff};
	uint64_t bottom[4] = {uint64_t(rhs.upper) >> 32, uint64_t(rhs.upper) & 0xffffffff, rhs.lower >> 32, rhs.lower & 0xffffffff};
	uint64_t products[4][4];

	// multiply each component of the values
	for(int y = 3; y > -1; y--){
		for(int x = 3; x > -1; x--){
			products[3 - x][y] = top[x] * bottom[y];
		}
	}

	// first row
	uint64_t fourth32 = (products[0][3] & 0xffffffff);
	uint64_t third32  = (products[0][2] & 0xffffffff) + (products[0][3] >> 32);
	uint64_t second32 = (products[0][1] & 0xffffffff) + (products[0][2] >> 32);
	uint64_t first32  = (products[0][0] & 0xffffffff) + (products[0][1] >> 32);

	// second row
	third32  += (products[1][3] & 0xffffffff);
	second32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);
	first32  += (products[1][1] & 0xffffffff) + (products[1][2] >> 32);

	// third row
	second32 += (products[2][3] & 0xffffffff);
	first32  += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);

	// fourth row
	first32  += (products[3][3] & 0xffffffff);
	if (first32 & 0xffffff80000000) {
		throw std::runtime_error("Overflow in HUGEINT conversion!");
	}

	// move carry to next digit
	third32  += fourth32 >> 32;
	second32 += third32  >> 32;
	first32  += second32 >> 32;

	// remove carry from current digit
	fourth32 &= 0xffffffff;
	third32  &= 0xffffffff;
	second32 &= 0xffffffff;
	first32  &= 0xffffffff;

	// combine components
	hugeint_t result;
	result.lower = (third32 << 32) | fourth32;
	result.upper = (first32 << 32) | second32;
	if (lhs_negative ^ rhs_negative) {
		NegateInPlace(result);
	}
	return result;
}

void Hugeint::MultiplyPositiveInPlace(hugeint_t &lhs, uint32_t rhs) {
	// Simplified version of the multiply code above that accepts only a positive LHS and a uint32_t on the RHS
	assert(lhs.upper >= 0);

	// split values into 4 32-bit parts
	uint64_t top[4] = {uint64_t(lhs.upper) >> 32, uint64_t(lhs.upper) & 0xffffffff, lhs.lower >> 32, lhs.lower & 0xffffffff};
	uint64_t bottom = (uint64_t) rhs ;
	uint64_t products[4];

	// multiply each component of the values
	for(int x = 3; x > -1; x--){
		products[3 - x] = top[x] * bottom;
	}

	// first row
	uint64_t fourth32 = (products[0] & 0xffffffff);
	uint64_t third32  = (products[0] >> 32);

	// second row
	third32  += (products[1] & 0xffffffff);
	uint64_t second32 = (products[1] >> 32);

	// third row
	second32 += (products[2] & 0xffffffff);
	uint64_t first32  = (products[2] >> 32);

	// fourth row
	first32  += (products[3] & 0xffffffff);
	if (first32 & 0xffffff80000000) {
		throw std::runtime_error("Overflow in HUGEINT conversion!");
	}

	// move carry to next digit
	third32  += fourth32 >> 32;
	second32 += third32  >> 32;
	first32  += second32 >> 32;

	// remove carry from current digit
	fourth32 &= 0xffffffff;
	third32  &= 0xffffffff;
	second32 &= 0xffffffff;
	first32  &= 0xffffffff;

	// combine components
	lhs.lower = (third32 << 32) | fourth32;
	lhs.upper = (first32 << 32) | second32;
}

void Hugeint::AddInPlace(hugeint_t &lhs, hugeint_t rhs) {
	int overflow = lhs.lower + rhs.lower < lhs.lower;
	if (rhs.upper >= 0) {
		// RHS is positive: check for overflow
		if (lhs.upper > (std::numeric_limits<int64_t>::max() - rhs.upper - overflow)) {
			throw OutOfRangeException("Overflow in HUGEINT addition");
		}
	} else {
		// RHS is negative: check for underflow
		if (lhs.upper <= std::numeric_limits<int64_t>::min() + 1 - rhs.upper - overflow) {
			throw OutOfRangeException("Underflow in HUGEINT addition");
		}
	}
	lhs.upper = lhs.upper + rhs.upper + overflow;
	lhs.lower += rhs.lower;
}

void Hugeint::AddInPlace(hugeint_t &lhs, uint32_t rhs) {
	int overflow = lhs.lower + rhs < lhs.lower;
	if (lhs.upper > (std::numeric_limits<int64_t>::max() - overflow)) {
		throw OutOfRangeException("Overflow in HUGEINT addition");
	}
	lhs.upper += overflow;
	lhs.lower += rhs;
}

void Hugeint::SubtractInPlace(hugeint_t &lhs, hugeint_t rhs) {
	// underflow
	int underflow = lhs.lower - rhs.lower > lhs.lower;
	if (rhs.upper >= 0) {
		// RHS is positive: check for underflow
		if (lhs.upper < (std::numeric_limits<int64_t>::min() + rhs.upper + underflow)) {
			throw OutOfRangeException("Underflow in HUGEINT subtraction");
		}
	} else {
		// RHS is negative: check for overflow
		if (lhs.upper >= (std::numeric_limits<int64_t>::max() + rhs.upper + underflow - 1)) {
			throw OutOfRangeException("Overflow in HUGEINT subtraction");
		}
	}
	lhs.upper = lhs.upper - rhs.upper - underflow;
	lhs.lower -= rhs.lower;
}

hugeint_t Hugeint::Add(hugeint_t lhs, hugeint_t rhs) {
	AddInPlace(lhs, rhs);
	return lhs;	
}

hugeint_t Hugeint::Subtract(hugeint_t lhs, hugeint_t rhs) {
	SubtractInPlace(lhs, rhs);
	return lhs;
}

template<class DST>
bool hugeint_try_cast_integer(hugeint_t input, DST &result) {
	switch(input.upper) {
	case 0:
		// positive number: check if the positive number is in range
		if (input.lower <= std::numeric_limits<DST>::max()) {
			result = DST(input.lower);
			return true;
		}
		break;
	case -1:
		// negative number: check if the negative number is in range
		if (input.lower > std::numeric_limits<uint64_t>::max() - uint64_t(std::numeric_limits<DST>::max())) {
			result = -DST(std::numeric_limits<uint64_t>::max() - input.lower + 1);
			return true;
		}
		break;
	default:
		break;
	}
	return false;
}

template<> bool Hugeint::TryCast(hugeint_t input, int8_t &result) {
	return hugeint_try_cast_integer<int8_t>(input, result);
}

template<> bool Hugeint::TryCast(hugeint_t input, int16_t &result) {
	return hugeint_try_cast_integer<int16_t>(input, result);
}

template<> bool Hugeint::TryCast(hugeint_t input, int32_t &result) {
	return hugeint_try_cast_integer<int32_t>(input, result);
}

template<> bool Hugeint::TryCast(hugeint_t input, int64_t &result) {
	return hugeint_try_cast_integer<int64_t>(input, result);
}

template<> bool Hugeint::TryCast(hugeint_t input, float &result) {
	double dbl_result;
	Hugeint::TryCast(input, dbl_result);
	result = (float) dbl_result;
	return true;
}

template<> bool Hugeint::TryCast(hugeint_t input, double &result) {
	switch(input.upper) {
	case -1:
		// special case for upper = -1 to avoid rounding issues in small negative numbers
		result = -double(numeric_limits<uint64_t>::max() - input.lower + 1);
		break;
	default:
		result = double(input.lower) + double(input.upper) * double(numeric_limits<uint64_t>::max());
		break;
	}
	return true;
}

template<class DST>
hugeint_t hugeint_convert_integer(DST input) {
	hugeint_t result;
	result.lower = (uint64_t) input;
	result.upper = (input < 0) * -1;
	return result;
}

template<> hugeint_t Hugeint::Convert(int8_t value) {
	return hugeint_convert_integer<int8_t>(value);
}

template<> hugeint_t Hugeint::Convert(int16_t value) {
	return hugeint_convert_integer<int16_t>(value);
}

template<> hugeint_t Hugeint::Convert(int32_t value) {
	return hugeint_convert_integer<int32_t>(value);
}

template<> hugeint_t Hugeint::Convert(int64_t value) {
	return hugeint_convert_integer<int64_t>(value);
}

template<> hugeint_t Hugeint::Convert(float value) {
	return Hugeint::Convert<double>(value);
}

template<> hugeint_t Hugeint::Convert(double value) {
	if (value <= -170141183460469231731687303715884105728.0 || value >= 170141183460469231731687303715884105727.0) {
		throw OutOfRangeException("Double out of range of HUGEINT");
	}
	hugeint_t result;
	bool negative = value < 0;
	if (negative) {
		value = -value;
	}
	result.lower = (uint64_t) fmod(value, double(numeric_limits<uint64_t>::max()));
	result.upper = (uint64_t) (value / double(numeric_limits<uint64_t>::max()));
	if (negative) {
		NegateInPlace(result);
	}
	return result;
}

}
