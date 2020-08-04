#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/algorithm.hpp"
#include <limits>

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
	result.negative = buf[0] == '-';
	
	idx_t start_pos = result.negative || buf[0] == '+' ? 1 : 0;
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
				return true;
			}
			return false;
		}
		uint8_t digit = buf[pos++] - '0';
		result = Hugeint::Multiply(result, Hugeint::Convert(10));
		result.lower += digit;
		if (result.lower < digit) {
			if (result.upper == std::numeric_limits<int64_t>::max()) {
				throw OutOfRangeException("Overflow in HUGEINT conversion!");
			}
			result.upper++;
		}
	}
	return pos > start_pos;
}

static uint8_t unsigned_hugeint_highest_bit(hugeint_t bits) {
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

static bool unsigned_hugeint_is_bit_set(hugeint_t lhs, uint8_t bit_position) {
	if (bit_position < 64) {
		return lhs.lower & (uint64_t(1) << uint64_t(bit_position));
	} else {
		return lhs.upper & (uint64_t(1) << uint64_t(bit_position - 64));
	}
}

hugeint_t unsigned_hugeint_leftshift(hugeint_t lhs, uint32_t amount) {
	assert(amount > 0 && amount < 64);
	hugeint_t result;
	result.lower = lhs.lower << amount;
	result.upper = (lhs.upper << amount) + (lhs.lower >> (64 - amount));
	result.negative = false;
	return result;
}

static hugeint_t unsigned_hugeint_divmod(hugeint_t lhs, uint32_t rhs, uint32_t &remainder) {
	// DivMod code taken from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// initialize the result and remainder to 0
	hugeint_t div_result;
	div_result.lower = 0;
	div_result.upper = 0;
	remainder = 0;

	uint8_t highest_bit_set = unsigned_hugeint_highest_bit(lhs);
	// now iterate over the amount of bits that are set in the LHS
	for(uint8_t x = highest_bit_set; x > 0; x--) {
		// left-shift the current result and remainder by 1
		div_result = unsigned_hugeint_leftshift(div_result, 1);
		remainder <<= 1;
		// we get the value of the bit at position X, where position 0 is the least-significant bit
		if (unsigned_hugeint_is_bit_set(lhs, x - 1)) {
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

string Hugeint::ToString(hugeint_t input) {
	uint32_t remainder;
	string result;
	bool negative = input.negative;
	while(true) {
		if (!input.lower && !input.upper) {
			break;
		}
		input = unsigned_hugeint_divmod(input, 10, remainder);
		result = string(1, '0' + remainder) + result;
	}
	if (result.empty()) {
		// value is zero
		return "0";
	}
	return negative ? "-" + result : result;
}

hugeint_t Hugeint::Multiply(hugeint_t lhs, hugeint_t rhs) {
    // split values into 4 32-bit parts
    uint64_t top[4] = {lhs.upper >> 32, lhs.upper & 0xffffffff, lhs.lower >> 32, lhs.lower & 0xffffffff};
    uint64_t bottom[4] = {rhs.upper >> 32, rhs.upper & 0xffffffff, rhs.lower >> 32, rhs.lower & 0xffffffff};
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
		throw OutOfRangeException("Overflow in HUGEINT conversion!");
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
	result.negative = lhs.negative ^ rhs.negative;
	return result;
}

// addition of two non-negative hugeints
hugeint_t hugeint_positive_addition(hugeint_t lhs, hugeint_t rhs) {
	int overflow = lhs.lower + rhs.lower < lhs.lower;
	if ((int64_t) lhs.upper > (std::numeric_limits<int64_t>::max() - (int64_t) rhs.upper - overflow)) {
		throw OutOfRangeException("Overflow in HUGEINT addition");
	}
	lhs.upper += rhs.upper + overflow;
	lhs.lower += rhs.lower;
	return lhs;
}

hugeint_t hugeint_subtract_lhs_gt_rhs(hugeint_t lhs, hugeint_t rhs) {
	assert(Hugeint::GreaterThanEquals(lhs, rhs));
	hugeint_t result;
	result.upper = lhs.upper - rhs.upper;
	if (lhs.lower >= rhs.lower) {
		result.lower = lhs.lower - rhs.lower;
	} else {
		// we have to cascade the lower side to the upper side
		result.lower = std::numeric_limits<uint64_t>::max() - (rhs.lower - lhs.lower) + 1;
		result.upper--;
	}
	return result;
}

// subtraction of two non-negative hugeints
hugeint_t hugeint_positive_subtraction(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t result;
	if (Hugeint::GreaterThan(rhs, lhs)) {
		// if the RHS is bigger than the LHS, the result is negative: -(RHS - LHS)
		result = hugeint_subtract_lhs_gt_rhs(rhs, lhs);
		result.negative = true;
	} else {;
		// if LHS is bigger than the RHS, the result is positive (LHS - RHS)
		result = hugeint_subtract_lhs_gt_rhs(lhs, rhs);
		result.negative = false;
	}
	return result;
}

hugeint_t Hugeint::Add(hugeint_t lhs, hugeint_t rhs) {
	if (lhs.negative && rhs.negative) {
		// both LHS and RHS are negative -X + -Y
		// equivalent to -(X+Y)
		lhs.negative = false;
		rhs.negative = false;
		auto result = hugeint_positive_addition(move(lhs), move(rhs));
		result.negative = true;
		return result;
	} else if (lhs.negative) {
		// only LHS is negative -X + Y
		// equivalent to Y - X
		lhs.negative = false;
		return hugeint_positive_subtraction(move(rhs), move(lhs));
	} else if (rhs.negative) {
		// only RHS is negative, X + - Y
		// equivalent to X - Y
		rhs.negative = false;
		return hugeint_positive_subtraction(move(lhs), move(rhs));
	} else {
		// both sides are positive: regular addition
		return hugeint_positive_addition(move(lhs), move(rhs));
	}
}

hugeint_t Hugeint::Subtract(hugeint_t lhs, hugeint_t rhs) {
	if (lhs.negative && rhs.negative) {
		// both LHS and RHS are negative -X - -Y
		// equivalent to Y-X
		lhs.negative = false;
		rhs.negative = false;
		return hugeint_positive_subtraction(move(rhs), move(lhs));
	} else if (lhs.negative) {
		// only LHS is negative -X - Y
		// equivalent to -(X + Y)
		lhs.negative = false;
		auto result = hugeint_positive_addition(move(lhs), move(rhs));
		result.negative = true;
		return result;
	} else if (rhs.negative) {
		// only RHS is negative, X - - Y
		// equivalent to X + Y
		rhs.negative = false;
		return hugeint_positive_addition(move(lhs), move(rhs));
	} else {
		// both sides are positive: regular subtraction
		return hugeint_positive_subtraction(move(lhs), move(rhs));
	}
}

template<class DST>
bool hugeint_try_cast_integer(hugeint_t input, DST &result) {
	// if "upper" is set at all the number is out of range
	if (input.upper != 0) {
		return false;
	}
	// verify that the lower part is not out of bounds
	if (input.lower >= std::numeric_limits<DST>::max()) {
		return false;
	}
	result = input.negative ? -DST(input.lower) : DST(input.lower);
	return true;
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
	result = double(input.lower) + double(input.upper) * double(numeric_limits<uint64_t>::max());
	if (input.negative) {
		result = -result;
	}
	return true;
}

template<class DST>
hugeint_t hugeint_convert_integer(DST input) {
	hugeint_t result;
	if (input < 0) {
		result.lower = -input;
		result.negative = true;
	} else {
		result.negative = false;
		result.lower = input;
	}
	result.upper = 0;
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
	// set the lower 8 bytes
	hugeint_t result;
	if (value < 0) {
		result.negative = true;
		value = -value;
	} else {
		result.negative = false;
	}
	result.lower = (uint64_t) fmod(value, double(numeric_limits<uint64_t>::max()));
	result.upper = (uint64_t) (value / double(numeric_limits<uint64_t>::max()));
	return result;
}

}
