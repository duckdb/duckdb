#include "lib/ryu.hpp"

#include <cstring>

namespace duckdb {

static inline uint64_t umul128(const uint64_t a, const uint64_t b, uint64_t *const productHi) {
	// The casts here help MSVC to avoid calls to the __allmul library function.
	const uint32_t aLo = (uint32_t)a;
	const uint32_t aHi = (uint32_t)(a >> 32);
	const uint32_t bLo = (uint32_t)b;
	const uint32_t bHi = (uint32_t)(b >> 32);

	const uint64_t b00 = (uint64_t)aLo * bLo;
	const uint64_t b01 = (uint64_t)aLo * bHi;
	const uint64_t b10 = (uint64_t)aHi * bLo;
	const uint64_t b11 = (uint64_t)aHi * bHi;

	const uint32_t b00Lo = (uint32_t)b00;
	const uint32_t b00Hi = (uint32_t)(b00 >> 32);

	const uint64_t mid1 = b10 + b00Hi;
	const uint32_t mid1Lo = (uint32_t)(mid1);
	const uint32_t mid1Hi = (uint32_t)(mid1 >> 32);

	const uint64_t mid2 = b01 + mid1Lo;
	const uint32_t mid2Lo = (uint32_t)(mid2);
	const uint32_t mid2Hi = (uint32_t)(mid2 >> 32);

	const uint64_t pHi = b11 + mid1Hi + mid2Hi;
	const uint64_t pLo = ((uint64_t)mid2Lo << 32) | b00Lo;

	*productHi = pHi;
	return pLo;
}

static inline uint64_t shiftright128(const uint64_t lo, const uint64_t hi, const uint32_t dist) {
	// We don't need to handle the case dist >= 64 here (see above).
	assert(dist < 64);
	// Avoid a 64-bit shift by taking advantage of the range of shift values.
	assert(dist >= 32);
	return (hi << (64 - dist)) | ((uint32_t)(lo >> 32) >> (dist - 32));
}

static inline uint64_t mulShift(const uint64_t m, const uint64_t *const mul, const int32_t j) {
	// m is maximum 55 bits
	uint64_t high1;                                   // 128
	const uint64_t low1 = umul128(m, mul[1], &high1); // 64
	uint64_t high0;                                   // 64
	umul128(m, mul[0], &high0);                       // 0
	const uint64_t sum = high0 + low1;
	if (sum < high0) {
		++high1; // overflow into high1
	}
	return shiftright128(sum, high1, j - 64);
}

static inline uint64_t mulShiftAll(const uint64_t m, const uint64_t *const mul, const int32_t j, uint64_t *const vp,
                                   uint64_t *const vm, const uint32_t mmShift) {
	*vp = mulShift(4 * m + 2, mul, j);
	*vm = mulShift(4 * m - 1 - mmShift, mul, j);
	return mulShift(4 * m, mul, j);
}

// A floating decimal representing m * 10^e.
typedef struct floating_decimal_64 {
	uint64_t mantissa;
	// Decimal exponent's range is -324 to 308
	// inclusive, and can fit in a short if needed.
	int32_t exponent;
} floating_decimal_64;

static inline bool d2d_small_int(const uint64_t ieeeMantissa, const uint32_t ieeeExponent,
                                 floating_decimal_64 *const v) {
	const uint64_t m2 = (1ull << DOUBLE_MANTISSA_BITS) | ieeeMantissa;
	const int32_t e2 = (int32_t)ieeeExponent - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS;

	if (e2 > 0) {
		// f = m2 * 2^e2 >= 2^53 is an integer.
		// Ignore this case for now.
		return false;
	}

	if (e2 < -52) {
		// f < 1.
		return false;
	}

	// Since 2^52 <= m2 < 2^53 and 0 <= -e2 <= 52: 1 <= f = m2 / 2^-e2 < 2^53.
	// Test if the lower -e2 bits of the significand are 0, i.e. whether the fraction is 0.
	const uint64_t mask = (1ull << -e2) - 1;
	const uint64_t fraction = m2 & mask;
	if (fraction != 0) {
		return false;
	}

	// f is an integer in the range [1, 2^53).
	// Note: mantissa might contain trailing (decimal) 0's.
	// Note: since 2^53 < 10^16, there is no need to adjust decimalLength17().
	v->mantissa = m2 >> -e2;
	v->exponent = 0;
	return true;
}

static inline floating_decimal_64 d2d(const uint64_t ieeeMantissa, const uint32_t ieeeExponent) {
	int32_t e2;
	uint64_t m2;
	if (ieeeExponent == 0) {
		// We subtract 2 so that the bounds computation has 2 additional bits.
		e2 = 1 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS - 2;
		m2 = ieeeMantissa;
	} else {
		e2 = (int32_t)ieeeExponent - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS - 2;
		m2 = (1ull << DOUBLE_MANTISSA_BITS) | ieeeMantissa;
	}
	const bool even = (m2 & 1) == 0;
	const bool acceptBounds = even;

	// Step 2: Determine the interval of valid decimal representations.
	const uint64_t mv = 4 * m2;
	// Implicit bool -> int conversion. True is 1, false is 0.
	const uint32_t mmShift = ieeeMantissa != 0 || ieeeExponent <= 1;
	// We would compute mp and mm like this:
	// uint64_t mp = 4 * m2 + 2;
	// uint64_t mm = mv - 1 - mmShift;

	// Step 3: Convert to a decimal power base using 128-bit arithmetic.
	uint64_t vr, vp, vm;
	int32_t e10;
	bool vmIsTrailingZeros = false;
	bool vrIsTrailingZeros = false;
	if (e2 >= 0) {
		// I tried special-casing q == 0, but there was no effect on performance.
		// This expression is slightly faster than max(0, log10Pow2(e2) - 1).
		const uint32_t q = log10Pow2(e2) - (e2 > 3);
		e10 = (int32_t)q;
		const int32_t k = DOUBLE_POW5_INV_BITCOUNT + pow5bits((int32_t)q) - 1;
		const int32_t i = -e2 + (int32_t)q + k;

		vr = mulShiftAll(m2, DOUBLE_POW5_INV_SPLIT[q], i, &vp, &vm, mmShift);
		if (q <= 21) {
			// This should use q <= 22, but I think 21 is also safe. Smaller values
			// may still be safe, but it's more difficult to reason about them.
			// Only one of mp, mv, and mm can be a multiple of 5, if any.
			const uint32_t mvMod5 = ((uint32_t)mv) - 5 * ((uint32_t)div5(mv));
			if (mvMod5 == 0) {
				vrIsTrailingZeros = multipleOfPowerOf5(mv, q);
			} else if (acceptBounds) {
				// Same as min(e2 + (~mm & 1), pow5Factor(mm)) >= q
				// <=> e2 + (~mm & 1) >= q && pow5Factor(mm) >= q
				// <=> true && pow5Factor(mm) >= q, since e2 >= q.
				vmIsTrailingZeros = multipleOfPowerOf5(mv - 1 - mmShift, q);
			} else {
				// Same as min(e2 + 1, pow5Factor(mp)) >= q.
				vp -= multipleOfPowerOf5(mv + 2, q);
			}
		}
	} else {
		// This expression is slightly faster than max(0, log10Pow5(-e2) - 1).
		const uint32_t q = log10Pow5(-e2) - (-e2 > 1);
		e10 = (int32_t)q + e2;
		const int32_t i = -e2 - (int32_t)q;
		const int32_t k = pow5bits(i) - DOUBLE_POW5_BITCOUNT;
		const int32_t j = (int32_t)q - k;
		vr = mulShiftAll(m2, DOUBLE_POW5_SPLIT[i], j, &vp, &vm, mmShift);
		if (q <= 1) {
			// {vr,vp,vm} is trailing zeros if {mv,mp,mm} has at least q trailing 0 bits.
			// mv = 4 * m2, so it always has at least two trailing 0 bits.
			vrIsTrailingZeros = true;
			if (acceptBounds) {
				// mm = mv - 1 - mmShift, so it has 1 trailing 0 bit iff mmShift == 1.
				vmIsTrailingZeros = mmShift == 1;
			} else {
				// mp = mv + 2, so it always has at least one trailing 0 bit.
				--vp;
			}
		} else if (q < 63) { // TODO(ulfjack): Use a tighter bound here.
			// We want to know if the full product has at least q trailing zeros.
			// We need to compute min(p2(mv), p5(mv) - e2) >= q
			// <=> p2(mv) >= q && p5(mv) - e2 >= q
			// <=> p2(mv) >= q (because -e2 >= q)
			vrIsTrailingZeros = multipleOfPowerOf2(mv, q);
		}
	}

	// Step 4: Find the shortest decimal representation in the interval of valid representations.
	int32_t removed = 0;
	uint8_t lastRemovedDigit = 0;
	uint64_t output;
	// On average, we remove ~2 digits.
	if (vmIsTrailingZeros || vrIsTrailingZeros) {
		// General case, which happens rarely (~0.7%).
		for (;;) {
			const uint64_t vpDiv10 = div10(vp);
			const uint64_t vmDiv10 = div10(vm);
			if (vpDiv10 <= vmDiv10) {
				break;
			}
			const uint32_t vmMod10 = ((uint32_t)vm) - 10 * ((uint32_t)vmDiv10);
			const uint64_t vrDiv10 = div10(vr);
			const uint32_t vrMod10 = ((uint32_t)vr) - 10 * ((uint32_t)vrDiv10);
			vmIsTrailingZeros &= vmMod10 == 0;
			vrIsTrailingZeros &= lastRemovedDigit == 0;
			lastRemovedDigit = (uint8_t)vrMod10;
			vr = vrDiv10;
			vp = vpDiv10;
			vm = vmDiv10;
			++removed;
		}
		if (vmIsTrailingZeros) {
			for (;;) {
				const uint64_t vmDiv10 = div10(vm);
				const uint32_t vmMod10 = ((uint32_t)vm) - 10 * ((uint32_t)vmDiv10);
				if (vmMod10 != 0) {
					break;
				}
				const uint64_t vpDiv10 = div10(vp);
				const uint64_t vrDiv10 = div10(vr);
				const uint32_t vrMod10 = ((uint32_t)vr) - 10 * ((uint32_t)vrDiv10);
				vrIsTrailingZeros &= lastRemovedDigit == 0;
				lastRemovedDigit = (uint8_t)vrMod10;
				vr = vrDiv10;
				vp = vpDiv10;
				vm = vmDiv10;
				++removed;
			}
		}
		if (vrIsTrailingZeros && lastRemovedDigit == 5 && vr % 2 == 0) {
			// Round even if the exact number is .....50..0.
			lastRemovedDigit = 4;
		}
		// We need to take vr + 1 if vr is outside bounds or we need to round up.
		output = vr + ((vr == vm && (!acceptBounds || !vmIsTrailingZeros)) || lastRemovedDigit >= 5);
	} else {
		// Specialized for the common case (~99.3%). Percentages below are relative to this.
		bool roundUp = false;
		const uint64_t vpDiv100 = div100(vp);
		const uint64_t vmDiv100 = div100(vm);
		if (vpDiv100 > vmDiv100) { // Optimization: remove two digits at a time (~86.2%).
			const uint64_t vrDiv100 = div100(vr);
			const uint32_t vrMod100 = ((uint32_t)vr) - 100 * ((uint32_t)vrDiv100);
			roundUp = vrMod100 >= 50;
			vr = vrDiv100;
			vp = vpDiv100;
			vm = vmDiv100;
			removed += 2;
		}
		// Loop iterations below (approximately), without optimization above:
		// 0: 0.03%, 1: 13.8%, 2: 70.6%, 3: 14.0%, 4: 1.40%, 5: 0.14%, 6+: 0.02%
		// Loop iterations below (approximately), with optimization above:
		// 0: 70.6%, 1: 27.8%, 2: 1.40%, 3: 0.14%, 4+: 0.02%
		for (;;) {
			const uint64_t vpDiv10 = div10(vp);
			const uint64_t vmDiv10 = div10(vm);
			if (vpDiv10 <= vmDiv10) {
				break;
			}
			const uint64_t vrDiv10 = div10(vr);
			const uint32_t vrMod10 = ((uint32_t)vr) - 10 * ((uint32_t)vrDiv10);
			roundUp = vrMod10 >= 5;
			vr = vrDiv10;
			vp = vpDiv10;
			vm = vmDiv10;
			++removed;
		}
		// We need to take vr + 1 if vr is outside bounds or we need to round up.
		output = vr + (vr == vm || roundUp);
	}
	const int32_t exp = e10 + removed;

	floating_decimal_64 fd;
	fd.exponent = exp;
	fd.mantissa = output;
	return fd;
}

static inline uint32_t decimalLength17(const uint64_t v) {
	// This is slightly faster than a loop.
	// The average output length is 16.38 digits, so we check high-to-low.
	// Function precondition: v is not an 18, 19, or 20-digit number.
	// (17 digits are sufficient for round-tripping.)
	assert(v < 100000000000000000L);
	if (v >= 10000000000000000L) {
		return 17;
	}
	if (v >= 1000000000000000L) {
		return 16;
	}
	if (v >= 100000000000000L) {
		return 15;
	}
	if (v >= 10000000000000L) {
		return 14;
	}
	if (v >= 1000000000000L) {
		return 13;
	}
	if (v >= 100000000000L) {
		return 12;
	}
	if (v >= 10000000000L) {
		return 11;
	}
	if (v >= 1000000000L) {
		return 10;
	}
	if (v >= 100000000L) {
		return 9;
	}
	if (v >= 10000000L) {
		return 8;
	}
	if (v >= 1000000L) {
		return 7;
	}
	if (v >= 100000L) {
		return 6;
	}
	if (v >= 10000L) {
		return 5;
	}
	if (v >= 1000L) {
		return 4;
	}
	if (v >= 100L) {
		return 3;
	}
	if (v >= 10L) {
		return 2;
	}
	return 1;
}

static inline uint64_t pow_10(const int32_t exp) {
	static const uint64_t POW_TABLE[18] = {1ULL,
	                                       10ULL,
	                                       100ULL,
	                                       1000ULL,
	                                       10000ULL,

	                                       100000ULL,
	                                       1000000ULL,
	                                       10000000ULL,
	                                       100000000ULL,
	                                       1000000000ULL,

	                                       10000000000ULL,
	                                       100000000000ULL,
	                                       1000000000000ULL,
	                                       10000000000000ULL,
	                                       100000000000000ULL,

	                                       1000000000000000ULL,
	                                       10000000000000000ULL,
	                                       100000000000000000ULL};
	assert(exp <= 17);
	assert(exp >= 0);
	return POW_TABLE[exp];
}

static inline int to_chars_uint64(uint64_t output, uint32_t olength, char *const result) {
	uint32_t i = 0;

	// We prefer 32-bit operations, even on 64-bit platforms.
	// We have at most 17 digits, and uint32_t can store 9 digits.
	// If output doesn't fit into uint32_t, we cut off 8 digits,
	// so the rest will fit into uint32_t.
	if ((output >> 32) != 0) {
		// Expensive 64-bit division.
		const uint64_t q = div1e8(output);
		uint32_t output2 = ((uint32_t)output) - 100000000 * ((uint32_t)q);
		output = q;

		const uint32_t c = output2 % 10000;
		output2 /= 10000;
		const uint32_t d = output2 % 10000;
		const uint32_t c0 = (c % 100) << 1;
		const uint32_t c1 = (c / 100) << 1;
		const uint32_t d0 = (d % 100) << 1;
		const uint32_t d1 = (d / 100) << 1;
		memcpy(result + olength - i - 2, DIGIT_TABLE + c0, 2);
		memcpy(result + olength - i - 4, DIGIT_TABLE + c1, 2);
		memcpy(result + olength - i - 6, DIGIT_TABLE + d0, 2);
		memcpy(result + olength - i - 8, DIGIT_TABLE + d1, 2);
		i += 8;
	}

	uint32_t output2 = (uint32_t)output;
	while (output2 >= 10000) {
#ifdef __clang__ // https://bugs.llvm.org/show_bug.cgi?id=38217
		const uint32_t c = output2 - 10000 * (output2 / 10000);
#else
		const uint32_t c = output2 % 10000;
#endif
		output2 /= 10000;
		const uint32_t c0 = (c % 100) << 1;
		const uint32_t c1 = (c / 100) << 1;
		memcpy(result + olength - i - 2, DIGIT_TABLE + c0, 2);
		memcpy(result + olength - i - 4, DIGIT_TABLE + c1, 2);
		i += 4;
	}

	if (output2 >= 100) {
#ifdef __clang__ // https://bugs.llvm.org/show_bug.cgi?id=38217
		const uint32_t c = (output2 % 100) << 1;
#else
		const uint32_t c = (output2 - 100 * (output2 / 100)) << 1;
#endif
		output2 /= 100;
		memcpy(result + olength - i - 2, DIGIT_TABLE + c, 2);
		i += 2;
	}
	if (output2 >= 10) {
		const uint32_t c = output2 << 1;
		memcpy(result + olength - i - 2, DIGIT_TABLE + c, 2);
		i += 2;
	} else {
		result[0] = (char)('0' + output2);
		i += 1;
	}

	return i;
}

static inline int to_chars_fixed(const floating_decimal_64 v, const bool sign, uint32_t precision, char *const result) {
	uint64_t output = v.mantissa;
	uint32_t olength = decimalLength17(output);
	int32_t exp = v.exponent;
	uint64_t integer_part;
	uint32_t integer_part_length = 0;
	uint64_t decimal_part;
	uint32_t decimal_part_length = 0;
	uint32_t trailing_integer_zeros = 0;
	uint32_t leading_decimal_zeros = 0;

	if (exp >= 0) {
		integer_part = output;
		integer_part_length = olength;
		trailing_integer_zeros = exp;
		decimal_part = 0;
	} else {
		/* Adapt the decimal digits to the desired precision */
		if (precision < (uint32_t)-exp) {
			int32_t digits_to_trim = -exp - precision;
			if (digits_to_trim > (int32_t)olength) {
				output = 0;
				exp = 0;
			} else {
				const uint64_t divisor = pow_10(digits_to_trim);
				const uint64_t divisor_half = divisor / 2;
				const uint64_t outputDiv = output / divisor;
				const uint64_t remainder = output - outputDiv * divisor;

				output = outputDiv;
				exp += digits_to_trim;

				if (remainder > divisor_half || (remainder == divisor_half && (output & 1))) {
					output++;
					olength = decimalLength17(output);
				} else {
					olength -= digits_to_trim;
				}

				while (output && output % 10 == 0) {
					output = div10(output);
					exp++;
					olength--;
				}
			}
		}

		int32_t nexp = -exp;
		if (exp >= 0) {
			integer_part = output;
			integer_part_length = olength;
			trailing_integer_zeros = exp;
			decimal_part = 0;
		} else if (nexp < (int32_t)olength) {
			uint64_t p = pow_10(nexp);
			integer_part = output / p;
			decimal_part = output % p;
			integer_part_length = olength - nexp;
			decimal_part_length = olength - integer_part_length;
			if (decimal_part < pow_10(decimal_part_length - 1)) {
				/* The decimal part had leading zeros (e.g. 123.0001) which were lost */
				decimal_part_length = decimalLength17(decimal_part);
				leading_decimal_zeros = olength - integer_part_length - decimal_part_length;
			}
		} else {
			integer_part = 0;
			decimal_part = output;
			decimal_part_length = olength;
			leading_decimal_zeros = nexp - olength;
		}
	}

	/* If we have removed all digits, it may happen that we have -0 and we want it to be just 0 */
	int index = 0;
	if (sign && (integer_part || decimal_part)) {
		result[index++] = '-';
	}

	index += to_chars_uint64(integer_part, integer_part_length, &result[index]);
	for (uint32_t i = 0; i < trailing_integer_zeros; i++)
		result[index++] = '0';

	if (decimal_part) {
		result[index++] = '.';
		for (uint32_t i = 0; i < leading_decimal_zeros; i++)
			result[index++] = '0';
		index += to_chars_uint64(decimal_part, decimal_part_length, &result[index]);
	}

	return index;
}

int d2sfixed_buffered_n(double f, uint32_t precision, char *result) {
	// Step 1: Decode the floating-point number, and unify normalized and subnormal cases.
	const uint64_t bits = double_to_bits(f);

	// Decode bits into sign, mantissa, and exponent.
	const bool ieeeSign = ((bits >> (DOUBLE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS)) & 1) != 0;
	const uint64_t ieeeMantissa = bits & ((1ull << DOUBLE_MANTISSA_BITS) - 1);
	const uint32_t ieeeExponent = (uint32_t)((bits >> DOUBLE_MANTISSA_BITS) & ((1u << DOUBLE_EXPONENT_BITS) - 1));
	// Case distinction; exit early for the easy cases.
	if (ieeeExponent == ((1u << DOUBLE_EXPONENT_BITS) - 1u) || (ieeeExponent == 0 && ieeeMantissa == 0)) {
		return copy_special_str(result, ieeeSign, ieeeExponent, ieeeMantissa);
	}

	floating_decimal_64 v;
	const bool isSmallInt = d2d_small_int(ieeeMantissa, ieeeExponent, &v);
	if (isSmallInt) {
		// For small integers in the range [1, 2^53), v.mantissa might contain trailing (decimal) zeros.
		// For scientific notation we need to move these zeros into the exponent.
		// (This is not needed for fixed-point notation, so it might be beneficial to trim
		// trailing zeros in to_chars only if needed - once fixed-point notation output is implemented.)
		for (;;) {
			const uint64_t q = div10(v.mantissa);
			const uint32_t r = ((uint32_t)v.mantissa) - 10 * ((uint32_t)q);
			if (r != 0) {
				break;
			}
			v.mantissa = q;
			++v.exponent;
		}
	} else {
		v = d2d(ieeeMantissa, ieeeExponent);
	}

	return to_chars_fixed(v, ieeeSign, precision, result);
}

int d2sexp_buffered_n(double f, uint32_t precision, char *result) {
	// Step 1: Decode the floating-point number, and unify normalized and subnormal cases.
	const uint64_t bits = double_to_bits(f);

	// Decode bits into sign, mantissa, and exponent.
	const bool ieeeSign = ((bits >> (DOUBLE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS)) & 1) != 0;
	const uint64_t ieeeMantissa = bits & ((1ull << DOUBLE_MANTISSA_BITS) - 1);
	const uint32_t ieeeExponent = (uint32_t)((bits >> DOUBLE_MANTISSA_BITS) & ((1u << DOUBLE_EXPONENT_BITS) - 1));
	// Case distinction; exit early for the easy cases.
	if (ieeeExponent == ((1u << DOUBLE_EXPONENT_BITS) - 1u) || (ieeeExponent == 0 && ieeeMantissa == 0)) {
		return copy_special_str(result, ieeeSign, ieeeExponent, ieeeMantissa);
	}

	floating_decimal_64 v;
	const bool isSmallInt = d2d_small_int(ieeeMantissa, ieeeExponent, &v);
	if (isSmallInt) {
		// For small integers in the range [1, 2^53), v.mantissa might contain trailing (decimal) zeros.
		// For scientific notation we need to move these zeros into the exponent.
		// (This is not needed for fixed-point notation, so it might be beneficial to trim
		// trailing zeros in to_chars only if needed - once fixed-point notation output is implemented.)
		for (;;) {
			const uint64_t q = div10(v.mantissa);
			const uint32_t r = ((uint32_t)v.mantissa) - 10 * ((uint32_t)q);
			if (r != 0) {
				break;
			}
			v.mantissa = q;
			++v.exponent;
		}
	} else {
		v = d2d(ieeeMantissa, ieeeExponent);
	}

	// Print first the mantissa using the fixed point notation, then add the exponent manually
	const int32_t olength = (int32_t)decimalLength17(v.mantissa);
	const int32_t original_ieeeExponent = v.exponent + olength - 1;
	v.exponent = 1 - olength;
	int index = to_chars_fixed(v, ieeeSign, precision, result);

	// Print the exponent.
	result[index++] = 'e';
	int32_t exp = original_ieeeExponent;
	if (exp < 0) {
		result[index++] = '-';
		exp = -exp;
	} else {
		result[index++] = '+';
	}

	if (exp >= 100) {
		const int32_t c = exp % 10;
		memcpy(result + index, DIGIT_TABLE + 2 * (exp / 10), 2);
		result[index + 2] = (char)('0' + c);
		index += 3;
	} else if (exp >= 10) {
		memcpy(result + index, DIGIT_TABLE + 2 * exp, 2);
		index += 2;
	} else {
		result[index++] = (char)('0' + exp);
	}

	return index;
}

} // namespace duckdb