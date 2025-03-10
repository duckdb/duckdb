#include "parquet_float16.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION

#endif

namespace duckdb {

float Float16ToFloat32(const uint16_t &float16_value) {
	uint32_t sign = float16_value >> 15;
	uint32_t exponent = (float16_value >> 10) & 0x1F;
	uint32_t fraction = (float16_value & 0x3FF);
	uint32_t float32_value;
	if (exponent == 0) {
		if (fraction == 0) {
			// zero
			float32_value = (sign << 31);
		} else {
			// can be represented as ordinary value in float32
			// 2 ** -14 * 0.0101
			// => 2 ** -16 * 1.0100
			// int int_exponent = -14;
			exponent = 127 - 14;
			while ((fraction & (1 << 10)) == 0) {
				// int_exponent--;
				exponent--;
				fraction <<= 1;
			}
			fraction &= 0x3FF;
			// int_exponent += 127;
			float32_value = (sign << 31) | (exponent << 23) | (fraction << 13);
		}
	} else if (exponent == 0x1F) {
		/* Inf or NaN */
		float32_value = (sign << 31) | (0xFF << 23) | (fraction << 13);
	} else {
		/* ordinary number */
		float32_value = (sign << 31) | ((exponent + (127 - 15)) << 23) | (fraction << 13);
	}

	return *reinterpret_cast<float *>(&float32_value);
}

} // namespace duckdb
