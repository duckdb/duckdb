#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

const int64_t NumericHelper::POWERS_OF_TEN[] {1,
                                              10,
                                              100,
                                              1000,
                                              10000,
                                              100000,
                                              1000000,
                                              10000000,
                                              100000000,
                                              1000000000,
                                              10000000000,
                                              100000000000,
                                              1000000000000,
                                              10000000000000,
                                              100000000000000,
                                              1000000000000000,
                                              10000000000000000,
                                              100000000000000000,
                                              1000000000000000000};

const double NumericHelper::DOUBLE_POWERS_OF_TEN[] {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
                                                    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
                                                    1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29,
                                                    1e30, 1e31, 1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39};

template <>
int NumericHelper::UnsignedLength(uint8_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	return length;
}

template <>
int NumericHelper::UnsignedLength(uint16_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	length += value >= 1000;
	length += value >= 10000;
	return length;
}

template <>
int NumericHelper::UnsignedLength(uint32_t value) {
	if (value >= 10000) {
		int length = 5;
		length += value >= 100000;
		length += value >= 1000000;
		length += value >= 10000000;
		length += value >= 100000000;
		length += value >= 1000000000;
		return length;
	} else {
		int length = 1;
		length += value >= 10;
		length += value >= 100;
		length += value >= 1000;
		return length;
	}
}

template <>
int NumericHelper::UnsignedLength(uint64_t value) {
	if (value >= 10000000000ULL) {
		if (value >= 1000000000000000ULL) {
			int length = 16;
			length += value >= 10000000000000000ULL;
			length += value >= 100000000000000000ULL;
			length += value >= 1000000000000000000ULL;
			length += value >= 10000000000000000000ULL;
			return length;
		} else {
			int length = 11;
			length += value >= 100000000000ULL;
			length += value >= 1000000000000ULL;
			length += value >= 10000000000000ULL;
			length += value >= 100000000000000ULL;
			return length;
		}
	} else {
		if (value >= 100000ULL) {
			int length = 6;
			length += value >= 1000000ULL;
			length += value >= 10000000ULL;
			length += value >= 100000000ULL;
			length += value >= 1000000000ULL;
			return length;
		} else {
			int length = 1;
			length += value >= 10ULL;
			length += value >= 100ULL;
			length += value >= 1000ULL;
			length += value >= 10000ULL;
			return length;
		}
	}
}

template <>
std::string NumericHelper::ToString(hugeint_t value) {
	return Hugeint::ToString(value);
}

} // namespace duckdb
