#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"

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
int NumericHelper::UnsignedLength(hugeint_t value) {
	D_ASSERT(value.upper >= 0);
	if (value.upper == 0) {
		return UnsignedLength<uint64_t>(value.lower);
	}
	// search the length using the POWERS_OF_TEN array
	// the length has to be between [17] and [38], because the hugeint is bigger than 2^63
	// we use the same approach as above, but split a bit more because comparisons for hugeints are more expensive
	if (value >= Hugeint::POWERS_OF_TEN[27]) {
		// [27..38]
		if (value >= Hugeint::POWERS_OF_TEN[32]) {
			if (value >= Hugeint::POWERS_OF_TEN[36]) {
				int length = 37;
				length += value >= Hugeint::POWERS_OF_TEN[37];
				length += value >= Hugeint::POWERS_OF_TEN[38];
				return length;
			} else {
				int length = 33;
				length += value >= Hugeint::POWERS_OF_TEN[33];
				length += value >= Hugeint::POWERS_OF_TEN[34];
				length += value >= Hugeint::POWERS_OF_TEN[35];
				return length;
			}
		} else {
			if (value >= Hugeint::POWERS_OF_TEN[30]) {
				int length = 31;
				length += value >= Hugeint::POWERS_OF_TEN[31];
				length += value >= Hugeint::POWERS_OF_TEN[32];
				return length;
			} else {
				int length = 28;
				length += value >= Hugeint::POWERS_OF_TEN[28];
				length += value >= Hugeint::POWERS_OF_TEN[29];
				return length;
			}
		}
	} else {
		// [17..27]
		if (value >= Hugeint::POWERS_OF_TEN[22]) {
			// [22..27]
			if (value >= Hugeint::POWERS_OF_TEN[25]) {
				int length = 26;
				length += value >= Hugeint::POWERS_OF_TEN[26];
				return length;
			} else {
				int length = 23;
				length += value >= Hugeint::POWERS_OF_TEN[23];
				length += value >= Hugeint::POWERS_OF_TEN[24];
				return length;
			}
		} else {
			// [17..22]
			if (value >= Hugeint::POWERS_OF_TEN[20]) {
				int length = 21;
				length += value >= Hugeint::POWERS_OF_TEN[21];
				return length;
			} else {
				int length = 18;
				length += value >= Hugeint::POWERS_OF_TEN[18];
				length += value >= Hugeint::POWERS_OF_TEN[19];
				return length;
			}
		}
	}
}

template <>
string_t NumericHelper::FormatSigned(hugeint_t value, Vector &vector) {
	int negative = value.upper < 0;
	if (negative) {
		if (value == NumericLimits<hugeint_t>::Minimum()) {
			string_t result = StringVector::AddString(vector, Hugeint::HUGEINT_MINIMUM_STRING);
			return result;
		}
		Hugeint::NegateInPlace(value);
	}
	int length = UnsignedLength(value) + negative;
	string_t result = StringVector::EmptyString(vector, NumericCast<size_t>(length));
	auto dataptr = result.GetDataWriteable();
	auto endptr = dataptr + length;
	if (value.upper == 0) {
		// small value: format as uint64_t
		endptr = NumericHelper::FormatUnsigned<uint64_t>(value.lower, endptr);
	} else {
		endptr = FormatUnsigned(value, endptr);
	}
	if (negative) {
		*--endptr = '-';
	}
	D_ASSERT(endptr == dataptr);
	result.Finalize();
	return result;
}

template <>
std::string NumericHelper::ToString(hugeint_t value) {
	return Hugeint::ToString(value);
}

template <>
std::string NumericHelper::ToString(uhugeint_t value) {
	return Uhugeint::ToString(value);
}

} // namespace duckdb
