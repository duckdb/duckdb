#include "duckdb/common/types/numeric_helper.hpp"

namespace duckdb {

int64_t NumericHelper::PowersOfTen[] {
	1,
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
	1000000000000000000
};

template <> int NumericHelper::UnsignedLength(uint8_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	return length;
}

template <> int NumericHelper::UnsignedLength(uint16_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	length += value >= 1000;
	length += value >= 10000;
	return length;
}

template <> int NumericHelper::UnsignedLength(uint32_t value) {
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

template <> int NumericHelper::UnsignedLength(uint64_t value) {
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

}