#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

idx_t ContainsChar(const unsigned char *haystack, idx_t haystack_size, unsigned char needle) {
	// contains for a single byte: simply scan the haystack and return true if the char is found
	for(idx_t i = 0; i < haystack_size; i++) {
		if (haystack[i] == needle) {
			return i;
		}
	}
	return INVALID_INDEX;
}

template<class UNSIGNED, int NEEDLE_SIZE>
static idx_t ContainsSmall(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle) {
	// contains for a small needle (2-8 bytes)
	// we perform unsigned integer comparisons to check for equality of the entire needle in a single comparison
	// this implementation is inspired by the memmem implementation of freebsd

	// first we set up the needle and the first NEEDLE_SIZE characters of the haystack as UNSIGNED integers
	UNSIGNED needle_entry = 0;
	UNSIGNED haystack_entry = 0;
	const UNSIGNED start = (sizeof(UNSIGNED) * 8) - 8;
	const UNSIGNED shift = (sizeof(UNSIGNED) - NEEDLE_SIZE) * 8;
	for(int i = 0; i < NEEDLE_SIZE; i++) {
		needle_entry   |= UNSIGNED(needle[i])   << UNSIGNED(start - i * 8);
		haystack_entry |= UNSIGNED(haystack[i]) << UNSIGNED(start - i * 8);
	}
	// now we perform the actual search
	for(idx_t offset = NEEDLE_SIZE; offset < haystack_size; offset++) {
		// for this position we first compare the haystack with the needle
		if (haystack_entry == needle_entry) {
			return offset - NEEDLE_SIZE;
		}
		// now we adjust the haystack entry by
		// (1) removing the left-most character (shift by 8)
		// (2) adding the next character (bitwise or, with potential shift)
		// this shift is only necessary if the needle size is not aligned with the unsigned integer size
		// (e.g. needle size 3, unsigned integer size 4, we need to shift by 1)
		haystack_entry = (haystack_entry << 8) | ((UNSIGNED(haystack[offset])) << shift);
	}
	if (haystack_entry == needle_entry) {
		return haystack_size - NEEDLE_SIZE;
	}
	return INVALID_INDEX;
}

idx_t ContainsGeneric(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle, idx_t needle_size) {
	// this implementation is inspired by Raphael Javaux's faststrstr (https://github.com/RaphaelJ/fast_strstr)
	// generic contains; note that we can't use strstr because we don't have null-terminated strings anymore
	// we keep track of a shifting window sum of all characters with window size equal to needle_size
	// this shifting sum is used to avoid calling into memcmp;
	// we only need to call into memcmp when the window sum is equal to the needle sum
	// when that happens, the characters are potentially the same and we call into memcmp to check if they are
	uint32_t sums_diff = 0;
	for(idx_t i = 0; i < needle_size; i++) {
        sums_diff += haystack[i];
        sums_diff -= needle[i];
	}
	idx_t offset = 0;
	while(true) {
		if (sums_diff == 0 && haystack[offset] == needle[0]) {
			if (memcmp(haystack + offset, needle, needle_size) == 0) {
				return offset;
			}
		}
		if (offset > haystack_size - needle_size) {
			return INVALID_INDEX;
		}
        sums_diff -= haystack[offset];
        sums_diff += haystack[offset + needle_size];
		offset++;
	}
}

idx_t ContainsFun::Find(const string_t &haystack_s, const string_t &needle_s) {
	auto haystack      = (const unsigned char*) haystack_s.GetDataUnsafe();
	auto haystack_size = haystack_s.GetSize();
	auto needle        = (const unsigned char*) needle_s.GetDataUnsafe();
	auto needle_size   = needle_s.GetSize();
	if (needle_size > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return INVALID_INDEX;
	}
	// switch algorithm depending on needle size
	switch(needle_size) {
	case 0:
		// empty needle: always true
		return 0;
	case 1:
		return ContainsChar(haystack, haystack_size, *needle);
	case 2:
		return ContainsSmall<uint16_t, 2>(haystack, haystack_size, needle);
	case 3:
		return ContainsSmall<uint32_t, 3>(haystack, haystack_size, needle);
	case 4:
		return ContainsSmall<uint32_t, 4>(haystack, haystack_size, needle);
	case 5:
		return ContainsSmall<uint64_t, 5>(haystack, haystack_size, needle);
	case 6:
		return ContainsSmall<uint64_t, 6>(haystack, haystack_size, needle);
	case 7:
		return ContainsSmall<uint64_t, 7>(haystack, haystack_size, needle);
	case 8:
		return ContainsSmall<uint64_t, 8>(haystack, haystack_size, needle);
	default:
		return ContainsGeneric(haystack, haystack_size, needle, needle_size);
	}
}

struct ContainsOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return ContainsFun::Find(left, right) != INVALID_INDEX;
	}
};

ScalarFunction ContainsFun::GetFunction() {
	return ScalarFunction("contains",                                   // name of the function
	                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                      LogicalType::BOOLEAN,                         // return type
	                      ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsOperator, true>);
}

void ContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
