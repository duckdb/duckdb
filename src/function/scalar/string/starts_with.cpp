#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

template <class UNSIGNED, int NEEDLE_SIZE>
static bool StartsWithUnaligned(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle) {
	if (NEEDLE_SIZE > haystack_size) {
		// needle is bigger than haystack: haystack cannot start with needle
		return false;
	}
	// starts_with for a small unaligned needle (3/5/6/7 bytes)
	// we perform unsigned integer comparisons to check for equality of the entire needle in a single comparison
	// this implementation is inspired by the memmem implementation of freebsd

	// first we set up the needle and the first NEEDLE_SIZE characters of the haystack as UNSIGNED integers
	UNSIGNED needle_entry = 0;
	UNSIGNED haystack_entry = 0;
	const UNSIGNED start = (sizeof(UNSIGNED) * 8) - 8;
	for (int i = 0; i < NEEDLE_SIZE; i++) {
		needle_entry |= UNSIGNED(needle[i]) << UNSIGNED(start - i * 8);
		haystack_entry |= UNSIGNED(haystack[i]) << UNSIGNED(start - i * 8);
	}
	return haystack_entry == needle_entry;
}

template <class UNSIGNED>
static bool StartsWithAligned(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle) {
	if (sizeof(UNSIGNED) > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return false;
	}
	// starts_with for a small needle aligned with unsigned integer (1/2/4/8)
	// similar to StartsWithUnaligned, but simpler because we only need to do a reinterpret cast
	auto needle_entry = Load<UNSIGNED>(needle);
	auto haystack_entry = Load<UNSIGNED>(haystack);
	return needle_entry == haystack_entry;
}

static bool StartsWithGeneric(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                              idx_t needle_size) {
	if (needle_size > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return false;
	}
	// this implementation is inspired by Raphael Javaux's faststrstr (https://github.com/RaphaelJ/fast_strstr)
	// generic contains; note that we can't use strstr because we don't have null-terminated strings anymore
	// we keep track of a shifting window sum of all characters with window size equal to needle_size
	// this shifting sum is used to avoid calling into memcmp;
	// we only need to call into memcmp when the window sum is equal to the needle sum
	// when that happens, the characters are potentially the same and we call into memcmp to check if they are
	uint32_t sums_diff = 0;
	for (idx_t i = 0; i < needle_size; i++) {
		sums_diff += haystack[i];
		sums_diff -= needle[i];
	}
	return sums_diff == 0 && haystack[0] == needle[0] && memcmp(haystack, needle, needle_size) == 0;
}

static bool StartsWith(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                       idx_t needle_size) {
	D_ASSERT(needle_size > 0);
	// switch algorithm depending on needle size
	switch (needle_size) {
	case 1:
		return StartsWithAligned<uint8_t>(haystack, haystack_size, needle);
	case 2:
		return StartsWithAligned<uint16_t>(haystack, haystack_size, needle);
	case 3:
		return StartsWithUnaligned<uint32_t, 3>(haystack, haystack_size, needle);
	case 4:
		return StartsWithAligned<uint32_t>(haystack, haystack_size, needle);
	case 5:
		return StartsWithUnaligned<uint64_t, 5>(haystack, haystack_size, needle);
	case 6:
		return StartsWithUnaligned<uint64_t, 6>(haystack, haystack_size, needle);
	case 7:
		return StartsWithUnaligned<uint64_t, 7>(haystack, haystack_size, needle);
	case 8:
		return StartsWithAligned<uint64_t>(haystack, haystack_size, needle);
	default:
		return StartsWithGeneric(haystack, haystack_size, needle, needle_size);
	}
}

static bool StartsWith(const string_t &haystack_s, const string_t &needle_s) {
	auto haystack = (const unsigned char *)haystack_s.GetDataUnsafe();
	auto haystack_size = haystack_s.GetSize();
	auto needle = (const unsigned char *)needle_s.GetDataUnsafe();
	auto needle_size = needle_s.GetSize();
	if (needle_size == 0) {
		// empty needle: always true
		return true;
	}
	return StartsWith(haystack, haystack_size, needle, needle_size);
}

struct StartsWithOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return StartsWith(left, right);
	}
};

void StartsWithFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction starts_with =
	    ScalarFunction("starts_with", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                   ScalarFunction::BinaryFunction<string_t, string_t, bool, StartsWithOperator>);
	set.AddFunction(starts_with);
	starts_with.name = "^@";
	set.AddFunction(starts_with);
}

} // namespace duckdb
