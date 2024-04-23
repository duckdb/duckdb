#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

template <class UNSIGNED, int NEEDLE_SIZE>
static idx_t ContainsUnaligned(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                               idx_t base_offset) {
	if (NEEDLE_SIZE > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return DConstants::INVALID_INDEX;
	}
	// contains for a small unaligned needle (3/5/6/7 bytes)
	// we perform unsigned integer comparisons to check for equality of the entire needle in a single comparison
	// this implementation is inspired by the memmem implementation of freebsd

	// first we set up the needle and the first NEEDLE_SIZE characters of the haystack as UNSIGNED integers
	UNSIGNED needle_entry = 0;
	UNSIGNED haystack_entry = 0;
	const UNSIGNED start = (sizeof(UNSIGNED) * 8) - 8;
	const UNSIGNED shift = (sizeof(UNSIGNED) - NEEDLE_SIZE) * 8;
	for (idx_t i = 0; i < NEEDLE_SIZE; i++) {
		needle_entry |= UNSIGNED(needle[i]) << UNSIGNED(start - i * 8);
		haystack_entry |= UNSIGNED(haystack[i]) << UNSIGNED(start - i * 8);
	}
	// now we perform the actual search
	for (idx_t offset = NEEDLE_SIZE; offset < haystack_size; offset++) {
		// for this position we first compare the haystack with the needle
		if (haystack_entry == needle_entry) {
			return base_offset + offset - NEEDLE_SIZE;
		}
		// now we adjust the haystack entry by
		// (1) removing the left-most character (shift by 8)
		// (2) adding the next character (bitwise or, with potential shift)
		// this shift is only necessary if the needle size is not aligned with the unsigned integer size
		// (e.g. needle size 3, unsigned integer size 4, we need to shift by 1)
		haystack_entry = (haystack_entry << 8) | ((UNSIGNED(haystack[offset])) << shift);
	}
	if (haystack_entry == needle_entry) {
		return base_offset + haystack_size - NEEDLE_SIZE;
	}
	return DConstants::INVALID_INDEX;
}

template <class UNSIGNED>
static idx_t ContainsAligned(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                             idx_t base_offset) {
	if (sizeof(UNSIGNED) > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return DConstants::INVALID_INDEX;
	}
	// contains for a small needle aligned with unsigned integer (2/4/8)
	// similar to ContainsUnaligned, but simpler because we only need to do a reinterpret cast
	auto needle_entry = Load<UNSIGNED>(needle);
	for (idx_t offset = 0; offset <= haystack_size - sizeof(UNSIGNED); offset++) {
		// for this position we first compare the haystack with the needle
		auto haystack_entry = Load<UNSIGNED>(haystack + offset);
		if (needle_entry == haystack_entry) {
			return base_offset + offset;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t ContainsGeneric(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                      idx_t needle_size, idx_t base_offset) {
	if (needle_size > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return DConstants::INVALID_INDEX;
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
	idx_t offset = 0;
	while (true) {
		if (sums_diff == 0 && haystack[offset] == needle[0]) {
			if (memcmp(haystack + offset, needle, needle_size) == 0) {
				return base_offset + offset;
			}
		}
		if (offset >= haystack_size - needle_size) {
			return DConstants::INVALID_INDEX;
		}
		sums_diff -= haystack[offset];
		sums_diff += haystack[offset + needle_size];
		offset++;
	}
}

idx_t ContainsFun::Find(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                        idx_t needle_size) {
	D_ASSERT(needle_size > 0);
	// start off by performing a memchr to find the first character of the
	auto location = memchr(haystack, needle[0], haystack_size);
	if (location == nullptr) {
		return DConstants::INVALID_INDEX;
	}
	idx_t base_offset = UnsafeNumericCast<idx_t>(const_uchar_ptr_cast(location) - haystack);
	haystack_size -= base_offset;
	haystack = const_uchar_ptr_cast(location);
	// switch algorithm depending on needle size
	switch (needle_size) {
	case 1:
		return base_offset;
	case 2:
		return ContainsAligned<uint16_t>(haystack, haystack_size, needle, base_offset);
	case 3:
		return ContainsUnaligned<uint32_t, 3>(haystack, haystack_size, needle, base_offset);
	case 4:
		return ContainsAligned<uint32_t>(haystack, haystack_size, needle, base_offset);
	case 5:
		return ContainsUnaligned<uint64_t, 5>(haystack, haystack_size, needle, base_offset);
	case 6:
		return ContainsUnaligned<uint64_t, 6>(haystack, haystack_size, needle, base_offset);
	case 7:
		return ContainsUnaligned<uint64_t, 7>(haystack, haystack_size, needle, base_offset);
	case 8:
		return ContainsAligned<uint64_t>(haystack, haystack_size, needle, base_offset);
	default:
		return ContainsGeneric(haystack, haystack_size, needle, needle_size, base_offset);
	}
}

idx_t ContainsFun::Find(const string_t &haystack_s, const string_t &needle_s) {
	auto haystack = const_uchar_ptr_cast(haystack_s.GetData());
	auto haystack_size = haystack_s.GetSize();
	auto needle = const_uchar_ptr_cast(needle_s.GetData());
	auto needle_size = needle_s.GetSize();
	if (needle_size == 0) {
		// empty needle: always true
		return 0;
	}
	return ContainsFun::Find(haystack, haystack_size, needle, needle_size);
}

struct ContainsOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return ContainsFun::Find(left, right) != DConstants::INVALID_INDEX;
	}
};

ScalarFunction ContainsFun::GetFunction() {
	return ScalarFunction("contains",                                   // name of the function
	                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                      LogicalType::BOOLEAN,                         // return type
	                      ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsOperator>);
}

void ContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
