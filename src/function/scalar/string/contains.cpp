#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/map_functions.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

#ifndef DUCKDB_SMALLER_BINARY
template <class UNSIGNED, idx_t NEEDLE_SIZE>
static idx_t ContainsUnaligned(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle, idx_t base_offset) {
	if (NEEDLE_SIZE > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return DConstants::INVALID_INDEX;
	}
	haystack_size -= NEEDLE_SIZE - 1;
	auto needle_entry = Load<UNSIGNED>(needle);
	for(idx_t offset = 0; offset < haystack_size; offset++) {
		// start off by performing a memchr to find the first character of the
		auto location = static_cast<const unsigned char *>(memchr(haystack + offset, needle[0], haystack_size - offset));
		if (!location) {
			return DConstants::INVALID_INDEX;
		}
		// for this position we first compare the haystack with the needle
		offset = UnsafeNumericCast<idx_t>(location - haystack);
		auto haystack_entry = Load<UNSIGNED>(location);
		if (needle_entry == haystack_entry) {
			idx_t matches = 0;
			for(idx_t i = sizeof(UNSIGNED); i < NEEDLE_SIZE; i++) {
				matches += location[i] == needle[i];
			}
			if (matches == NEEDLE_SIZE - sizeof(UNSIGNED)) {
				return base_offset + offset;
			}
		}
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
	haystack_size -= sizeof(UNSIGNED) - 1;
	auto needle_entry = Load<UNSIGNED>(needle);
	for(idx_t offset = 0; offset < haystack_size; offset++) {
		// start off by performing a memchr to find the first character of the
		auto location = static_cast<const unsigned char *>(memchr(haystack + offset, needle[0], haystack_size - offset));
		if (!location) {
			return DConstants::INVALID_INDEX;
		}
		// for this position we first compare the haystack with the needle
		offset = UnsafeNumericCast<idx_t>(location - haystack);
		auto haystack_entry = Load<UNSIGNED>(location);
		if (needle_entry == haystack_entry) {
			return base_offset + offset;
		}
	}
	return DConstants::INVALID_INDEX;
}
#endif

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

idx_t FindStrInStr(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle, idx_t needle_size) {
	D_ASSERT(needle_size > 0);
	// start off by performing a memchr to find the first character of the
	auto location = memchr(haystack, needle[0], haystack_size);
	if (location == nullptr) {
		return DConstants::INVALID_INDEX;
	}
	idx_t base_offset = UnsafeNumericCast<idx_t>(const_uchar_ptr_cast(location) - haystack);
	haystack_size -= base_offset;
	haystack = const_uchar_ptr_cast(location);
#ifndef DUCKDB_SMALLER_BINARY
	// switch algorithm depending on needle size
	switch (needle_size) {
	case 1:
		return base_offset;
	case 2:
		return ContainsAligned<uint16_t>(haystack, haystack_size, needle, base_offset);
	case 3:
		return ContainsUnaligned<uint16_t, 3>(haystack, haystack_size, needle, base_offset);
	case 4:
		return ContainsAligned<uint32_t>(haystack, haystack_size, needle, base_offset);
	case 5:
		return ContainsUnaligned<uint32_t, 5>(haystack, haystack_size, needle, base_offset);
	case 6:
		return ContainsUnaligned<uint32_t, 6>(haystack, haystack_size, needle, base_offset);
	case 7:
		return ContainsUnaligned<uint32_t, 7>(haystack, haystack_size, needle, base_offset);
	case 8:
		return ContainsAligned<uint64_t>(haystack, haystack_size, needle, base_offset);
	default:
		return ContainsGeneric(haystack, haystack_size, needle, needle_size, base_offset);
	}
#else
	return ContainsGeneric(haystack, haystack_size, needle, needle_size, base_offset);
#endif
}

idx_t FindStrInStr(const string_t &haystack_s, const string_t &needle_s) {
	auto haystack = const_uchar_ptr_cast(haystack_s.GetData());
	auto haystack_size = haystack_s.GetSize();
	auto needle = const_uchar_ptr_cast(needle_s.GetData());
	auto needle_size = needle_s.GetSize();
	if (needle_size == 0) {
		// empty needle: always true
		return 0;
	}
	return FindStrInStr(haystack, haystack_size, needle, needle_size);
}

struct ContainsOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return FindStrInStr(left, right) != DConstants::INVALID_INDEX;
	}
};

ScalarFunction GetStringContains() {
	ScalarFunction string_fun("contains", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                          ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsOperator>);
	string_fun.collation_handling = FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS;
	return string_fun;
}

ScalarFunctionSet ContainsFun::GetFunctions() {
	auto string_fun = GetStringContains();
	auto list_fun = ListContainsFun::GetFunction();
	auto map_fun = MapContainsFun::GetFunction();
	ScalarFunctionSet set("contains");
	set.AddFunction(string_fun);
	set.AddFunction(list_fun);
	set.AddFunction(map_fun);
	return set;
}

} // namespace duckdb
