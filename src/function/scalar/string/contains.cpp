#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/map_functions.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ContainsAligned {
	template <class UNSIGNED>
	static bool MatchRemainder(const unsigned char *haystack, const unsigned char *needle, idx_t needle_size) {
		return true;
	}
};

struct ContainsUnaligned {
	template <class UNSIGNED>
	static bool MatchRemainder(const unsigned char *haystack, const unsigned char *needle, idx_t needle_size) {
		idx_t matches = 0;
		for (idx_t i = sizeof(UNSIGNED); i < needle_size; i++) {
			matches += haystack[i] == needle[i];
		}
		return matches == needle_size - sizeof(UNSIGNED);
	}
};

struct ContainsGeneric {
	template <class UNSIGNED>
	static bool MatchRemainder(const unsigned char *haystack, const unsigned char *needle, idx_t needle_size) {
		return memcmp(haystack + sizeof(UNSIGNED), needle + sizeof(UNSIGNED), needle_size - sizeof(UNSIGNED)) == 0;
	}
};

template <class UNSIGNED, class OP>
static idx_t Contains(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                      idx_t needle_size, idx_t base_offset) {
	if (needle_size > haystack_size) {
		// needle is bigger than haystack: haystack cannot contain needle
		return DConstants::INVALID_INDEX;
	}
	haystack_size -= needle_size - 1;
	auto needle_entry = Load<UNSIGNED>(needle);
	for (idx_t offset = 0; offset < haystack_size; offset++) {
		// start off by performing a memchr to find the first character of the
		auto location =
		    static_cast<const unsigned char *>(memchr(haystack + offset, needle[0], haystack_size - offset));
		if (!location) {
			return DConstants::INVALID_INDEX;
		}
		// for this position we first compare the haystack with the needle
		offset = UnsafeNumericCast<idx_t>(location - haystack);
		auto haystack_entry = Load<UNSIGNED>(location);
		if (needle_entry == haystack_entry) {
			if (OP::template MatchRemainder<UNSIGNED>(location, needle, needle_size)) {
				return base_offset + offset;
			}
		}
	}
	return DConstants::INVALID_INDEX;
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
	// switch algorithm depending on needle size
	switch (needle_size) {
	case 1:
		return base_offset;
	case 2:
		return Contains<uint16_t, ContainsAligned>(haystack, haystack_size, needle, 2, base_offset);
	case 3:
		return Contains<uint16_t, ContainsUnaligned>(haystack, haystack_size, needle, 3, base_offset);
	case 4:
		return Contains<uint32_t, ContainsAligned>(haystack, haystack_size, needle, 4, base_offset);
	case 5:
		return Contains<uint32_t, ContainsUnaligned>(haystack, haystack_size, needle, 5, base_offset);
	case 6:
		return Contains<uint32_t, ContainsUnaligned>(haystack, haystack_size, needle, 6, base_offset);
	case 7:
		return Contains<uint32_t, ContainsUnaligned>(haystack, haystack_size, needle, 7, base_offset);
	case 8:
		return Contains<uint64_t, ContainsAligned>(haystack, haystack_size, needle, 8, base_offset);
	default:
		return Contains<uint64_t, ContainsGeneric>(haystack, haystack_size, needle, needle_size, base_offset);
	}
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
