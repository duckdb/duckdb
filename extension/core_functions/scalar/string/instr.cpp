#include "core_functions/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "utf8proc.hpp"

namespace duckdb {

struct InstrOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA haystack, TB needle) {
		int64_t string_position = 0;

		auto location = FindStrInStr(haystack, needle);
		if (location != DConstants::INVALID_INDEX) {
			auto len = (utf8proc_ssize_t)location;
			auto str = reinterpret_cast<const utf8proc_uint8_t *>(haystack.GetData());
			D_ASSERT(len <= (utf8proc_ssize_t)haystack.GetSize());
			for (++string_position; len > 0; ++string_position) {
				utf8proc_int32_t codepoint;
				auto bytes = utf8proc_iterate(str, len, &codepoint);
				str += bytes;
				len -= bytes;
			}
		}
		return string_position;
	}
};

struct InstrAsciiOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA haystack, TB needle) {
		auto location = FindStrInStr(haystack, needle);
		return UnsafeNumericCast<TR>(location == DConstants::INVALID_INDEX ? 0U : location + 1U);
	}
};

static unique_ptr<BaseStatistics> InStrPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 2);
	// can only propagate stats if the children have stats
	// for strpos, we only care if the FIRST string has unicode or not
	if (!StringStats::CanContainUnicode(child_stats[0])) {
		expr.function.function = ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrAsciiOperator>;
	}
	return nullptr;
}

ScalarFunction InstrFun::GetFunction() {
	auto function = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                               ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrOperator>, nullptr,
	                               nullptr, InStrPropagateStats);
	function.SetCollationHandling(FunctionCollationHandling::PUSH_COMBINABLE_COLLATIONS);
	return function;
}

} // namespace duckdb
