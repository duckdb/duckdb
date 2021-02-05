#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "utf8proc.hpp"

namespace duckdb {

// length returns the size in characters
struct StringLengthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return LengthFun::Length<TA, TR>(input);
	}
};

// strlen returns the size in bytes
struct StrLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return input.GetSize();
	}
};

// bitlen returns the size in bits
struct BitLenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 8 * input.GetSize();
	}
};

static unique_ptr<BaseStatistics> LengthPropagateStats(ClientContext &context, BoundFunctionExpression &expr,
                                                         FunctionData *bind_data,
                                                         vector<unique_ptr<BaseStatistics>> &child_stats) {
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	if (!child_stats[0]) {
		return nullptr;
	}
	auto &sstats = (StringStatistics &)*child_stats[0];
	if (!sstats.has_unicode) {
		expr.function.function = ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator, true>;
	}
	return nullptr;
}

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"length", "len"},
	                ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StringLengthOperator, true>, false,
	                               nullptr, nullptr, LengthPropagateStats));
	set.AddFunction(ScalarFunction("strlen", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator, true>));
	set.AddFunction(ScalarFunction("bit_length", {LogicalType::VARCHAR}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, BitLenOperator, true>));
	// length for BLOB type
	set.AddFunction(ScalarFunction("octet_length", {LogicalType::BLOB}, LogicalType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator, true>));
}

struct UnicodeOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		const auto str = reinterpret_cast<const utf8proc_uint8_t *>(input.GetDataUnsafe());
		const auto len = input.GetSize();
		utf8proc_int32_t codepoint;
		(void)utf8proc_iterate(str, len, &codepoint);
		return codepoint;
	}
};

void UnicodeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction unicode("unicode", {LogicalType::VARCHAR}, LogicalType::INTEGER,
	                       ScalarFunction::UnaryFunction<string_t, int32_t, UnicodeOperator, true>);
	set.AddFunction(unicode);
	unicode.name = "ord";
	set.AddFunction(unicode);
}

} // namespace duckdb
