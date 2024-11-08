#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "utf8proc_wrapper.hpp"

#include <string.h>

namespace duckdb {

template <bool IS_UPPER>
static string_t ASCIICaseConvert(Vector &result, const char *input_data, idx_t input_length) {
	idx_t output_length = input_length;
	auto result_str = StringVector::EmptyString(result, output_length);
	auto result_data = result_str.GetDataWriteable();
	for (idx_t i = 0; i < input_length; i++) {
		result_data[i] = UnsafeNumericCast<char>(IS_UPPER ? StringUtil::ASCII_TO_UPPER_MAP[uint8_t(input_data[i])]
		                                                  : StringUtil::ASCII_TO_LOWER_MAP[uint8_t(input_data[i])]);
	}
	result_str.Finalize();
	return result_str;
}

template <bool IS_UPPER>
static idx_t GetResultLength(const char *input_data, idx_t input_length) {
	idx_t output_length = 0;
	for (idx_t i = 0; i < input_length;) {
		if (input_data[i] & 0x80) {
			// unicode
			int sz = 0;
			auto codepoint = Utf8Proc::UTF8ToCodepoint(input_data + i, sz);
			auto converted_codepoint =
			    IS_UPPER ? Utf8Proc::CodepointToUpper(codepoint) : Utf8Proc::CodepointToLower(codepoint);
			auto new_sz = Utf8Proc::CodepointLength(converted_codepoint);
			D_ASSERT(new_sz >= 0);
			output_length += UnsafeNumericCast<idx_t>(new_sz);
			i += UnsafeNumericCast<idx_t>(sz);
		} else {
			// ascii
			output_length++;
			i++;
		}
	}
	return output_length;
}

template <bool IS_UPPER>
static void CaseConvert(const char *input_data, idx_t input_length, char *result_data) {
	for (idx_t i = 0; i < input_length;) {
		if (input_data[i] & 0x80) {
			// non-ascii character
			int sz = 0, new_sz = 0;
			auto codepoint = Utf8Proc::UTF8ToCodepoint(input_data + i, sz);
			auto converted_codepoint =
			    IS_UPPER ? Utf8Proc::CodepointToUpper(codepoint) : Utf8Proc::CodepointToLower(codepoint);
			auto success = Utf8Proc::CodepointToUtf8(converted_codepoint, new_sz, result_data);
			D_ASSERT(success);
			(void)success;
			result_data += new_sz;
			i += UnsafeNumericCast<idx_t>(sz);
		} else {
			// ascii
			*result_data = UnsafeNumericCast<char>(IS_UPPER ? StringUtil::ASCII_TO_UPPER_MAP[uint8_t(input_data[i])]
			                                                : StringUtil::ASCII_TO_LOWER_MAP[uint8_t(input_data[i])]);
			result_data++;
			i++;
		}
	}
}

idx_t LowerLength(const char *input_data, idx_t input_length) {
	return GetResultLength<false>(input_data, input_length);
}

void LowerCase(const char *input_data, idx_t input_length, char *result_data) {
	CaseConvert<false>(input_data, input_length, result_data);
}

template <bool IS_UPPER>
static string_t UnicodeCaseConvert(Vector &result, const char *input_data, idx_t input_length) {
	// first figure out the output length
	idx_t output_length = GetResultLength<IS_UPPER>(input_data, input_length);
	auto result_str = StringVector::EmptyString(result, output_length);
	auto result_data = result_str.GetDataWriteable();

	CaseConvert<IS_UPPER>(input_data, input_length, result_data);
	result_str.Finalize();
	return result_str;
}

template <bool IS_UPPER>
struct CaseConvertOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		return UnicodeCaseConvert<IS_UPPER>(result, input_data, input_length);
	}
};

template <bool IS_UPPER>
static void CaseConvertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, CaseConvertOperator<IS_UPPER>>(args.data[0], result, args.size());
}

template <bool IS_UPPER>
struct CaseConvertOperatorASCII {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		return ASCIICaseConvert<IS_UPPER>(result, input_data, input_length);
	}
};

template <bool IS_UPPER>
static void CaseConvertFunctionASCII(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, CaseConvertOperatorASCII<IS_UPPER>>(args.data[0], result,
	                                                                                     args.size());
}

template <bool IS_UPPER>
static unique_ptr<BaseStatistics> CaseConvertPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	if (!StringStats::CanContainUnicode(child_stats[0])) {
		expr.function.function = CaseConvertFunctionASCII<IS_UPPER>;
	}
	return nullptr;
}

ScalarFunction LowerFun::GetFunction() {
	return ScalarFunction("lower", {LogicalType::VARCHAR}, LogicalType::VARCHAR, CaseConvertFunction<false>, nullptr,
	                      nullptr, CaseConvertPropagateStats<false>);
}

ScalarFunction UpperFun::GetFunction() {
	return ScalarFunction("upper", {LogicalType::VARCHAR}, LogicalType::VARCHAR, CaseConvertFunction<true>, nullptr,
	                      nullptr, CaseConvertPropagateStats<true>);
}

} // namespace duckdb
