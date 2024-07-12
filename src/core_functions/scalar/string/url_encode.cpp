#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct URLEncodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		string encoded_string = StringUtil::URLEncode(input.GetString());
		return StringVector::AddString(result, encoded_string);
	}
};

static void URLEncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, URLEncodeOperator>(args.data[0], result, args.size());
}

ScalarFunction UrlEncodeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, URLEncodeFunction);
}

struct URLDecodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		string encoded_string = StringUtil::URLDecode(input.GetString());
		return StringVector::AddString(result, encoded_string);
	}
};

static void URLDecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, URLDecodeOperator>(args.data[0], result, args.size());
}

ScalarFunction UrlDecodeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, URLDecodeFunction);
}

} // namespace duckdb
