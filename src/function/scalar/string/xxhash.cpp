#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "xxhash.h"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct XXHash32Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		const auto& inputStr = input.GetString();
		return XXH32(inputStr.c_str(), inputStr.size(), 0);
	}
};

struct XXHash64Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		const auto& inputStr = input.GetString();
		return XXH3_64bits(inputStr.c_str(), inputStr.size());
	}
};

static void XXHash32Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, int32_t, XXHash32Operator>(input, result, args.size());
}

static void XXHash64Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, int64_t, XXHash64Operator>(input, result, args.size());
}

void XXHashFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("xxhash32",                  // name of the function
	                               {LogicalType::VARCHAR},      // argument list
	                               LogicalType::INTEGER,        // return type
	                               XXHash32Function));          // pointer to function implementation
	set.AddFunction(ScalarFunction("xxhash64",                  // name of the function
	                               {LogicalType::VARCHAR},      // argument list
	                               LogicalType::BIGINT,         // return type
	                               XXHash64Function));          // pointer to function implementation
}

} // namespace duckdb
