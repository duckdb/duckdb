#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct MD5Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto hash = StringVector::EmptyString(result, MD5Context::MD5_HASH_LENGTH_TEXT);
		MD5Context context;
		context.Add(input);
		context.FinishHex(hash.GetDataWriteable());
		hash.Finalize();
		return hash;
	}
};

static void MD5Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, MD5Operator>(input, result, args.size());
}

void MD5Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("md5",                  // name of the function
	                               {LogicalType::VARCHAR}, // argument list
	                               LogicalType::VARCHAR,   // return type
	                               MD5Function));          // pointer to function implementation
}

} // namespace duckdb
