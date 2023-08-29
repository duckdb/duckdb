#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/crypto/sha256.hpp"

namespace duckdb {

struct SHA256Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto hash = StringVector::EmptyString(result, SHA256Context::SHA256_HASH_LENGTH_TEXT);
		SHA256Context context;
		context.Add(input);
		context.Finish(hash.GetDataWriteable());
		hash.Finalize();
		return hash;
	}
};

static void SHA256Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, SHA256Operator>(input, result, args.size());
}


ScalarFunction SHA256Fun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, SHA256Function);
}
} // namespace duckdb
