#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "mbedtls_wrapper.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/crypto/sha512.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct SHA512Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto hash = StringVector::EmptyString(result, SHA512Context::SHA512_HASH_LENGTH_BINARY);
		SHA512Context context;
		context.Add(input);
		context.Finish(hash.GetDataWriteable());
		hash.Finalize();
		return hash;
	}
};

static void SHA512Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, SHA512Operator>(input, result, args.size());
}

ScalarFunction SHA512Fun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, SHA512Function);
}

} // namespace duckdb