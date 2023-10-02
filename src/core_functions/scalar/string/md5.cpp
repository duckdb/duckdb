#include "duckdb/core_functions/scalar/string_functions.hpp"

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

struct MD5Number128Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		data_t digest[MD5Context::MD5_HASH_LENGTH_BINARY];

		MD5Context context;
		context.Add(input);
		context.Finish(digest);
		return *reinterpret_cast<hugeint_t *>(digest);
	}
};

template <bool lower>
struct MD5Number64Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		data_t digest[MD5Context::MD5_HASH_LENGTH_BINARY];

		MD5Context context;
		context.Add(input);
		context.Finish(digest);
		return *reinterpret_cast<uint64_t *>(&digest[lower ? 8 : 0]);
	}
};

static void MD5Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, MD5Operator>(input, result, args.size());
}

static void MD5NumberFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, hugeint_t, MD5Number128Operator>(input, result, args.size());
}

static void MD5NumberUpperFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, uint64_t, MD5Number64Operator<false>>(input, result, args.size());
}

static void MD5NumberLowerFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, uint64_t, MD5Number64Operator<true>>(input, result, args.size());
}

ScalarFunction MD5Fun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, MD5Function);
}

ScalarFunction MD5NumberFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::HUGEINT, MD5NumberFunction);
}

ScalarFunction MD5NumberUpperFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, MD5NumberUpperFunction);
}

ScalarFunction MD5NumberLowerFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, MD5NumberLowerFunction);
}

} // namespace duckdb
