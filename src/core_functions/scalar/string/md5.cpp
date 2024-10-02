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

static void MD5Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, MD5Operator>(input, result, args.size());
}

static void MD5NumberFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, hugeint_t, MD5Number128Operator>(input, result, args.size());
}

ScalarFunctionSet MD5Fun::GetFunctions() {
	ScalarFunctionSet set("md5");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, MD5Function));
	set.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, MD5Function));
	return set;
}

ScalarFunctionSet MD5NumberFun::GetFunctions() {
	ScalarFunctionSet set("md5_number");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::HUGEINT, MD5NumberFunction));
	set.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::HUGEINT, MD5NumberFunction));
	return set;
}

} // namespace duckdb
