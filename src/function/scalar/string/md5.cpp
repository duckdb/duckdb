#include "duckdb/function/scalar/crypto_hash.hpp"

#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

namespace {

struct MD5Number128Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &, idx_t, crypto_hash_scalar::NumberData &data) {
		data_t digest[16];
		uhugeint_t result;
		D_ASSERT(CryptoHash::GetDigestSize(CryptoHashFunction::MD5) == sizeof(digest));
		D_ASSERT(sizeof(result) == sizeof(digest));
		data.hash_state.Hash(const_data_ptr_cast(input.GetData()), input.GetSize(), digest);
		memcpy(&result, digest, sizeof(result));
		return BSwapIfBE(result);
	}
};

void MD5Function(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &input = args.data[0];
	auto &local_state = crypto_hash_scalar::GetLocalState(state);
	auto &heap = StringVector::GetStringHeap(result);
	crypto_hash_scalar::StringData data(*local_state.hash_state, heap);

	UnaryExecutor::GenericExecute<string_t, string_t, crypto_hash_scalar::StringOperator<CryptoHashFunction::MD5>>(
	    input, result, data);
}

void MD5NumberFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &input = args.data[0];
	auto &local_state = crypto_hash_scalar::GetLocalState(state);
	crypto_hash_scalar::NumberData data(*local_state.hash_state);

	UnaryExecutor::GenericExecute<string_t, uhugeint_t, MD5Number128Operator>(input, result, data);
}

} // namespace

ScalarFunctionSet MD5Fun::GetFunctions() {
	ScalarFunctionSet set("md5");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, MD5Function, nullptr, nullptr,
	                               crypto_hash_scalar::InitLocalState<CryptoHashFunction::MD5>));
	set.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, MD5Function, nullptr, nullptr,
	                               crypto_hash_scalar::InitLocalState<CryptoHashFunction::MD5>));
	return set;
}

ScalarFunctionSet MD5NumberFun::GetFunctions() {
	ScalarFunctionSet set("md5_number");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::UHUGEINT, MD5NumberFunction, nullptr, nullptr,
	                               crypto_hash_scalar::InitLocalState<CryptoHashFunction::MD5>));
	set.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::UHUGEINT, MD5NumberFunction, nullptr, nullptr,
	                               crypto_hash_scalar::InitLocalState<CryptoHashFunction::MD5>));
	return set;
}

} // namespace duckdb
