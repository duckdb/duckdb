#include "duckdb/function/scalar/crypto_hash.hpp"

#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

namespace {

void SHA1Function(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &input = args.data[0];
	auto &local_state = crypto_hash_scalar::GetLocalState(state);
	auto &heap = StringVector::GetStringHeap(result);
	crypto_hash_scalar::StringData data(*local_state.hash_state, heap);

	UnaryExecutor::GenericExecute<string_t, string_t, crypto_hash_scalar::StringOperator<CryptoHashFunction::SHA1>>(
	    input, result, data);
}

} // namespace

ScalarFunctionSet SHA1Fun::GetFunctions() {
	ScalarFunctionSet set("sha1");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, SHA1Function, nullptr, nullptr,
	                               crypto_hash_scalar::InitLocalState<CryptoHashFunction::SHA1>));
	set.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, SHA1Function, nullptr, nullptr,
	                               crypto_hash_scalar::InitLocalState<CryptoHashFunction::SHA1>));
	return set;
}

} // namespace duckdb
