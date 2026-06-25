//===----------------------------------------------------------------------===//
//                         DuckDB
//
// crypto_hash.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace crypto_hash_scalar {

struct LocalState : public FunctionLocalState {
	explicit LocalState(shared_ptr<EncryptionUtil> encryption_util_p) : encryption_util(std::move(encryption_util_p)) {
	}

	shared_ptr<EncryptionUtil> encryption_util;
};

template <CryptoHashFunction FUNCTION>
unique_ptr<FunctionLocalState> InitLocalState(ExpressionState &state, const BoundFunctionExpression &, FunctionData *) {
	auto &context = state.GetContext();
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.force_mbedtls && config.encryption_util && config.encryption_util->SupportsHash(FUNCTION)) {
		return make_uniq<LocalState>(config.encryption_util);
	}
	return make_uniq<LocalState>(context.db->GetMbedTLSUtil(config.options.force_mbedtls));
}

struct StringData {
	StringData(EncryptionUtil &encryption_util, StringHeap &heap) : encryption_util(encryption_util), heap(heap) {
	}

	EncryptionUtil &encryption_util;
	StringHeap &heap;
};

struct NumberData {
	explicit NumberData(EncryptionUtil &encryption_util) : encryption_util(encryption_util) {
	}

	EncryptionUtil &encryption_util;
};

template <CryptoHashFunction FUNCTION>
struct StringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &, idx_t, StringData &data) {
		auto hash = data.heap.EmptyString(CryptoHash::GetHexDigestSize(FUNCTION));
		data.encryption_util.HashHex(FUNCTION, const_data_ptr_cast(input.GetData()), input.GetSize(),
		                             hash.GetDataWriteable());
		hash.Finalize();
		return hash;
	}
};

inline LocalState &GetLocalState(ExpressionState &state) {
	return ExecuteFunctionState::GetFunctionState(state)->Cast<LocalState>();
}

} // namespace crypto_hash_scalar
} // namespace duckdb
