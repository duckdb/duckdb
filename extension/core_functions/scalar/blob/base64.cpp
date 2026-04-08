#include <memory>

#include "core_functions/scalar/blob_functions.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
struct ExpressionState;

namespace {
struct Base64EncodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, StringHeap &heap) {
		auto result_str = heap.EmptyString(Blob::ToBase64Size(input));
		Blob::ToBase64(input, result_str.GetDataWriteable());
		result_str.Finalize();
		return result_str;
	}
};

struct Base64DecodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, StringHeap &heap) {
		auto result_size = Blob::FromBase64Size(input);
		auto result_blob = heap.EmptyString(result_size);
		Blob::FromBase64(input, data_ptr_cast(result_blob.GetDataWriteable()), result_size);
		result_blob.Finalize();
		return result_blob;
	}
};

void Base64EncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::ExecuteString<string_t, string_t, Base64EncodeOperator>(args.data[0], result, args.size());
}

void Base64DecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::ExecuteString<string_t, string_t, Base64DecodeOperator>(args.data[0], result, args.size());
}

} // namespace

ScalarFunction ToBase64Fun::GetFunction() {
	return ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, Base64EncodeFunction);
}

ScalarFunction FromBase64Fun::GetFunction() {
	ScalarFunction function({LogicalType::VARCHAR}, LogicalType::BLOB, Base64DecodeFunction);
	function.SetFallible();
	return function;
}

} // namespace duckdb
