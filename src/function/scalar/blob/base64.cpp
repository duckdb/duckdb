#include "duckdb/function/scalar/blob_functions.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

struct Base64EncodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto result_str = StringVector::EmptyString(result, Blob::ToBase64Size(input));
		Blob::ToBase64(input, result_str.GetDataWriteable());
		result_str.Finalize();
		return result_str;
	}
};

struct Base64DecodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto result_size = Blob::FromBase64Size(input);
		auto result_blob = StringVector::EmptyString(result, result_size);
		Blob::FromBase64(input, (data_ptr_t)result_blob.GetDataWriteable(), result_size);
		result_blob.Finalize();
		return result_blob;
	}
};

static void Base64EncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::ExecuteString<string_t, string_t, Base64EncodeOperator>(args.data[0], result, args.size());
}

static void Base64DecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::ExecuteString<string_t, string_t, Base64DecodeOperator>(args.data[0], result, args.size());
}

void Base64Fun::RegisterFunction(BuiltinFunctions &set) {
	// base64 encode
	ScalarFunction to_base64({LogicalType::BLOB}, LogicalType::VARCHAR, Base64EncodeFunction);
	set.AddFunction({"base64", "to_base64"}, to_base64); // to_base64 is a mysql alias

	set.AddFunction(ScalarFunction("from_base64", {LogicalType::VARCHAR}, LogicalType::BLOB, Base64DecodeFunction));
}

} // namespace duckdb
