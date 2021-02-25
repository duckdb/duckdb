#include "duckdb/function/scalar/blob_functions.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

static void Base64EncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		auto result_str = StringVector::EmptyString(result, Blob::ToBase64Size(input));
		Blob::ToBase64(input, result_str.GetDataWriteable());
		result_str.Finalize();
		return result_str;
	});
}

static void Base64DecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		auto result_size = Blob::FromBase64Size(input);
		auto result_blob = StringVector::EmptyString(result, result_size);
		Blob::FromBase64(input, (data_ptr_t)result_blob.GetDataWriteable(), result_size);
		result_blob.Finalize();
		return result_blob;
	});
}

void Base64Fun::RegisterFunction(BuiltinFunctions &set) {
	// base64 encode
	ScalarFunction to_base64("base64", {LogicalType::BLOB}, LogicalType::VARCHAR, Base64EncodeFunction);
	set.AddFunction(to_base64);
	to_base64.name = "to_base64"; // mysql alias
	set.AddFunction(to_base64);

	set.AddFunction(ScalarFunction("from_base64", {LogicalType::VARCHAR}, LogicalType::BLOB, Base64DecodeFunction));
}

} // namespace duckdb
