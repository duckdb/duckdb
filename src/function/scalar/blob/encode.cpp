#include "duckdb/function/scalar/blob_functions.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

static void EncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// encode is essentially a nop cast from varchar to blob
	// we just reference the input vector and set the type to blob
	result.Reference(args.data[0], false);
}

static void DecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		auto input_data = input.GetDataUnsafe();
		auto input_length = input.GetSize();
		if (Utf8Proc::Analyze(input_data, input_length) == UnicodeType::INVALID) {
			throw ConversionException(
			    "Failure in decode: could not convert blob to UTF8 string, the blob contained invalid UTF8 characters");
		}
		return input;
	});
	StringVector::AddHeapReference(result, args.data[0]);
}

void EncodeFun::RegisterFunction(BuiltinFunctions &set) {
	// encode goes from varchar -> blob, this never fails
	set.AddFunction(ScalarFunction("encode", {LogicalType::VARCHAR}, LogicalType::BLOB, EncodeFunction));
	// decode goes from blob -> varchar, this fails if the varchar is not valid utf8
	set.AddFunction(ScalarFunction("decode", {LogicalType::BLOB}, LogicalType::VARCHAR, DecodeFunction));
}

} // namespace duckdb
