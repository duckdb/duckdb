#include "core_functions/scalar/blob_functions.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

namespace duckdb {

namespace {

void EncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// encode is essentially a nop cast from varchar to blob
	// we only need to reinterpret the data using the blob type
	result.Reinterpret(args.data[0]);
}

enum class DecodeErrorBehavior : uint8_t {
	ABORT = 1,  // raise error
	STRICT = 2, // return null
	REPLACE = 3 // replace invalid characters with '?'
};

bool TryGetDecodeErrorBehavior(const string &specifier_p, DecodeErrorBehavior &result) {
	auto specifier = StringUtil::Lower(specifier_p);
	if (specifier == "abort" || specifier == "a") {
		result = DecodeErrorBehavior::ABORT;
	} else if (specifier == "strict" || specifier == "s") {
		result = DecodeErrorBehavior::STRICT;
	} else if (specifier == "replace" || specifier == "r") {
		result = DecodeErrorBehavior::REPLACE;
	} else {
		return false;
	}
	return true;
}

DecodeErrorBehavior GetDecodeErrorBehavior(const string &specifier) {
	DecodeErrorBehavior result;
	if (!TryGetDecodeErrorBehavior(specifier, result)) {
		throw ConversionException("decode error behavior specifier \"%s\" not recognized", specifier);
	}
	return result;
}

struct UnaryBlobDecodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		if (Utf8Proc::Analyze(input_data, input_length) == UnicodeType::INVALID) {
			throw ConversionException(
			    "Failure in decode: could not convert blob to UTF8 string, the blob contained invalid UTF8 characters");
		}
		return input;
	}
};

void UnaryDecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::Execute<string_t, string_t, UnaryBlobDecodeOperator>(args.data[0], result, args.size());
	StringVector::AddHeapReference(result, args.data[0]);
}

void BinaryDecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](string_t input, string_t error_option, ValidityMask &mask, idx_t idx) {
		    auto input_data = input.GetDataWriteable();
		    auto input_length = input.GetSize();
		    auto const error_behavior = GetDecodeErrorBehavior(error_option.GetData());
		    switch (error_behavior) {
		    case DecodeErrorBehavior::ABORT:
			    if (Utf8Proc::Analyze(input_data, input_length) == UnicodeType::INVALID) {
				    throw ConversionException("Failure in decode: could not convert blob to UTF8 string, the blob "
				                              "contained invalid UTF8 characters");
			    }
			    return input;

		    case DecodeErrorBehavior::STRICT:
			    if (Utf8Proc::Analyze(input_data, input_length) == UnicodeType::INVALID) {
				    mask.SetInvalid(idx);
				    return StringVector::EmptyString(result, input_length);
			    } else {
				    return input;
			    }
		    case DecodeErrorBehavior::REPLACE:
			    if (Utf8Proc::Analyze(input_data, input_length) == UnicodeType::INVALID) {
				    Utf8Proc::MakeValid(input_data, input_length);
			    }
			    return input;
		    }
	    });
	StringVector::AddHeapReference(result, args.data[0]);
}

} // namespace

ScalarFunction EncodeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, EncodeFunction);
}

ScalarFunctionSet DecodeFun::GetFunctions() {
	ScalarFunctionSet decode("decode");

	ScalarFunction unary_function({LogicalType::BLOB}, LogicalType::VARCHAR, UnaryDecodeFunction);
	ScalarFunction binary_function({LogicalType::BLOB, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               BinaryDecodeFunction);

	unary_function.SetFallible();
	binary_function.SetFallible();

	decode.AddFunction(unary_function);
	decode.AddFunction(binary_function);

	return decode;
}

} // namespace duckdb
