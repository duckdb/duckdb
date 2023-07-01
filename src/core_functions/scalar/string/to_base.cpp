#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

static void ToBaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &radix = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<int64_t, int32_t, string_t>(input, radix, result, count, [&](int64_t input, int32_t radix) {
		if (radix < 2 || radix > 36) {
			throw InvalidInputException("radix must be between 2 and 36");
		}

		char buf[64];
		char *end = buf + sizeof(buf);
		char *ptr = end;

		do {
			*--ptr = alphabet[input % radix];
			input /= radix;
		} while (input > 0);
		return StringVector::AddString(result, ptr, end - ptr);
	});
}

static void ToBaseFunctionWithPadding(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &radix = args.data[1];
	auto &min_length = args.data[2];
	auto count = args.size();

	TernaryExecutor::Execute<int64_t, int32_t, int32_t, string_t>(
	    input, radix, min_length, result, count, [&](int64_t input, int32_t radix, int32_t min_length) {
		    if (radix < 2 || radix > 36) {
			    throw InvalidInputException("radix must be between 2 and 36");
		    }
		    if (min_length > 64 || min_length < 0) {
			    throw InvalidInputException("min_length must be between 0 and 64");
		    }

		    char buf[64];
		    char *end = buf + sizeof(buf);
		    char *ptr = end;
		    do {
			    *--ptr = alphabet[input % radix];
			    input /= radix;
		    } while (input > 0);

		    auto length = end - ptr;
		    while (length < min_length) {
			    *--ptr = '0';
			    length++;
		    }

		    return StringVector::AddString(result, ptr, end - ptr);
	    });
}

ScalarFunctionSet ToBaseFun::GetFunctions() {
	ScalarFunctionSet set("to_base");

	set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER}, LogicalType::VARCHAR, ToBaseFunction));
	set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::INTEGER},
	                               LogicalType::VARCHAR, ToBaseFunctionWithPadding));

	return set;
}

} // namespace duckdb
