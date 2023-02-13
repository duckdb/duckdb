#include "duckdb/common/types/bit.hpp"
#include "duckdb/function/scalar/bit_functions.hpp"

namespace duckdb {

static void BitStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, string_t>(
        args.data[0], args.data[1], result, args.size(), [&](string_t input, int32_t n) {
            if (n < 0) {
                throw InvalidInputException("The bitstring length cannot be negative");
            }
            if ((idx_t)n < input.GetSize()) {
                throw InvalidInputException("Length must be equal or larger than input string");
            }
            idx_t len;
            Bit::TryGetBitStringSize(input, len, nullptr);  // string verification

            len = n % 8 ? (n / 8) + 1 : n / 8;
            len++;
            string_t target = StringVector::EmptyString(result, len);

            Bit::BitString(input, n, target);
            return target;
        });
}

void BitStringFun::RegisterFunction(BuiltinFunctions &set) {
	// bitstring creates a new bitstring from varchar with a fixed-length
	set.AddFunction(ScalarFunction("bitstring", {LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::BIT, BitStringFunction));
}

} // namespace duckdb