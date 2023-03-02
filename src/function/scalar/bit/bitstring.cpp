#include "duckdb/common/types/bit.hpp"
#include "duckdb/function/scalar/bit_functions.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BitStringFunction
//===--------------------------------------------------------------------===//

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
		    Bit::TryGetBitStringSize(input, len, nullptr); // string verification

		    len = Bit::ComputeBitstringLen(n);
		    string_t target = StringVector::EmptyString(result, len);

		    Bit::BitString(input, n, target);
		    return target;
	    });
}

void BitStringFun::RegisterFunction(BuiltinFunctions &set) {
	// bitstring creates a new bitstring from varchar with a fixed-length
	set.AddFunction(
	    ScalarFunction("bitstring", {LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::BIT, BitStringFunction));
}

//===--------------------------------------------------------------------===//
// get_bit
//===--------------------------------------------------------------------===//
struct GetBitOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB n) {
		if (n < 0 || (idx_t)n > Bit::BitLength(input) - 1) {
			throw OutOfRangeException("bit index %s out of valid range (0..%s)", NumericHelper::ToString(n),
			                          NumericHelper::ToString(Bit::BitLength(input) - 1));
		}
		return Bit::GetBit(input, n);
	}
};

void GetBitFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("get_bit", {LogicalType::BIT, LogicalType::INTEGER}, LogicalType::INTEGER,
	                               ScalarFunction::BinaryFunction<string_t, int32_t, int32_t, GetBitOperator>));
}

//===--------------------------------------------------------------------===//
// set_bit
//===--------------------------------------------------------------------===//
static void SetBitOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	TernaryExecutor::Execute<string_t, int32_t, int32_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](string_t input, int32_t n, int32_t new_value) {
		    if (new_value != 0 && new_value != 1) {
			    throw InvalidInputException("The new bit must be 1 or 0");
		    }
		    if (n < 0 || (idx_t)n > Bit::BitLength(input) - 1) {
			    throw OutOfRangeException("bit index %s out of valid range (0..%s)", NumericHelper::ToString(n),
			                              NumericHelper::ToString(Bit::BitLength(input) - 1));
		    }
		    string_t target = StringVector::EmptyString(result, input.GetSize());
		    Bit::SetBit(input, n, new_value, target);
		    return target;
	    });
}

void SetBitFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("set_bit", {LogicalType::BIT, LogicalType::INTEGER, LogicalType::INTEGER},
	                               LogicalType::BIT, SetBitOperation));
}

//===--------------------------------------------------------------------===//
// bit_position
//===--------------------------------------------------------------------===//
struct BitPositionOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA substring, TB input) {
		if (substring.GetSize() > input.GetSize()) {
			return 0;
		}
		return Bit::BitPosition(substring, input);
	}
};

void BitPositionFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("bit_position", {LogicalType::BIT, LogicalType::BIT}, LogicalType::INTEGER,
	                               ScalarFunction::BinaryFunction<string_t, string_t, int32_t, BitPositionOperator>));
}

} // namespace duckdb
