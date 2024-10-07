#include "duckdb/core_functions/scalar/bit_functions.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BitStringFunction
//===--------------------------------------------------------------------===//
template <bool FROM_STRING>
static void BitStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t input, int32_t n) {
		    if (n < 0) {
			    throw InvalidInputException("The bitstring length cannot be negative");
		    }
		    if (!FROM_STRING) {
			    // FIXME: this could be optimized to not go through the VARCHAR -> BIT path
			    auto converted = Bit::ToString(input);
			    input = string_t(converted.c_str(), UnsafeNumericCast<uint32_t>(converted.size()));
		    }
		    if (idx_t(n) < input.GetSize()) {
			    throw InvalidInputException("Length must be equal or larger than input string");
		    }
		    idx_t len;
		    Bit::TryGetBitStringSize(input, len, nullptr); // string verification

		    len = Bit::ComputeBitstringLen(UnsafeNumericCast<idx_t>(n));
		    string_t target = StringVector::EmptyString(result, len);
		    Bit::BitString(input, UnsafeNumericCast<idx_t>(n), target);
		    target.Finalize();
		    return target;
	    });
}

ScalarFunctionSet BitStringFun::GetFunctions() {
	ScalarFunctionSet bitstring;
	bitstring.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::BIT, BitStringFunction<true>));
	bitstring.AddFunction(
	    ScalarFunction({LogicalType::BIT, LogicalType::INTEGER}, LogicalType::BIT, BitStringFunction<false>));
	return bitstring;
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
		return UnsafeNumericCast<TR>(Bit::GetBit(input, UnsafeNumericCast<idx_t>(n)));
	}
};

ScalarFunction GetBitFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::INTEGER}, LogicalType::INTEGER,
	                      ScalarFunction::BinaryFunction<string_t, int32_t, int32_t, GetBitOperator>);
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
		    memcpy(target.GetDataWriteable(), input.GetData(), input.GetSize());
		    Bit::SetBit(target, UnsafeNumericCast<idx_t>(n), UnsafeNumericCast<idx_t>(new_value));
		    return target;
	    });
}

ScalarFunction SetBitFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::BIT,
	                      SetBitOperation);
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
		return UnsafeNumericCast<TR>(Bit::BitPosition(substring, input));
	}
};

ScalarFunction BitPositionFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::BIT}, LogicalType::INTEGER,
	                      ScalarFunction::BinaryFunction<string_t, string_t, int32_t, BitPositionOperator>);
}

} // namespace duckdb
