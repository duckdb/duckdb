#include "duckdb/core_functions/scalar/bit_functions.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BitStringFunction
//===--------------------------------------------------------------------===//
static void BitStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, idx_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t input, idx_t n) {
		    if (n < input.GetSize()) {
			    throw InvalidInputException("Length must be equal or larger than input string");
		    }
		    idx_t len;
		    Bit::TryGetBitStringSize(input, len, nullptr); // string verification
		    len = Bit::ComputeBitstringLen(n);
		    string_t target = StringVector::EmptyString(result, len);
		    Bit::BitString(input, n, target);
		    target.Finalize();
		    return target;
	    });
}

ScalarFunction BitStringFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::UBIGINT}, LogicalType::BIT, BitStringFunction);
}

//===--------------------------------------------------------------------===//
// get_bit
//===--------------------------------------------------------------------===//
struct GetBitOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB n) {
		if (n > Bit::BitLength(input) - 1) {
			throw OutOfRangeException("bit index %s out of valid range (0..%s)", NumericHelper::ToString(n),
			                          NumericHelper::ToString(Bit::BitLength(input) - 1));
		}
		return UnsafeNumericCast<TR>(Bit::GetBit(input, n));
	}
};

ScalarFunction GetBitFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::UBIGINT}, LogicalType::INTEGER,
	                      ScalarFunction::BinaryFunction<string_t, idx_t, int32_t, GetBitOperator>);
}

//===--------------------------------------------------------------------===//
// set_bit
//===--------------------------------------------------------------------===//
static void SetBitOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	TernaryExecutor::Execute<string_t, idx_t, idx_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(), [&](string_t input, idx_t n, idx_t new_value) {
		    if (new_value != 0 && new_value != 1) {
			    throw InvalidInputException("The new bit must be 1 or 0");
		    }
		    if (n > Bit::BitLength(input) - 1) {
			    throw OutOfRangeException("bit index %s out of valid range (0..%s)", NumericHelper::ToString(n),
			                              NumericHelper::ToString(Bit::BitLength(input) - 1));
		    }
		    string_t target = StringVector::EmptyString(result, input.GetSize());
		    memcpy(target.GetDataWriteable(), input.GetData(), input.GetSize());
		    Bit::SetBit(target, n, new_value);
		    return target;
	    });
}

ScalarFunction SetBitFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::UBIGINT, LogicalType::UBIGINT}, LogicalType::BIT,
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
