#include "duckdb/common/operator/multiply.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/types/hugeint.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// * [multiply]
//===--------------------------------------------------------------------===//
template <> float MultiplyOperator::Operation(float left, float right) {
	auto result = left * right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in multiplication of float!");
	}
	return result;
}

template <> double MultiplyOperator::Operation(double left, double right) {
	auto result = left * right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in multiplication of double!");
	}
	return result;
}

template <> interval_t MultiplyOperator::Operation(interval_t left, int64_t right) {
	left.months = MultiplyOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.months, right);
	left.days = MultiplyOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.days, right);
	left.msecs = MultiplyOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(left.msecs, right);
	return left;
}

template <> interval_t MultiplyOperator::Operation(int64_t left, interval_t right) {
	return MultiplyOperator::Operation<interval_t, int64_t, interval_t>(right, left);
}

//===--------------------------------------------------------------------===//
// * [multiply] with overflow check
//===--------------------------------------------------------------------===//
struct OverflowCheckedMultiply {
	template<class SRCTYPE, class UTYPE>
	static inline bool Operation(SRCTYPE left, SRCTYPE right, SRCTYPE &result) {
		UTYPE uresult = MultiplyOperator::Operation<UTYPE, UTYPE, UTYPE>(UTYPE(left), UTYPE(right));
		if (uresult < NumericLimits<SRCTYPE>::Minimum() || uresult > NumericLimits<SRCTYPE>::Maximum()) {
			return false;
		}
		result = SRCTYPE(uresult);
		return true;
	}
};

template <> bool TryMultiplyOperator::Operation(int8_t left, int8_t right, int8_t &result) {
	return OverflowCheckedMultiply::Operation<int8_t, int16_t>(left, right, result);
}

template <> bool TryMultiplyOperator::Operation(int16_t left, int16_t right, int16_t &result) {
	return OverflowCheckedMultiply::Operation<int16_t, int32_t>(left, right, result);
}

template <> bool TryMultiplyOperator::Operation(int32_t left, int32_t right, int32_t &result) {
	return OverflowCheckedMultiply::Operation<int32_t, int64_t>(left, right, result);
}

template <> bool TryMultiplyOperator::Operation(int64_t left, int64_t right, int64_t &result) {
#if defined(__GNUC__) || defined(__clang__)
	if (__builtin_mul_overflow(left, right, &result)) {
		return false;
	}
#else
	uint64_t left_non_negative = uint64_t(abs(left));
	uint64_t right_non_negative = uint64_t(abs(right));
	// split values into 2 32-bit parts
	uint64_t top[2] = {left_non_negative >> 32, left_non_negative & 0xffffffff };
	uint64_t bottom[2] = {right_non_negative >> 32, right_non_negative & 0xffffffff };

	// check the high bits of both
	// the high bits define the overflow
	if (top[0] == 0) {
		if (bottom[0] != 0) {
			// only the right has high bits set
			// multiply the high bits of right with the low bits of left and check if there is an overflow
			auto low_high = top[1] * bottom[0];
			if (low_high & 0xffffff80000000) {
				// there is! abort
				return false;
			}
		}
	} else if (bottom[0] == 0) {
		// only the left has high bits set
		// multiply the high bits of left with the low bits of right and check if there is an overflow
		auto high_low = top[0] * bottom[1];
		if (high_low & 0xffffff80000000) {
			// there is! abort
			return false;
		}
	} else {
		// both left and right have high bits set: guaranteed overflow
		// abort!
		return false;
	}
	// now we know that there is no overflow, we can just perform the multiplication
	result = left * right;
#endif
	// FIXME: this check can be removed if we get rid of NullValue<T>
	if (result == std::numeric_limits<int64_t>::min()) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// multiply  decimal with overflow check
//===--------------------------------------------------------------------===//
template <> int64_t DecimalMultiplyOperatorOverflowCheck::Operation(int64_t left, int64_t right) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(left, right, result) || result <= -1000000000000000000 || result >= 1000000000000000000) {
		throw OutOfRangeException("Overflow in multiplication of DECIMAL(18) (%d * %d). You might want to add an explicit cast to a bigger decimal.", left, right);
	}
	return result;
}

template <> hugeint_t DecimalMultiplyOperatorOverflowCheck::Operation(hugeint_t left, hugeint_t right) {
	hugeint_t result = left * right;
	if (result <= -Hugeint::PowersOfTen[38] || result >= Hugeint::PowersOfTen[38]) {
		throw OutOfRangeException("Overflow in subtraction of DECIMAL(38) (%s - %s).", left.ToString(), right.ToString());
	}
	return result;
}


}
