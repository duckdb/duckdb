#include "duckdb/common/operator/multiply.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/windows_undefs.hpp"

#include <limits>
#include <algorithm>

namespace duckdb {

//===--------------------------------------------------------------------===//
// * [multiply]
//===--------------------------------------------------------------------===//
template <>
float MultiplyOperator::Operation(float left, float right) {
	auto result = left * right;
	return result;
}

template <>
double MultiplyOperator::Operation(double left, double right) {
	auto result = left * right;
	return result;
}

template <>
interval_t MultiplyOperator::Operation(interval_t left, int64_t right) {
	left.months = MultiplyOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.months, right);
	left.days = MultiplyOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(left.days, right);
	left.micros = MultiplyOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(left.micros, right);
	return left;
}

template <>
interval_t MultiplyOperator::Operation(int64_t left, interval_t right) {
	return MultiplyOperator::Operation<interval_t, int64_t, interval_t>(right, left);
}

//===--------------------------------------------------------------------===//
// * [multiply] with overflow check
//===--------------------------------------------------------------------===//
struct OverflowCheckedMultiply {
	template <class SRCTYPE, class UTYPE>
	static inline bool Operation(SRCTYPE left, SRCTYPE right, SRCTYPE &result) {
		UTYPE uresult = MultiplyOperator::Operation<UTYPE, UTYPE, UTYPE>(UTYPE(left), UTYPE(right));
		if (uresult < NumericLimits<SRCTYPE>::Minimum() || uresult > NumericLimits<SRCTYPE>::Maximum()) {
			return false;
		}
		result = SRCTYPE(uresult);
		return true;
	}
};

template <>
bool TryMultiplyOperator::Operation(uint8_t left, uint8_t right, uint8_t &result) {
	return OverflowCheckedMultiply::Operation<uint8_t, uint16_t>(left, right, result);
}
template <>
bool TryMultiplyOperator::Operation(uint16_t left, uint16_t right, uint16_t &result) {
	return OverflowCheckedMultiply::Operation<uint16_t, uint32_t>(left, right, result);
}
template <>
bool TryMultiplyOperator::Operation(uint32_t left, uint32_t right, uint32_t &result) {
	return OverflowCheckedMultiply::Operation<uint32_t, uint64_t>(left, right, result);
}
template <>
bool TryMultiplyOperator::Operation(uint64_t left, uint64_t right, uint64_t &result) {
	if (left > right) {
		std::swap(left, right);
	}
	if (left > NumericLimits<uint32_t>::Maximum()) {
		return false;
	}
	uint32_t c = right >> 32;
	uint32_t d = NumericLimits<uint32_t>::Maximum() & right;
	uint64_t r = left * c;
	uint64_t s = left * d;
	if (r > NumericLimits<uint32_t>::Maximum()) {
		return false;
	}
	r <<= 32;
	if (NumericLimits<uint64_t>::Maximum() - s < r) {
		return false;
	}
	return OverflowCheckedMultiply::Operation<uint64_t, uint64_t>(left, right, result);
}

template <>
bool TryMultiplyOperator::Operation(int8_t left, int8_t right, int8_t &result) {
	return OverflowCheckedMultiply::Operation<int8_t, int16_t>(left, right, result);
}

template <>
bool TryMultiplyOperator::Operation(int16_t left, int16_t right, int16_t &result) {
	return OverflowCheckedMultiply::Operation<int16_t, int32_t>(left, right, result);
}

template <>
bool TryMultiplyOperator::Operation(int32_t left, int32_t right, int32_t &result) {
	return OverflowCheckedMultiply::Operation<int32_t, int64_t>(left, right, result);
}

template <>
bool TryMultiplyOperator::Operation(int64_t left, int64_t right, int64_t &result) {
#if (__GNUC__ >= 5) || defined(__clang__)
	if (__builtin_mul_overflow(left, right, &result)) {
		return false;
	}
#else
	if (left == std::numeric_limits<int64_t>::min()) {
		if (right == 0) {
			result = 0;
			return true;
		}
		if (right == 1) {
			result = left;
			return true;
		}
		return false;
	}
	if (right == std::numeric_limits<int64_t>::min()) {
		if (left == 0) {
			result = 0;
			return true;
		}
		if (left == 1) {
			result = right;
			return true;
		}
		return false;
	}
	uint64_t left_non_negative = uint64_t(std::abs(left));
	uint64_t right_non_negative = uint64_t(std::abs(right));
	// split values into 2 32-bit parts
	uint64_t left_high_bits = left_non_negative >> 32;
	uint64_t left_low_bits = left_non_negative & 0xffffffff;
	uint64_t right_high_bits = right_non_negative >> 32;
	uint64_t right_low_bits = right_non_negative & 0xffffffff;

	// check the high bits of both
	// the high bits define the overflow
	if (left_high_bits == 0) {
		if (right_high_bits != 0) {
			// only the right has high bits set
			// multiply the high bits of right with the low bits of left
			// multiply the low bits, and carry any overflow to the high bits
			// then check for any overflow
			auto low_low = left_low_bits * right_low_bits;
			auto low_high = left_low_bits * right_high_bits;
			auto high_bits = low_high + (low_low >> 32);
			if (high_bits & 0xffffff80000000) {
				// there is! abort
				return false;
			}
		}
	} else if (right_high_bits == 0) {
		// only the left has high bits set
		// multiply the high bits of left with the low bits of right
		// multiply the low bits, and carry any overflow to the high bits
		// then check for any overflow
		auto low_low = left_low_bits * right_low_bits;
		auto high_low = left_high_bits * right_low_bits;
		auto high_bits = high_low + (low_low >> 32);
		if (high_bits & 0xffffff80000000) {
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
	return true;
}

template <>
bool TryMultiplyOperator::Operation(hugeint_t left, hugeint_t right, hugeint_t &result) {
	return Hugeint::TryMultiply(left, right, result);
}

//===--------------------------------------------------------------------===//
// multiply  decimal with overflow check
//===--------------------------------------------------------------------===//
template <class T, T min, T max>
bool TryDecimalMultiplyTemplated(T left, T right, T &result) {
	if (!TryMultiplyOperator::Operation(left, right, result) || result < min || result > max) {
		return false;
	}
	return true;
}

template <>
bool TryDecimalMultiply::Operation(int16_t left, int16_t right, int16_t &result) {
	return TryDecimalMultiplyTemplated<int16_t, -9999, 9999>(left, right, result);
}

template <>
bool TryDecimalMultiply::Operation(int32_t left, int32_t right, int32_t &result) {
	return TryDecimalMultiplyTemplated<int32_t, -999999999, 999999999>(left, right, result);
}

template <>
bool TryDecimalMultiply::Operation(int64_t left, int64_t right, int64_t &result) {
	return TryDecimalMultiplyTemplated<int64_t, -999999999999999999, 999999999999999999>(left, right, result);
}

template <>
bool TryDecimalMultiply::Operation(hugeint_t left, hugeint_t right, hugeint_t &result) {
	result = left * right;
	if (result <= -Hugeint::POWERS_OF_TEN[38] || result >= Hugeint::POWERS_OF_TEN[38]) {
		return false;
	}
	return true;
}

template <>
hugeint_t DecimalMultiplyOverflowCheck::Operation(hugeint_t left, hugeint_t right) {
	hugeint_t result;
	if (!TryDecimalMultiply::Operation(left, right, result)) {
		throw OutOfRangeException("Overflow in multiplication of DECIMAL(38) (%s * %s). You might want to add an "
		                          "explicit cast to a decimal with a smaller scale.",
		                          left.ToString(), right.ToString());
	}
	return result;
}

} // namespace duckdb
