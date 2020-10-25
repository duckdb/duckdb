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
	static inline SRCTYPE Operation(SRCTYPE left, SRCTYPE right) {
		UTYPE result = MultiplyOperator::Operation<UTYPE, UTYPE, UTYPE>(UTYPE(left), UTYPE(right));
		if (result < NumericLimits<SRCTYPE>::Minimum() || result > NumericLimits<SRCTYPE>::Maximum()) {
			throw OutOfRangeException("Overflow in multiplication of %s (%d * %d = %d)!", TypeIdToString(GetTypeId<SRCTYPE>()), left, right, result);
		}
		return result;
	}
};

template <> int8_t MultiplyOperatorOverflowCheck::Operation(int8_t left, int8_t right) {
	return OverflowCheckedMultiply::Operation<int8_t, int16_t>(left, right);
}

template <> int16_t MultiplyOperatorOverflowCheck::Operation(int16_t left, int16_t right) {
	return OverflowCheckedMultiply::Operation<int16_t, int32_t>(left, right);
}

template <> int32_t MultiplyOperatorOverflowCheck::Operation(int32_t left, int32_t right) {
	return OverflowCheckedMultiply::Operation<int32_t, int64_t>(left, right);
}

static bool int64_try_multiply(int64_t left, int64_t right, int64_t *result) {
// #if defined(__GNUC__) || defined(__clang__)
// 	int64_t result;
// 	if (__builtin_mul_overflow(left, right, &result)) {
// 		return false;
// 	}
// 	// FIXME: this check can be removed if we get rid of NullValue<T>
// 	if (result == std::numeric_limits<int64_t>::min()) {
// 		return false;
// 	}
// 	return result;
// #else

	bool lhs_negative = left < 0;
	bool rhs_negative = right < 0;
	if (lhs_negative) {
		left = -left;
	}
	if (rhs_negative) {
		right = -right;
	}
	// split values into 2 32-bit parts
	uint64_t top[2] = {uint64_t(left) >> 32, uint64_t(left) & 0xffffffff };
	uint64_t bottom[2] = {uint64_t(right) >> 32, uint64_t(right) & 0xffffffff };
	uint64_t products[2][2];

	// multiply each component of the values
	for (auto x = 0; x < 2; x++) {
		for (auto y = 0; y < 2; y++) {
			products[x][y] = top[x] * bottom[y];
		}
	}

	// products[0][0] is the two high parts multiplied by each other
	// if that product is non-zero we have an overflow
	if (products[0][0]) {
		return false;
	}
	// products[0][1] and products[1][0] are the high part of one multiplied by the low part of another
	// note that only ONE of these can be non-zero
	// for both to be non-zero, both sides would need to have high bits set
	// which means products[1][1] would be non-zero
	// if the high bits are set in either of these, we have an overflow
	if ((products[0][1] & 0xffffff80000000) ||
	    (products[1][0] & 0xffffff80000000)) {
		return false;
	}
	// the total high bits we get are adding these two together
	// note again that one of these is necessarily zero, so this is essentially an assignment of the non-zero one
	uint32_t high_bits = products[1][0] + products[0][1];
	assert(!(high_bits & 0xffffff80000000));

	// now we create the final result
	*result = (uint64_t(high_bits) << 32) + products[1][1];
	if (lhs_negative ^ rhs_negative) {
		*result = -*result;
	}
	return true;
// #endif
}

template <> int64_t MultiplyOperatorOverflowCheck::Operation(int64_t left, int64_t right) {
	int64_t result;
	if (!int64_try_multiply(left, right, &result)) {
		throw OutOfRangeException("Overflow in multiplication of BIGINT (%d * %d);", left, right);
	}
	return result;
}

}
