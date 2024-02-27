//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/numeric_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <type_traits>

#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

template <class T>
struct MakeSigned {
	using type = typename std::make_signed<T>::type;
};

template <>
struct MakeSigned<hugeint_t> {
	using type = hugeint_t;
};

template <>
struct MakeSigned<uhugeint_t> {
	using type = hugeint_t;
};

template <class T>
struct MakeUnsigned {
	using type = typename std::make_unsigned<T>::type;
};

template <>
struct MakeUnsigned<hugeint_t> {
	using type = uhugeint_t;
};

template <>
struct MakeUnsigned<uhugeint_t> {
	using type = uhugeint_t;
};

template <class T>
struct IsIntegral {
	static constexpr bool value = std::is_integral<T>::value;
};

template <>
struct IsIntegral<hugeint_t> {
	static constexpr bool value = true;
};

template <>
struct IsIntegral<uhugeint_t> {
	static constexpr bool value = true;
};

template <class OUT, class IN>
static void ThrowNumericCastError(IN in, OUT minval, OUT maxval) {
	throw InternalException("Information loss on integer cast: value %d outside of target range [%d, %d]", in, minval,
	                        maxval);
}

template <class OUT, class IN>
OUT NumericCast(IN in) {
	// some dance around signed-unsigned integer comparison below
	auto min = NumericLimits<OUT>::Minimum();
	auto max = NumericLimits<OUT>::Maximum();
	auto unsigned_in = static_cast<typename MakeUnsigned<IN>::type>(in);
	auto unsigned_min = static_cast<typename MakeUnsigned<OUT>::type>(min);
	auto unsigned_max = static_cast<typename MakeUnsigned<OUT>::type>(max);
	auto signed_in = static_cast<typename MakeSigned<IN>::type>(in);
	auto signed_min = static_cast<typename MakeSigned<OUT>::type>(min);
	auto signed_max = static_cast<typename MakeSigned<OUT>::type>(max);

	if (std::is_unsigned<IN>() && std::is_unsigned<OUT>() &&
	    (unsigned_in < unsigned_min || unsigned_in > unsigned_max)) {
		ThrowNumericCastError(in, min, max);
	}

	if (std::is_signed<IN>() && std::is_signed<OUT>() && (signed_in < signed_min || signed_in > signed_max)) {
		ThrowNumericCastError(in, min, max);
	}

	if (std::is_signed<IN>() != std::is_signed<OUT>() && (signed_in < signed_min || unsigned_in > unsigned_max)) {
		ThrowNumericCastError(in, min, max);
	}

	return static_cast<OUT>(in);
}

template <class OUT>
OUT NumericCast(double val) {
	return static_cast<OUT>(val);
}

template <class OUT>
OUT NumericCast(float val) {
	return static_cast<OUT>(val);
}

template <class OUT, class IN>
OUT UnsafeNumericCast(IN in) {
#ifdef DEBUG
	return NumericCast<OUT, IN>(in);
#endif
	return static_cast<OUT>(in);
}

template <class OUT>
OUT UnsafeNumericCast(double val) {
	return NumericCast<OUT>(val);
}

template <class OUT>
OUT UnsafeNumericCast(float val) {
	return NumericCast<OUT>(val);
}

} // namespace duckdb
