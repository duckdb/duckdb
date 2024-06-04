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

template <class TO, class FROM>
static void ThrowNumericCastError(FROM in, TO minval, TO maxval) {
	throw InternalException("Information loss on integer cast: value %d outside of target range [%d, %d]", in, minval,
	                        maxval);
}

template <class TO, class FROM>
TO NumericCast(FROM val) {
	if (std::is_same<TO, FROM>::value) {
		return static_cast<TO>(val);
	}
	// some dance around signed-unsigned integer comparison below
	auto minval = NumericLimits<TO>::Minimum();
	auto maxval = NumericLimits<TO>::Maximum();
	auto unsigned_in = static_cast<typename MakeUnsigned<FROM>::type>(val);
	auto unsigned_min = static_cast<typename MakeUnsigned<TO>::type>(minval);
	auto unsigned_max = static_cast<typename MakeUnsigned<TO>::type>(maxval);
	auto signed_in = static_cast<typename MakeSigned<FROM>::type>(val);
	auto signed_min = static_cast<typename MakeSigned<TO>::type>(minval);
	auto signed_max = static_cast<typename MakeSigned<TO>::type>(maxval);

	if (std::is_unsigned<FROM>() && std::is_unsigned<TO>() &&
	    (unsigned_in < unsigned_min || unsigned_in > unsigned_max)) {
		ThrowNumericCastError(val, minval, maxval);
	}

	if (std::is_signed<FROM>() && std::is_signed<TO>() && (signed_in < signed_min || signed_in > signed_max)) {
		ThrowNumericCastError(val, minval, maxval);
	}

	if (std::is_signed<FROM>() != std::is_signed<TO>() && (signed_in < signed_min || unsigned_in > unsigned_max)) {
		ThrowNumericCastError(val, minval, maxval);
	}

	return static_cast<TO>(val);
}

template <class TO>
TO NumericCast(double val) {
	return static_cast<TO>(val);
}

template <class TO>
TO NumericCast(float val) {
	return static_cast<TO>(val);
}

template <class TO, class FROM>
TO UnsafeNumericCast(FROM in) {
#ifdef DEBUG
	return NumericCast<TO, FROM>(in);
#endif
	return static_cast<TO>(in);
}

template <class TO>
TO UnsafeNumericCast(double val) {
	return NumericCast<TO>(val);
}

template <class TO>
TO UnsafeNumericCast(float val) {
	return NumericCast<TO>(val);
}

} // namespace duckdb
