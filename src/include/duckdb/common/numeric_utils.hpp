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

// TODO write a bunch of test cases on this!
// TODO bunch of other combos signed/unsigned

template <class OUT, class IN>
OUT UnsafeNumericCast(IN val) {
#ifdef DEBUG
	if (std::is_unsigned<OUT>() && val < 0) {
		throw InternalException("Information loss on integer cast: Negative value to unsigned");
	}
	auto unsigned_val = static_cast<typename MakeUnsigned<IN>::type>(val);
	auto unsigned_max = static_cast<typename MakeUnsigned<OUT>::type>(NumericLimits<OUT>::Maximum());
	if (std::is_unsigned<OUT>() && unsigned_val > unsigned_max) {
		throw InternalException("Information loss on integer cast: Value too large for target type");
	}
	auto signed_val = static_cast<typename MakeSigned<IN>::type>(val);
	auto signed_min = static_cast<typename MakeSigned<OUT>::type>(NumericLimits<OUT>::Minimum());
	if (std::is_signed<OUT>() && signed_val < signed_min) {
		throw InternalException("Information loss on integer cast: Value too small for target type");
	}

#endif
	return static_cast<OUT>(val);
}

template <class OUT>
OUT UnsafeNumericCast(double val) {
#ifdef DEBUG

#endif
	return static_cast<OUT>(val);
}

template <class OUT>
OUT UnsafeNumericCast(float val) {
#ifdef DEBUG

#endif
	return static_cast<OUT>(val);
}

template <class OUT, class IN>
OUT NumericCast(IN val) {
	return UnsafeNumericCast<OUT, IN>(val);
}

} // namespace duckdb
