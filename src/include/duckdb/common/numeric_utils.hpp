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
OUT UnsafeNumericCast(IN val) {
	return static_cast<OUT>(val);
}

template <class OUT, class IN>
OUT NumericCast(IN val) {
	// TODO deal with signedness
	//#ifdef DEBUG
	//	if (val > NumericLimits<OUT>::Maximum() || val < NumericLimits<OUT>::Minimum()) {
	//		// TODO more details in error message
	//		throw InternalException("Information loss on integer cast");
	//	}
	//#endif
	return UnsafeNumericCast<OUT, IN>(val);
}

} // namespace duckdb
