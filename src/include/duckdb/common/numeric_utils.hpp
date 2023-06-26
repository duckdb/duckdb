//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bit_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <type_traits>
#include <typeinfo>
#include "duckdb/common/hugeint.hpp"

namespace duckdb {

template <class T>
struct MakeSigned {
	using type = typename std::make_signed<T>::type;
};

template <>
struct MakeSigned<hugeint_t> {
	using type = hugeint_t;
};

template <class T>
struct MakeUnsigned {
	using type = typename std::make_unsigned<T>::type;
};

template <>
struct MakeUnsigned<hugeint_t> {
	using type = hugeint_t;
};

template <class T>
struct IsIntegral {
	static constexpr bool value = std::is_integral<T>::value;
};

template <>
struct IsIntegral<hugeint_t> {
	static constexpr bool value = true;
};

} // namespace duckdb
