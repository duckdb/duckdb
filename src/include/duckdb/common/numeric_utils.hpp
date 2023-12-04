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

// hugeint_t does not actually have an unsigned variant (yet), but this is required to make compression work
// if an unsigned variant gets implemented this (probably) can be changed without breaking anything
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
