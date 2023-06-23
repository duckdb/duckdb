//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bit_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <type_traits>
#include "duckdb/common/hugeint.hpp"

namespace duckdb {

// Make Signed

template <class T>
struct make_signed {
	using type = typename std::make_signed<T>::type;
};

template <>
struct make_signed<hugeint_t> {
	using type = hugeint_t;
};

// Make Unsigned

template <class T>
struct make_unsigned {
	using type = typename std::make_unsigned<T>::type;
};

template <>
struct make_unsigned<hugeint_t> {
	using type = hugeint_t;
};

// Is Integral

template <class T>
struct is_integral {
	static constexpr bool value = std::is_integral<T>::value;
};

template <>
struct is_integral<hugeint_t> {
	static constexpr bool value = true;
};

} // namespace duckdb
