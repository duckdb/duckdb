// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#ifdef R_INTERNALS_H_
#if !(defined(R_NO_REMAP) && defined(STRICT_R_HEADERS))
#error R headers were included before cpp11 headers \
  and at least one of R_NO_REMAP or STRICT_R_HEADERS \
  was not defined.
#endif
#endif

#define R_NO_REMAP
#define STRICT_R_HEADERS
#include "Rinternals.h"

// clang-format off
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wattributes"
#endif

#ifdef __GNUC__
# pragma GCC diagnostic push
# pragma GCC diagnostic ignored "-Wattributes"
#endif
// clang-format on

#include "cpp11/altrep.hpp"

namespace cpp11 {
namespace literals {

constexpr R_xlen_t operator"" _xl(unsigned long long int value) { return value; }

}  // namespace literals

template <typename T>
inline T na();

template <typename T>
inline bool is_na(const T& value) {
  return value == na<T>();
}

}  // namespace cpp11
