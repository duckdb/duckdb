// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include "Rversion.h"

#if defined(R_VERSION) && R_VERSION >= R_Version(3, 5, 0)
#define HAS_ALTREP
#endif

#ifndef HAS_ALTREP

#define ALTREP(x) false

#define REAL_ELT(x, i) REAL(x)[i]
#define INTEGER_ELT(x, i) INTEGER(x)[i]
#define LOGICAL_ELT(x, i) LOGICAL(x)[i]
#define RAW_ELT(x, i) RAW(x)[i]

#define SET_REAL_ELT(x, i, val) REAL(x)[i] = val
#define SET_INTEGER_ELT(x, i, val) INTEGER(x)[i] = val
#define SET_LOGICAL_ELT(x, i, val) LOGICAL(x)[i] = val
#define SET_RAW_ELT(x, i, val) RAW(x)[i] = val

#define REAL_GET_REGION(...) \
  do {                       \
  } while (false)

#define INTEGER_GET_REGION(...) \
  do {                          \
  } while (false)
#endif

#if !defined HAS_ALTREP || (defined(R_VERSION) && R_VERSION < R_Version(3, 6, 0))

#define LOGICAL_GET_REGION(...) \
  do {                          \
  } while (false)

#define RAW_GET_REGION(...) \
  do {                      \
  } while (false)

#endif
