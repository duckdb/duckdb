// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include <cstring>
#include <string>
#include <vector>

#ifndef CPP11_PARTIAL
#include "cpp11.hpp"
namespace writable = ::cpp11::writable;
using namespace ::cpp11;
#endif

#include <R_ext/Rdynload.h>

namespace cpp11 {
template <class T>
T& unmove(T&& t) {
  return t;
}
}  // namespace cpp11

#ifdef HAS_UNWIND_PROTECT
#define CPP11_UNWIND R_ContinueUnwind(err);
#else
#define CPP11_UNWIND \
  do {               \
  } while (false);
#endif

#define CPP11_ERROR_BUFSIZE 8192

#define BEGIN_CPP11                   \
  SEXP err = R_NilValue;              \
  char buf[CPP11_ERROR_BUFSIZE] = ""; \
  try {
#define END_CPP11_EX(RET)                                       \
  }                                                             \
  catch (cpp11::unwind_exception & e) {                         \
    err = e.token;                                              \
  }                                                             \
  catch (std::exception & e) {                                  \
    strncpy(buf, e.what(), sizeof(buf) - 1);                    \
  }                                                             \
  catch (...) {                                                 \
    strncpy(buf, "C++ error (unknown cause)", sizeof(buf) - 1); \
  }                                                             \
  if (buf[0] != '\0') {                                         \
    Rf_errorcall(R_NilValue, "%s", buf);                        \
  } else if (err != R_NilValue) {                               \
    CPP11_UNWIND                                                \
  }                                                             \
  return RET;
#define END_CPP11 END_CPP11_EX(R_NilValue)
