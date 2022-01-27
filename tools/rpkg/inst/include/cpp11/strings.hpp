// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include <initializer_list>  // for initializer_list
#include <string>            // for string, basic_string

#include "cpp11/R.hpp"                // for SEXP, TYPEOF, SEXPREC, SET_STRI...
#include "cpp11/as.hpp"               // for as_sexp
#include "cpp11/attribute_proxy.hpp"  // for attribute_proxy
#include "cpp11/named_arg.hpp"        // for named_arg
#include "cpp11/protect.hpp"          // for preserved
#include "cpp11/r_string.hpp"         // for r_string
#include "cpp11/r_vector.hpp"         // for r_vector, r_vector<>::proxy
#include "cpp11/sexp.hpp"             // for sexp

// Specializations for strings

namespace cpp11 {

template <>
inline SEXP r_vector<r_string>::valid_type(SEXP data) {
  if (data == nullptr) {
    throw type_error(STRSXP, NILSXP);
  }
  if (TYPEOF(data) != STRSXP) {
    throw type_error(STRSXP, TYPEOF(data));
  }
  return data;
}

template <>
inline r_string r_vector<r_string>::operator[](const R_xlen_t pos) const {
  // NOPROTECT: likely too costly to unwind protect every elt
  return STRING_ELT(data_, pos);
}

template <>
inline r_string* r_vector<r_string>::get_p(bool, SEXP) {
  return nullptr;
}

template <>
inline void r_vector<r_string>::const_iterator::fill_buf(R_xlen_t) {
  return;
}

template <>
inline r_string r_vector<r_string>::const_iterator::operator*() const {
  return STRING_ELT(data_->data(), pos_);
}

typedef r_vector<r_string> strings;

namespace writable {

template <>
inline typename r_vector<r_string>::proxy& r_vector<r_string>::proxy::operator=(
    const r_string& rhs) {
  unwind_protect([&] { SET_STRING_ELT(data_, index_, rhs); });
  return *this;
}

template <>
inline r_vector<r_string>::proxy::operator r_string() const {
  // NOPROTECT: likely too costly to unwind protect every elt
  return STRING_ELT(data_, index_);
}

inline bool operator==(const r_vector<r_string>::proxy& lhs, r_string rhs) {
  return static_cast<r_string>(lhs).operator==(static_cast<std::string>(rhs).c_str());
}

inline SEXP alloc_or_copy(const SEXP data) {
  switch (TYPEOF(data)) {
    case CHARSXP:
      return cpp11::r_vector<r_string>(safe[Rf_allocVector](STRSXP, 1));
    case STRSXP:
      return safe[Rf_shallow_duplicate](data);
    default:
      throw type_error(STRSXP, TYPEOF(data));
  }
}

inline SEXP alloc_if_charsxp(const SEXP data) {
  switch (TYPEOF(data)) {
    case CHARSXP:
      return cpp11::r_vector<r_string>(safe[Rf_allocVector](STRSXP, 1));
    case STRSXP:
      return data;
    default:
      throw type_error(STRSXP, TYPEOF(data));
  }
}

template <>
inline r_vector<r_string>::r_vector(const SEXP& data)
    : cpp11::r_vector<r_string>(alloc_or_copy(data)),
      protect_(preserved.insert(data_)),
      capacity_(length_) {
  if (TYPEOF(data) == CHARSXP) {
    SET_STRING_ELT(data_, 0, data);
  }
}

template <>
inline r_vector<r_string>::r_vector(SEXP&& data)
    : cpp11::r_vector<r_string>(alloc_if_charsxp(data)),
      protect_(preserved.insert(data_)),
      capacity_(length_) {
  if (TYPEOF(data) == CHARSXP) {
    SET_STRING_ELT(data_, 0, data);
  }
}

template <>
inline r_vector<r_string>::r_vector(std::initializer_list<r_string> il)
    : cpp11::r_vector<r_string>(as_sexp(il)), capacity_(il.size()) {}

template <>
inline r_vector<r_string>::r_vector(std::initializer_list<named_arg> il)
    : cpp11::r_vector<r_string>(safe[Rf_allocVector](STRSXP, il.size())),
      capacity_(il.size()) {
  protect_ = preserved.insert(data_);
  int n_protected = 0;

  try {
    unwind_protect([&] {
      Rf_setAttrib(data_, R_NamesSymbol, Rf_allocVector(STRSXP, capacity_));
      SEXP names = PROTECT(Rf_getAttrib(data_, R_NamesSymbol));
      ++n_protected;
      auto it = il.begin();
      for (R_xlen_t i = 0; i < capacity_; ++i, ++it) {
        SET_STRING_ELT(data_, i, STRING_ELT(it->value(), 0));
        SET_STRING_ELT(names, i, Rf_mkCharCE(it->name(), CE_UTF8));
      }
      UNPROTECT(n_protected);
    });
  } catch (const unwind_exception& e) {
    preserved.release(protect_);
    UNPROTECT(n_protected);
    throw e;
  }
}

template <>
inline void r_vector<r_string>::reserve(R_xlen_t new_capacity) {
  data_ = data_ == R_NilValue ? safe[Rf_allocVector](STRSXP, new_capacity)
                              : safe[Rf_xlengthgets](data_, new_capacity);

  SEXP old_protect = protect_;
  protect_ = preserved.insert(data_);
  preserved.release(old_protect);

  capacity_ = new_capacity;
}

template <>
inline void r_vector<r_string>::push_back(r_string value) {
  while (length_ >= capacity_) {
    reserve(capacity_ == 0 ? 1 : capacity_ *= 2);
  }
  unwind_protect([&] { SET_STRING_ELT(data_, length_, value); });
  ++length_;
}

typedef r_vector<r_string> strings;

template <typename T>
inline void r_vector<T>::push_back(const named_arg& value) {
  push_back(value.value());
  if (Rf_xlength(names()) == 0) {
    cpp11::writable::strings new_nms(size());
    names() = new_nms;
  }
  cpp11::writable::strings nms(names());
  nms[size() - 1] = value.name();
}

}  // namespace writable

}  // namespace cpp11
