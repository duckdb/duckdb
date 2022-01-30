// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include <algorithm>         // for min
#include <array>             // for array
#include <initializer_list>  // for initializer_list

#include "R_ext/Arith.h"              // for NA_INTEGER
#include "cpp11/R.hpp"                // for SEXP, SEXPREC, Rf_allocVector
#include "cpp11/as.hpp"               // for as_sexp
#include "cpp11/attribute_proxy.hpp"  // for attribute_proxy
#include "cpp11/named_arg.hpp"        // for named_arg
#include "cpp11/protect.hpp"          // for preserved
#include "cpp11/r_vector.hpp"         // for r_vector, r_vector<>::proxy
#include "cpp11/sexp.hpp"             // for sexp

// Specializations for integers

namespace cpp11 {

template <>
inline SEXP r_vector<int>::valid_type(SEXP data) {
  if (data == nullptr) {
    throw type_error(INTSXP, NILSXP);
  }
  if (TYPEOF(data) != INTSXP) {
    throw type_error(INTSXP, TYPEOF(data));
  }
  return data;
}

template <>
inline int r_vector<int>::operator[](const R_xlen_t pos) const {
  // NOPROTECT: likely too costly to unwind protect every elt
  return is_altrep_ ? INTEGER_ELT(data_, pos) : data_p_[pos];
}

template <>
inline int* r_vector<int>::get_p(bool is_altrep, SEXP data) {
  if (is_altrep) {
    return nullptr;
  } else {
    return INTEGER(data);
  }
}

template <>
inline void r_vector<int>::const_iterator::fill_buf(R_xlen_t pos) {
  length_ = std::min(64_xl, data_->size() - pos);
  INTEGER_GET_REGION(data_->data_, pos, length_, buf_.data());
  block_start_ = pos;
}

typedef r_vector<int> integers;

namespace writable {

template <>
inline typename r_vector<int>::proxy& r_vector<int>::proxy::operator=(const int& rhs) {
  if (is_altrep_) {
    // NOPROTECT: likely too costly to unwind protect every set elt
    SET_INTEGER_ELT(data_, index_, rhs);
  } else {
    *p_ = rhs;
  }
  return *this;
}

template <>
inline r_vector<int>::proxy::operator int() const {
  if (p_ == nullptr) {
    // NOPROTECT: likely too costly to unwind protect every elt
    return INTEGER_ELT(data_, index_);
  } else {
    return *p_;
  }
}

template <>
inline r_vector<int>::r_vector(std::initializer_list<int> il)
    : cpp11::r_vector<int>(as_sexp(il)), capacity_(il.size()) {}

template <>
inline void r_vector<int>::reserve(R_xlen_t new_capacity) {
  data_ = data_ == R_NilValue ? safe[Rf_allocVector](INTSXP, new_capacity)
                              : safe[Rf_xlengthgets](data_, new_capacity);
  SEXP old_protect = protect_;

  // Protect the new data
  protect_ = preserved.insert(data_);

  // Release the old protection;
  preserved.release(old_protect);

  data_p_ = INTEGER(data_);
  capacity_ = new_capacity;
}

template <>
inline r_vector<int>::r_vector(std::initializer_list<named_arg> il)
    : cpp11::r_vector<int>(safe[Rf_allocVector](INTSXP, il.size())),
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
        data_p_[i] = INTEGER_ELT(it->value(), 0);
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
inline void r_vector<int>::push_back(int value) {
  while (length_ >= capacity_) {
    reserve(capacity_ == 0 ? 1 : capacity_ *= 2);
  }
  if (is_altrep_) {
    // NOPROTECT: likely too costly to unwind protect every elt
    SET_INTEGER_ELT(data_, length_, value);
  } else {
    data_p_[length_] = value;
  }
  ++length_;
}

typedef r_vector<int> integers;

}  // namespace writable

template <>
inline int na() {
  return NA_INTEGER;
}

// forward declaration

typedef r_vector<double> doubles;

inline integers as_integers(sexp x) {
  if (TYPEOF(x) == INTSXP) {
    return as_cpp<integers>(x);
  } else if (TYPEOF(x) == REALSXP) {
    doubles xn = as_cpp<doubles>(x);
    size_t len = (xn.size());
    writable::integers ret = writable::integers(len);
    for (size_t i = 0; i < len; ++i) {
      double el = xn[i];
      if (!is_convertable_without_loss_to_integer(el)) {
        throw std::runtime_error("All elements must be integer-like");
      }
      ret[i] = (static_cast<int>(el));
    }

    return ret;
  }

  throw type_error(INTSXP, TYPEOF(x));
}

}  // namespace cpp11
