// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include <algorithm>         // for min
#include <array>             // for array
#include <cstdint>           // for uint8_t
#include <initializer_list>  // for initializer_list

#include "cpp11/R.hpp"                // for RAW, SEXP, SEXPREC, Rf_allocVector
#include "cpp11/attribute_proxy.hpp"  // for attribute_proxy
#include "cpp11/named_arg.hpp"        // for named_arg
#include "cpp11/protect.hpp"          // for preserved
#include "cpp11/r_vector.hpp"         // for r_vector, r_vector<>::proxy
#include "cpp11/sexp.hpp"             // for sexp

// Specializations for raws

namespace cpp11 {

template <>
inline SEXP r_vector<uint8_t>::valid_type(SEXP data) {
  if (data == nullptr) {
    throw type_error(RAWSXP, NILSXP);
  }
  if (TYPEOF(data) != RAWSXP) {
    throw type_error(RAWSXP, TYPEOF(data));
  }
  return data;
}

template <>
inline uint8_t r_vector<uint8_t>::operator[](const R_xlen_t pos) const {
  // NOPROTECT: likely too costly to unwind protect every elt
  return is_altrep_ ? RAW_ELT(data_, pos) : data_p_[pos];
}

template <>
inline uint8_t* r_vector<uint8_t>::get_p(bool is_altrep, SEXP data) {
  if (is_altrep) {
    return nullptr;
  } else {
    return reinterpret_cast<uint8_t*>(RAW(data));
  }
}

template <>
inline void r_vector<uint8_t>::const_iterator::fill_buf(R_xlen_t pos) {
  using namespace cpp11::literals;
  length_ = std::min(64_xl, data_->size() - pos);
  unwind_protect(
      [&] { RAW_GET_REGION(data_->data_, pos, length_, (uint8_t*)buf_.data()); });
  block_start_ = pos;
}

typedef r_vector<uint8_t> raws;

namespace writable {

template <>
inline typename r_vector<uint8_t>::proxy& r_vector<uint8_t>::proxy::operator=(
    const uint8_t& rhs) {
  if (is_altrep_) {
    // NOPROTECT: likely too costly to unwind protect every set elt
    RAW(data_)[index_] = rhs;
  } else {
    *p_ = rhs;
  }
  return *this;
}

template <>
inline r_vector<uint8_t>::proxy::operator uint8_t() const {
  if (p_ == nullptr) {
    // NOPROTECT: likely too costly to unwind protect every elt
    return RAW(data_)[index_];
  } else {
    return *p_;
  }
}

template <>
inline r_vector<uint8_t>::r_vector(std::initializer_list<uint8_t> il)
    : cpp11::r_vector<uint8_t>(safe[Rf_allocVector](RAWSXP, il.size())),
      capacity_(il.size()) {
  protect_ = preserved.insert(data_);
  auto it = il.begin();
  for (R_xlen_t i = 0; i < capacity_; ++i, ++it) {
    data_p_[i] = *it;
  }
}

template <>
inline r_vector<uint8_t>::r_vector(std::initializer_list<named_arg> il)
    : cpp11::r_vector<uint8_t>(safe[Rf_allocVector](RAWSXP, il.size())),
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
        data_p_[i] = RAW_ELT(it->value(), 0);
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
inline void r_vector<uint8_t>::reserve(R_xlen_t new_capacity) {
  data_ = data_ == R_NilValue ? safe[Rf_allocVector](RAWSXP, new_capacity)
                              : safe[Rf_xlengthgets](data_, new_capacity);

  SEXP old_protect = protect_;
  protect_ = preserved.insert(data_);
  preserved.release(old_protect);

  data_p_ = reinterpret_cast<uint8_t*>(RAW(data_));
  capacity_ = new_capacity;
}

template <>
inline void r_vector<uint8_t>::push_back(uint8_t value) {
  while (length_ >= capacity_) {
    reserve(capacity_ == 0 ? 1 : capacity_ *= 2);
  }
  if (is_altrep_) {
    // NOPROTECT: likely too costly to unwind protect every elt
    RAW(data_)[length_] = value;
  } else {
    data_p_[length_] = value;
  }
  ++length_;
}

typedef r_vector<uint8_t> raws;

}  // namespace writable

}  // namespace cpp11
