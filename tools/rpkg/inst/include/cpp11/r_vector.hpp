// cpp11 version: 0.4.2
// vendored on: 2022-01-10
#pragma once

#include <stddef.h>  // for ptrdiff_t, size_t

#include <algorithm>         // for max
#include <array>             // for array
#include <cstdio>            // for snprintf
#include <exception>         // for exception
#include <initializer_list>  // for initializer_list
#include <iterator>          // for forward_iterator_tag, random_ac...
#include <stdexcept>         // for out_of_range
#include <string>            // for string, basic_string
#include <type_traits>       // for decay, is_same, enable_if, is_c...
#include <utility>           // for declval

#include "cpp11/R.hpp"                // for R_xlen_t, SEXP, SEXPREC, Rf_xle...
#include "cpp11/attribute_proxy.hpp"  // for attribute_proxy
#include "cpp11/protect.hpp"          // for preserved
#include "cpp11/r_string.hpp"         // for r_string
#include "cpp11/sexp.hpp"             // for sexp

namespace cpp11 {

using namespace cpp11::literals;

class type_error : public std::exception {
 public:
  type_error(int expected, int actual) : expected_(expected), actual_(actual) {}
  virtual const char* what() const noexcept override {
    snprintf(str_, 64, "Invalid input type, expected '%s' actual '%s'",
             Rf_type2char(expected_), Rf_type2char(actual_));
    return str_;
  }

 private:
  int expected_;
  int actual_;
  mutable char str_[64];
};

// Forward Declarations
class named_arg;

namespace writable {
template <typename T>
class r_vector;
}  // namespace writable

// Declarations
template <typename T>
class r_vector {
 public:
  typedef ptrdiff_t difference_type;
  typedef size_t size_type;
  typedef T value_type;
  typedef T* pointer;
  typedef T& reference;

  r_vector() noexcept = default;

  r_vector(SEXP data);

  r_vector(SEXP data, bool is_altrep);

#ifdef LONG_VECTOR_SUPPORT
  T operator[](const int pos) const;
  T at(const int pos) const;
#endif
  T operator[](const R_xlen_t pos) const;
  T operator[](const size_type pos) const;
  T operator[](const r_string& name) const;

  T at(const R_xlen_t pos) const;
  T at(const size_type pos) const;
  T at(const r_string& name) const;

  bool contains(const r_string& name) const;

  r_vector& operator=(const r_vector& rhs) {
    SEXP old_protect = protect_;

    data_ = rhs.data_;
    protect_ = preserved.insert(data_);
    is_altrep_ = rhs.is_altrep_;
    data_p_ = rhs.data_p_;
    length_ = rhs.length_;

    preserved.release(old_protect);

    return *this;
  };

  r_vector(const r_vector& rhs) {
    SEXP old_protect = protect_;

    data_ = rhs.data_;
    protect_ = preserved.insert(data_);
    is_altrep_ = rhs.is_altrep_;
    data_p_ = rhs.data_p_;
    length_ = rhs.length_;

    preserved.release(old_protect);
  };

  r_vector(const writable::r_vector<T>& rhs) : r_vector(static_cast<SEXP>(rhs)) {}
  r_vector(named_arg) = delete;

  bool is_altrep() const;

  bool named() const;

  R_xlen_t size() const;

  operator SEXP() const;

  operator sexp() const;

  bool empty() const;

  /// Provide access to the underlying data, mainly for interface
  /// compatibility with std::vector
  SEXP data() const;

  const sexp attr(const char* name) const {
    return SEXP(attribute_proxy<r_vector<T>>(*this, name));
  }

  const sexp attr(const std::string& name) const {
    return SEXP(attribute_proxy<r_vector<T>>(*this, name.c_str()));
  }

  const sexp attr(SEXP name) const {
    return SEXP(attribute_proxy<r_vector<T>>(*this, name));
  }

  r_vector<r_string> names() const {
    SEXP nms = SEXP(Rf_getAttrib(data_, R_NamesSymbol));
    if (nms == R_NilValue) {
      return r_vector<r_string>();
    }

    return r_vector<r_string>(nms);
  }

  class const_iterator {
   public:
    using difference_type = ptrdiff_t;
    using value_type = T;
    using pointer = T*;
    using reference = T&;
    using iterator_category = std::random_access_iterator_tag;

    const_iterator(const r_vector* data, R_xlen_t pos);

    inline const_iterator operator+(R_xlen_t pos);
    inline ptrdiff_t operator-(const const_iterator& other) const;

    inline const_iterator& operator++();
    inline const_iterator& operator--();

    inline const_iterator& operator+=(R_xlen_t pos);
    inline const_iterator& operator-=(R_xlen_t pos);

    inline bool operator!=(const const_iterator& other) const;
    inline bool operator==(const const_iterator& other) const;

    inline T operator*() const;

    friend class writable::r_vector<T>::iterator;

   private:
    const r_vector* data_;
    void fill_buf(R_xlen_t pos);

    R_xlen_t pos_;
    std::array<T, 64 * 64> buf_;
    R_xlen_t block_start_ = 0;
    R_xlen_t length_ = 0;
  };

 public:
  const_iterator begin() const;
  const_iterator end() const;

  const_iterator cbegin() const;
  const_iterator cend() const;

  const_iterator find(const r_string& name) const;

  ~r_vector() { preserved.release(protect_); }

 private:
  SEXP data_ = R_NilValue;
  SEXP protect_ = R_NilValue;
  bool is_altrep_ = false;
  T* data_p_ = nullptr;
  R_xlen_t length_ = 0;

  static T* get_p(bool is_altrep, SEXP data);

  static SEXP valid_type(SEXP data);

  friend class writable::r_vector<T>;
};

namespace writable {

template <typename T>
using has_begin_fun = std::decay<decltype(*begin(std::declval<T>()))>;

/// Read/write access to new or copied r_vectors
template <typename T>
class r_vector : public cpp11::r_vector<T> {
 private:
  SEXP protect_ = R_NilValue;

  // These are necessary because type names are not directly accessible in
  // template inheritance
  using cpp11::r_vector<T>::data_;
  using cpp11::r_vector<T>::data_p_;
  using cpp11::r_vector<T>::is_altrep_;
  using cpp11::r_vector<T>::length_;

  R_xlen_t capacity_ = 0;

 public:
  class proxy {
   private:
    const SEXP data_;
    const R_xlen_t index_;
    T* const p_;
    bool is_altrep_;

   public:
    proxy(SEXP data, const R_xlen_t index, T* const p, bool is_altrep);

    proxy& operator=(const T& rhs);
    proxy& operator+=(const T& rhs);
    proxy& operator-=(const T& rhs);
    proxy& operator*=(const T& rhs);
    proxy& operator/=(const T& rhs);
    proxy& operator++(int);
    proxy& operator--(int);

    void operator++();
    void operator--();

    operator T() const;
  };

  typedef ptrdiff_t difference_type;
  typedef size_t size_type;
  typedef proxy value_type;
  typedef proxy* pointer;
  typedef proxy& reference;

  class iterator : public cpp11::r_vector<T>::const_iterator {
   private:
    const r_vector& data_;
    using cpp11::r_vector<T>::const_iterator::block_start_;
    using cpp11::r_vector<T>::const_iterator::pos_;
    using cpp11::r_vector<T>::const_iterator::buf_;
    using cpp11::r_vector<T>::const_iterator::length_;
    using cpp11::r_vector<T>::const_iterator::fill_buf;

   public:
    using difference_type = ptrdiff_t;
    using value_type = proxy;
    using pointer = proxy*;
    using reference = proxy&;
    using iterator_category = std::forward_iterator_tag;

    iterator(const r_vector& data, R_xlen_t pos);

    inline iterator& operator++();

    inline proxy operator*() const;

    using cpp11::r_vector<T>::const_iterator::operator!=;

    inline iterator& operator+=(R_xlen_t rhs);
    inline iterator operator+(R_xlen_t rhs);
  };

  r_vector() noexcept = default;
  r_vector(const SEXP& data);
  r_vector(SEXP&& data);
  r_vector(const SEXP& data, bool is_altrep);
  r_vector(SEXP&& data, bool is_altrep);
  r_vector(std::initializer_list<T> il);
  r_vector(std::initializer_list<named_arg> il);

  template <typename Iter>
  r_vector(Iter first, Iter last);

  template <typename V, typename W = has_begin_fun<V>>
  r_vector(const V& obj);

  explicit r_vector(const R_xlen_t size);

  ~r_vector();

  r_vector(const r_vector& rhs);
  r_vector(r_vector&& rhs);

  r_vector(const cpp11::r_vector<T>& rhs);

  r_vector& operator=(const r_vector& rhs);
  r_vector& operator=(r_vector&& rhs);

#ifdef LONG_VECTOR_SUPPORT
  proxy operator[](const int pos) const;
  proxy at(const int pos) const;
#endif
  proxy operator[](const R_xlen_t pos) const;
  proxy operator[](const size_type pos) const;
  proxy operator[](const r_string& name) const;

  proxy at(const R_xlen_t pos) const;
  proxy at(const size_type pos) const;
  proxy at(const r_string& name) const;

  void push_back(T value);
  void push_back(const named_arg& value);
  void pop_back();

  void resize(R_xlen_t count);

  void reserve(R_xlen_t new_capacity);

  iterator insert(R_xlen_t pos, T value);
  iterator erase(R_xlen_t pos);

  void clear();

  iterator begin() const;
  iterator end() const;

  using cpp11::r_vector<T>::cbegin;
  using cpp11::r_vector<T>::cend;
  using cpp11::r_vector<T>::size;

  iterator find(const r_string& name) const;

  attribute_proxy<r_vector<T>> attr(const char* name) const {
    return attribute_proxy<r_vector<T>>(*this, name);
  }

  attribute_proxy<r_vector<T>> attr(const std::string& name) const {
    return attribute_proxy<r_vector<T>>(*this, name.c_str());
  }

  attribute_proxy<r_vector<T>> attr(SEXP name) const {
    return attribute_proxy<r_vector<T>>(*this, name);
  }

  attribute_proxy<r_vector<T>> names() const {
    return attribute_proxy<r_vector<T>>(*this, R_NamesSymbol);
  }

  operator SEXP() const;
};
}  // namespace writable

// Implementations below

template <typename T>
inline r_vector<T>::r_vector(const SEXP data)
    : data_(valid_type(data)),
      protect_(preserved.insert(data)),
      is_altrep_(ALTREP(data)),
      data_p_(get_p(ALTREP(data), data)),
      length_(Rf_xlength(data)) {}

template <typename T>
inline r_vector<T>::r_vector(const SEXP data, bool is_altrep)
    : data_(valid_type(data)),
      protect_(preserved.insert(data)),
      is_altrep_(is_altrep),
      data_p_(get_p(is_altrep, data)),
      length_(Rf_xlength(data)) {}

template <typename T>
inline bool r_vector<T>::is_altrep() const {
  return is_altrep_;
}

template <typename T>
inline bool r_vector<T>::named() const {
  return Rf_getAttrib(data_, R_NamesSymbol) != R_NilValue;
}

template <typename T>
inline R_xlen_t r_vector<T>::size() const {
  return length_;
}

template <typename T>
inline r_vector<T>::operator SEXP() const {
  return data_;
}

template <typename T>
inline bool r_vector<T>::empty() const {
  return (!(this->size() > 0));
}

template <typename T>
inline r_vector<T>::operator sexp() const {
  return data_;
}

/// Provide access to the underlying data, mainly for interface
/// compatibility with std::vector
template <typename T>
inline SEXP r_vector<T>::data() const {
  return data_;
}

template <typename T>
inline typename r_vector<T>::const_iterator r_vector<T>::begin() const {
  return const_iterator(this, 0);
}

template <typename T>
inline typename r_vector<T>::const_iterator r_vector<T>::end() const {
  return const_iterator(this, length_);
}

template <typename T>
inline typename r_vector<T>::const_iterator r_vector<T>::cbegin() const {
  return const_iterator(this, 0);
}

template <typename T>
inline typename r_vector<T>::const_iterator r_vector<T>::cend() const {
  return const_iterator(this, length_);
}

template <typename T>
r_vector<T>::const_iterator::const_iterator(const r_vector* data, R_xlen_t pos)
    : data_(data), pos_(pos), buf_() {
  if (data_->is_altrep()) {
    fill_buf(pos);
  }
}

template <typename T>
inline typename r_vector<T>::const_iterator& r_vector<T>::const_iterator::operator++() {
  ++pos_;
  if (data_->is_altrep() && pos_ >= block_start_ + length_) {
    fill_buf(pos_);
  }
  return *this;
}

template <typename T>
inline typename r_vector<T>::const_iterator& r_vector<T>::const_iterator::operator--() {
  --pos_;
  if (data_->is_altrep() && pos_ > 0 && pos_ < block_start_) {
    fill_buf(std::max(0_xl, pos_ - 64));
  }
  return *this;
}

template <typename T>
inline typename r_vector<T>::const_iterator& r_vector<T>::const_iterator::operator+=(
    R_xlen_t i) {
  pos_ += i;
  if (data_->is_altrep() && pos_ >= block_start_ + length_) {
    fill_buf(pos_);
  }
  return *this;
}

template <typename T>
inline typename r_vector<T>::const_iterator& r_vector<T>::const_iterator::operator-=(
    R_xlen_t i) {
  pos_ -= i;
  if (data_->is_altrep() && pos_ >= block_start_ + length_) {
    fill_buf(std::max(0_xl, pos_ - 64));
  }
  return *this;
}

template <typename T>
inline bool r_vector<T>::const_iterator::operator!=(
    const r_vector<T>::const_iterator& other) const {
  return pos_ != other.pos_;
}

template <typename T>
inline bool r_vector<T>::const_iterator::operator==(
    const r_vector<T>::const_iterator& other) const {
  return pos_ == other.pos_;
}

template <typename T>
inline ptrdiff_t r_vector<T>::const_iterator::operator-(
    const r_vector<T>::const_iterator& other) const {
  return pos_ - other.pos_;
}

template <typename T>
inline typename r_vector<T>::const_iterator r_vector<T>::const_iterator::operator+(
    R_xlen_t rhs) {
  auto it = *this;
  it += rhs;
  return it;
}

template <typename T>
inline T cpp11::r_vector<T>::at(R_xlen_t pos) const {
  if (pos < 0 || pos >= length_) {
    throw std::out_of_range("r_vector");
  }

  return operator[](pos);
}

template <typename T>
inline T cpp11::r_vector<T>::at(size_type pos) const {
  return at(static_cast<R_xlen_t>(pos));
}

template <typename T>
inline T cpp11::r_vector<T>::operator[](const r_string& name) const {
  SEXP names = this->names();
  R_xlen_t size = Rf_xlength(names);

  for (R_xlen_t pos = 0; pos < size; ++pos) {
    auto cur = Rf_translateCharUTF8(STRING_ELT(names, pos));
    if (name == cur) {
      return operator[](pos);
    }
  }

  throw std::out_of_range("r_vector");
}

template <typename T>
inline bool cpp11::r_vector<T>::contains(const r_string& name) const {
  SEXP names = this->names();
  R_xlen_t size = Rf_xlength(names);

  for (R_xlen_t pos = 0; pos < size; ++pos) {
    auto cur = Rf_translateCharUTF8(STRING_ELT(names, pos));
    if (name == cur) {
      return true;
    }
  }

  return false;
}

template <typename T>
inline typename cpp11::r_vector<T>::const_iterator cpp11::r_vector<T>::find(
    const r_string& name) const {
  SEXP names = this->names();
  R_xlen_t size = Rf_xlength(names);

  for (R_xlen_t pos = 0; pos < size; ++pos) {
    auto cur = Rf_translateCharUTF8(STRING_ELT(names, pos));
    if (name == cur) {
      return begin() + pos;
    }
  }

  return end();
}

template <typename T>
inline T r_vector<T>::const_iterator::operator*() const {
  if (data_->is_altrep()) {
    return buf_[pos_ - block_start_];
  } else {
    return data_->data_p_[pos_];
  }
}

#ifdef LONG_VECTOR_SUPPORT
template <typename T>
inline T r_vector<T>::operator[](const int pos) const {
  return operator[](static_cast<R_xlen_t>(pos));
}

template <typename T>
inline T r_vector<T>::at(const int pos) const {
  return at(static_cast<R_xlen_t>(pos));
}
#endif

template <typename T>
inline T r_vector<T>::operator[](size_type pos) const {
  return operator[](static_cast<R_xlen_t>(pos));
}

namespace writable {

template <typename T>
r_vector<T>::proxy::proxy(SEXP data, const R_xlen_t index, T* const p, bool is_altrep)
    : data_(data), index_(index), p_(p), is_altrep_(is_altrep) {}

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::iterator::operator*() const {
  if (data_.is_altrep()) {
    return proxy(data_.data(), pos_, const_cast<T*>(&buf_[pos_ - block_start_]), true);
  } else {
    return proxy(data_.data(), pos_,
                 data_.data_p_ != nullptr ? &data_.data_p_[pos_] : nullptr, false);
  }
}

template <typename T>
r_vector<T>::iterator::iterator(const r_vector& data, R_xlen_t pos)
    : r_vector<T>::const_iterator(&data, pos), data_(data) {}

template <typename T>
inline typename r_vector<T>::iterator& r_vector<T>::iterator::operator++() {
  ++pos_;
  if (data_.is_altrep() && pos_ >= block_start_ + length_) {
    fill_buf(pos_);
  }
  return *this;
}

template <typename T>
inline typename r_vector<T>::iterator& r_vector<T>::iterator::operator+=(R_xlen_t rhs) {
  pos_ += rhs;
  if (data_.is_altrep() && pos_ >= block_start_ + length_) {
    fill_buf(pos_);
  }
  return *this;
}

template <typename T>
inline typename r_vector<T>::iterator r_vector<T>::iterator::operator+(R_xlen_t rhs) {
  auto it = *this;
  it += rhs;
  return it;
}

template <typename T>
inline typename r_vector<T>::iterator r_vector<T>::begin() const {
  return iterator(*this, 0);
}

template <typename T>
inline typename r_vector<T>::iterator r_vector<T>::end() const {
  return iterator(*this, length_);
}

template <typename T>
inline r_vector<T>::r_vector(const SEXP& data)
    : cpp11::r_vector<T>(safe[Rf_shallow_duplicate](data)),
      protect_(preserved.insert(data_)),
      capacity_(length_) {}

template <typename T>
inline r_vector<T>::r_vector(const SEXP& data, bool is_altrep)
    : cpp11::r_vector<T>(safe[Rf_shallow_duplicate](data), is_altrep),
      protect_(preserved.insert(data_)),
      capacity_(length_) {}

template <typename T>
inline r_vector<T>::r_vector(SEXP&& data)
    : cpp11::r_vector<T>(data), protect_(preserved.insert(data_)), capacity_(length_) {}

template <typename T>
inline r_vector<T>::r_vector(SEXP&& data, bool is_altrep)
    : cpp11::r_vector<T>(data, is_altrep),
      protect_(preserved.insert(data_)),
      capacity_(length_) {}

template <typename T>
template <typename Iter>
inline r_vector<T>::r_vector(Iter first, Iter last) : r_vector() {
  reserve(last - first);
  while (first != last) {
    push_back(*first);
    ++first;
  }
}

template <typename T>
template <typename V, typename W>
inline r_vector<T>::r_vector(const V& obj) : r_vector() {
  auto first = obj.begin();
  auto last = obj.end();
  reserve(last - first);
  while (first != last) {
    push_back(*first);
    ++first;
  }
}

template <typename T>
inline r_vector<T>::r_vector(const R_xlen_t size) : r_vector() {
  resize(size);
}

template <typename T>
inline r_vector<T>::~r_vector() {
  preserved.release(protect_);
}

#ifdef LONG_VECTOR_SUPPORT
template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::operator[](const int pos) const {
  return operator[](static_cast<R_xlen_t>(pos));
}

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::at(const int pos) const {
  return at(static_cast<R_xlen_t>(pos));
}
#endif

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::operator[](const R_xlen_t pos) const {
  if (is_altrep_) {
    return {data_, pos, nullptr, true};
  }
  return {data_, pos, data_p_ != nullptr ? &data_p_[pos] : nullptr, false};
}

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::operator[](size_type pos) const {
  return operator[](static_cast<R_xlen_t>(pos));
}

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::at(const R_xlen_t pos) const {
  if (pos < 0 || pos >= length_) {
    throw std::out_of_range("r_vector");
  }
  return operator[](static_cast<R_xlen_t>(pos));
}

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::at(size_type pos) const {
  return at(static_cast<R_xlen_t>(pos));
}

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::operator[](const r_string& name) const {
  SEXP names = PROTECT(this->names());
  R_xlen_t size = Rf_xlength(names);

  for (R_xlen_t pos = 0; pos < size; ++pos) {
    auto cur = Rf_translateCharUTF8(STRING_ELT(names, pos));
    if (name == cur) {
      UNPROTECT(1);
      return operator[](pos);
    }
  }

  UNPROTECT(1);
  throw std::out_of_range("r_vector");
}

template <typename T>
inline typename r_vector<T>::proxy r_vector<T>::at(const r_string& name) const {
  return operator[](name);
}

template <typename T>
inline typename r_vector<T>::iterator r_vector<T>::find(const r_string& name) const {
  SEXP names = PROTECT(this->names());
  R_xlen_t size = Rf_xlength(names);

  for (R_xlen_t pos = 0; pos < size; ++pos) {
    auto cur = Rf_translateCharUTF8(STRING_ELT(names, pos));
    if (name == cur) {
      UNPROTECT(1);
      return begin() + pos;
    }
  }

  UNPROTECT(1);
  return end();
}

template <typename T>
inline r_vector<T>::r_vector(const r_vector<T>& rhs)
    : cpp11::r_vector<T>(safe[Rf_shallow_duplicate](rhs)),
      protect_(preserved.insert(data_)),
      capacity_(rhs.capacity_) {}

template <typename T>
inline r_vector<T>::r_vector(r_vector<T>&& rhs)
    : cpp11::r_vector<T>(rhs), protect_(rhs.protect_), capacity_(rhs.capacity_) {
  rhs.data_ = R_NilValue;
  rhs.protect_ = R_NilValue;
}

template <typename T>
inline r_vector<T>::r_vector(const cpp11::r_vector<T>& rhs)
    : cpp11::r_vector<T>(safe[Rf_shallow_duplicate](rhs)),
      protect_(preserved.insert(data_)),
      capacity_(rhs.length_) {}

// We don't release the old object until the end in case we throw an exception
// during the duplicate.
template <typename T>
inline r_vector<T>& r_vector<T>::operator=(const r_vector<T>& rhs) {
  if (data_ == rhs.data_) {
    return *this;
  }

  cpp11::r_vector<T>::operator=(rhs);

  auto old_protect = protect_;

  data_ = safe[Rf_shallow_duplicate](rhs.data_);
  protect_ = preserved.insert(data_);

  preserved.release(old_protect);

  capacity_ = rhs.capacity_;

  return *this;
}

template <typename T>
inline r_vector<T>& r_vector<T>::operator=(r_vector<T>&& rhs) {
  if (data_ == rhs.data_) {
    return *this;
  }

  cpp11::r_vector<T>::operator=(rhs);

  SEXP old_protect = protect_;

  data_ = rhs.data_;
  protect_ = preserved.insert(data_);

  preserved.release(old_protect);

  capacity_ = rhs.capacity_;

  rhs.data_ = R_NilValue;
  rhs.protect_ = R_NilValue;

  return *this;
}

template <typename T>
inline void r_vector<T>::pop_back() {
  --length_;
}

template <typename T>
inline void r_vector<T>::resize(R_xlen_t count) {
  reserve(count);
  length_ = count;
}

template <typename T>
inline typename r_vector<T>::iterator r_vector<T>::insert(R_xlen_t pos, T value) {
  push_back(value);

  R_xlen_t i = length_ - 1;
  while (i > pos) {
    operator[](i) = (T) operator[](i - 1);
    --i;
  };
  operator[](pos) = value;

  return begin() + pos;
}

template <typename T>
inline typename r_vector<T>::iterator r_vector<T>::erase(R_xlen_t pos) {
  R_xlen_t i = pos;
  while (i < length_ - 1) {
    operator[](i) = (T) operator[](i + 1);
    ++i;
  }
  pop_back();

  return begin() + pos;
}

template <typename T>
inline void r_vector<T>::clear() {
  length_ = 0;
}

inline SEXP truncate(SEXP x, R_xlen_t length, R_xlen_t capacity) {
#if R_VERSION >= R_Version(3, 4, 0)
  SETLENGTH(x, length);
  SET_TRUELENGTH(x, capacity);
  SET_GROWABLE_BIT(x);
#else
  x = safe[Rf_lengthgets](x, length);
#endif
  return x;
}

template <typename T>
inline r_vector<T>::operator SEXP() const {
  auto* p = const_cast<r_vector<T>*>(this);
  if (data_ == R_NilValue) {
    p->resize(0);
    return data_;
  }
  if (length_ < capacity_) {
    p->data_ = truncate(p->data_, length_, capacity_);
    SEXP nms = names();
    auto nms_size = Rf_xlength(nms);
    if ((nms_size > 0) && (length_ < nms_size)) {
      nms = truncate(nms, length_, capacity_);
      names() = nms;
    }
  }
  return data_;
}

template <typename T>
inline typename r_vector<T>::proxy& r_vector<T>::proxy::operator+=(const T& rhs) {
  operator=(static_cast<T>(*this) + rhs);
  return *this;
}

template <typename T>
inline typename r_vector<T>::proxy& r_vector<T>::proxy::operator-=(const T& rhs) {
  operator=(static_cast<T>(*this) - rhs);
  return *this;
}

template <typename T>
inline typename r_vector<T>::proxy& r_vector<T>::proxy::operator*=(const T& rhs) {
  operator=(static_cast<T>(*this) * rhs);
  return *this;
}

template <typename T>
inline typename r_vector<T>::proxy& r_vector<T>::proxy::operator/=(const T& rhs) {
  operator=(static_cast<T>(*this) / rhs);
  return *this;
}

template <typename T>
inline typename r_vector<T>::proxy& r_vector<T>::proxy::operator++(int) {
  operator=(static_cast<T>(*this) + 1);
  return *this;
}

template <typename T>
inline typename r_vector<T>::proxy& r_vector<T>::proxy::operator--(int) {
  operator=(static_cast<T>(*this) - 1);
  return *this;
}

template <typename T>
inline void r_vector<T>::proxy::operator--() {
  operator=(static_cast<T>(*this) - 1);
}

template <typename T>
inline void r_vector<T>::proxy::operator++() {
  operator=(static_cast<T>(*this) + 1);
}

}  // namespace writable

// TODO: is there a better condition we could use, e.g. assert something true
// rather than three things false?
template <typename C, typename T>
using is_container_but_not_sexp_or_string = typename std::enable_if<
    !std::is_constructible<C, SEXP>::value &&
        !std::is_same<typename std::decay<C>::type, std::string>::value &&
        !std::is_same<typename std::decay<T>::type, std::string>::value,
    typename std::decay<C>::type>::type;

template <typename C, typename T = typename std::decay<C>::type::value_type>
// typename T = typename C::value_type>
is_container_but_not_sexp_or_string<C, T> as_cpp(SEXP from) {
  auto obj = cpp11::r_vector<T>(from);
  return {obj.begin(), obj.end()};
}

// TODO: could we make this generalize outside of std::string?
template <typename C, typename T = C>
using is_vector_of_strings = typename std::enable_if<
    std::is_same<typename std::decay<T>::type, std::string>::value,
    typename std::decay<C>::type>::type;

template <typename C, typename T = typename std::decay<C>::type::value_type>
// typename T = typename C::value_type>
is_vector_of_strings<C, T> as_cpp(SEXP from) {
  auto obj = cpp11::r_vector<cpp11::r_string>(from);
  typename std::decay<C>::type res;
  auto it = obj.begin();
  while (it != obj.end()) {
    r_string s = *it;
    res.emplace_back(static_cast<std::string>(s));
    ++it;
  }
  return res;
}

template <typename T>
bool operator==(const r_vector<T>& lhs, const r_vector<T>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  auto lhs_it = lhs.begin();
  auto rhs_it = rhs.begin();

  auto end = lhs.end();
  while (lhs_it != end) {
    if (!(*lhs_it == *rhs_it)) {
      return false;
    }
    ++lhs_it;
    ++rhs_it;
  }
  return true;
}

template <typename T>
bool operator!=(const r_vector<T>& lhs, const r_vector<T>& rhs) {
  return !(lhs == rhs);
}

}  // namespace cpp11
