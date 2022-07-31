/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef QUANTILE_SKETCH_SORTED_VIEW_HPP_
#define QUANTILE_SKETCH_SORTED_VIEW_HPP_

#include <functional>

namespace datasketches {

template<
  typename T,
  typename Comparator, // strict weak ordering function (see C++ named requirements: Compare)
  typename Allocator
>
class quantile_sketch_sorted_view {
public:
  using Entry = typename std::conditional<std::is_arithmetic<T>::value, std::pair<T, uint64_t>, std::pair<const T*, uint64_t>>::type;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;
  using Container = std::vector<Entry, AllocEntry>;

  quantile_sketch_sorted_view(uint32_t num, const Allocator& allocator);

  template<typename Iterator>
  void add(Iterator begin, Iterator end, uint64_t weight);

  template<bool inclusive>
  void convert_to_cummulative();

  class const_iterator;
  const_iterator begin() const;
  const_iterator end() const;

  size_t size() const;

  // makes sense only with cumulative weight
  using quantile_return_type = typename std::conditional<std::is_arithmetic<T>::value, T, const T&>::type;
  quantile_return_type get_quantile(double rank) const;

private:
  static inline const T& deref_helper(const T* t) { return *t; }
  static inline T deref_helper(T t) { return t; }

  struct compare_pairs_by_first {
    bool operator()(const Entry& a, const Entry& b) const {
      return Comparator()(deref_helper(a.first), deref_helper(b.first));
    }
  };

  struct compare_pairs_by_second {
    bool operator()(const Entry& a, const Entry& b) const {
      return a.second < b.second;
    }
  };

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  static inline T ref_helper(const T& t) { return t; }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  static inline const T* ref_helper(const T& t) { return std::addressof(t); }

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  static inline Entry make_dummy_entry(uint64_t weight) { return Entry(0, weight); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  static inline Entry make_dummy_entry(uint64_t weight) { return Entry(nullptr, weight); }

  uint64_t total_weight_;
  Container entries_;
};

template<typename T, typename C, typename A>
class quantile_sketch_sorted_view<T, C, A>::const_iterator: public quantile_sketch_sorted_view<T, C, A>::Container::const_iterator {
public:
  using Base = typename quantile_sketch_sorted_view<T, C, A>::Container::const_iterator;
  using value_type = typename std::conditional<std::is_arithmetic<T>::value, typename Base::value_type, std::pair<const T&, const uint64_t>>::type;

  const_iterator(const Base& it): Base(it) {}

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  value_type operator*() const { return Base::operator*(); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  value_type operator*() const { return value_type(*(Base::operator*().first), Base::operator*().second); }

  class return_value_holder {
  public:
    return_value_holder(value_type value): value_(value) {}
    const value_type* operator->() const { return &value_; }
  private:
    value_type value_;
  };

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  const value_type* operator->() const { return Base::operator->(); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  return_value_holder operator->() const { return **this; }
};

} /* namespace datasketches */

#include "quantile_sketch_sorted_view_impl.hpp"

#endif
