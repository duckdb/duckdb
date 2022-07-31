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

#ifndef REQ_SKETCH_HPP_
#define REQ_SKETCH_HPP_

#include "req_common.hpp"
#include "req_compactor.hpp"
#include "quantile_sketch_sorted_view.hpp"

#include <stdexcept>

namespace datasketches {

template<
  typename T,
  typename Comparator = std::less<T>, // strict weak ordering function (see C++ named requirements: Compare)
  typename S = serde<T>, // deprecated, to be removed in the next major version
  typename Allocator = std::allocator<T>
>
class req_sketch {
public:
  using value_type = T;
  using comparator = Comparator;
  using Compactor = req_compactor<T, Comparator, Allocator>;
  using AllocCompactor = typename std::allocator_traits<Allocator>::template rebind_alloc<Compactor>;
  using vector_double = std::vector<double, typename std::allocator_traits<Allocator>::template rebind_alloc<double>>;

  /**
   * Constructor
   * @param k Controls the size and error of the sketch. It must be even and in the range [4, 1024], inclusive.
   * Value of 12 roughly corresponds to 1% relative error guarantee at 95% confidence.
   * @param hra if true, the default, the high ranks are prioritized for better
   * accuracy. Otherwise the low ranks are prioritized for better accuracy.
   * @param allocator to use by this instance
   */
  explicit req_sketch(uint16_t k, bool hra = true, const Allocator& allocator = Allocator());

  ~req_sketch();
  req_sketch(const req_sketch& other);
  req_sketch(req_sketch&& other) noexcept;
  req_sketch& operator=(const req_sketch& other);
  req_sketch& operator=(req_sketch&& other);

  /*
   * Type converting constructor.
   * @param other sketch of a different type
   * @param allocator instance of an Allocator
   */
  template<typename TT, typename CC, typename SS, typename AA>
  explicit req_sketch(const req_sketch<TT, CC, SS, AA>& other, const Allocator& allocator = Allocator());

  /**
   * Returns configured parameter K
   * @return parameter K
   */
  uint16_t get_k() const;

  /**
   * Returns configured parameter High Rank Accuracy
   * @return parameter HRA
   */
  bool is_HRA() const;

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  bool is_empty() const;

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  uint64_t get_n() const;

  /**
   * Returns the number of retained items in the sketch.
   * @return number of retained items
   */
  uint32_t get_num_retained() const;

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  bool is_estimation_mode() const;

  template<typename FwdT>
  void update(FwdT&& item);

  template<typename FwdSk>
  void merge(FwdSk&& other);

  /**
   * Returns the min value of the stream.
   * For floating point types: if the sketch is empty this returns NaN.
   * For other types: if the sketch is empty this throws runtime_error.
   * @return the min value of the stream
   */
  const T& get_min_value() const;

  /**
   * Returns the max value of the stream.
   * For floating point types: if the sketch is empty this returns NaN.
   * For other types: if the sketch is empty this throws runtime_error.
   * @return the max value of the stream
   */
  const T& get_max_value() const;

  /**
   * Returns an instance of the comparator for this sketch.
   * @return comparator
   */
  Comparator get_comparator() const;

  /**
   * Returns an approximation to the normalized (fractional) rank of the given item from 0 to 1 inclusive.
   * With the template parameter inclusive=true the weight of the given item is included into the rank.
   * Otherwise the rank equals the sum of the weights of items less than the given item according to the Comparator.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param item to be ranked
   * @return an approximate rank of the given item
   */
  template<bool inclusive = false>
  double get_rank(const T& item) const;

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of split points (values).
   *
   * <p>If the sketch is empty this returns an empty vector.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing values
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals.
   * If the template parameter inclusive=false, the definition of an "interval" is inclusive of the left split point and exclusive of the right
   * split point, with the exception that the last interval will include the maximum value.
   * If the template parameter inclusive=true, the definition of an "interval" is exclusive of the left split point and inclusive of the right
   * split point.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values (the mass) that fall into one of those intervals.
   * If the template parameter inclusive=false, the definition of an "interval" is inclusive of the left split point and exclusive of the right
   * split point, with the exception that the last interval will include the maximum value.
   * If the template parameter inclusive=true, the definition of an "interval" is exclusive of the left split point and inclusive of the right
   * split point.
   */
  template<bool inclusive = false>
  vector_double get_PMF(const T* split_points, uint32_t size) const;

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of split points (values).
   *
   * <p>If the sketch is empty this returns an empty vector.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing float values
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals.
   * If the template parameter inclusive=false, the definition of an "interval" is inclusive of the left split point and exclusive of the right
   * split point, with the exception that the last interval will include the maximum value.
   * If the template parameter inclusive=true, the definition of an "interval" is exclusive of the left split point and inclusive of the right
   * split point.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the split_points. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   */
  template<bool inclusive = false>
  vector_double get_CDF(const T* split_points, uint32_t size) const;

  /**
   * Returns an approximate quantile of the given normalized rank.
   * The normalized rank must be in the range [0.0, 1.0] (both inclusive).
   * @param rank the given normalized rank
   * @return approximate quantile given the normalized rank
   */
  using quantile_return_type = typename quantile_sketch_sorted_view<T, Comparator, Allocator>::quantile_return_type;
  template<bool inclusive = false>
  quantile_return_type get_quantile(double rank) const;

  /**
   * Returns an array of quantiles that correspond to the given array of normalized ranks.
   * @param ranks given array of normalized ranks.
   * @return array of quantiles that correspond to the given array of normalized ranks
   */
  template<bool inclusive = false>
  std::vector<T, Allocator> get_quantiles(const double* ranks, uint32_t size) const;

  /**
   * Returns an approximate lower bound of the given noramalized rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param num_std_dev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate lower bound rank.
   */
  double get_rank_lower_bound(double rank, uint8_t num_std_dev) const;

  /**
   * Returns an approximate upper bound of the given noramalized rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param num_std_dev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate upper bound rank.
   */
  double get_rank_upper_bound(double rank, uint8_t num_std_dev) const;

  /**
   * Returns an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   * Derived from Lemma 12 in https://arxiv.org/abs/2004.01668v2, but the constant factors were
   * modified based on empirical measurements.
   *
   * @param k the given value of k
   * @param rank the given normalized rank, a number in [0,1].
   * @param hra if true High Rank Accuracy mode is being selected, otherwise, Low Rank Accuracy.
   * @param n an estimate of the total number of items submitted to the sketch.
   * @return an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   */
  static double get_RSE(uint16_t k, double rank, bool hra, uint64_t n);

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for fixed-size arithmetic types (integral and floating point).
   * @param instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename TT = T, typename SerDe = S, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for all other types and can be expensive since every item needs to be looked at.
   * @param instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename TT = T, typename SerDe = S, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   * @param instance of a SerDe
   */
  template<typename SerDe = S>
  void serialize(std::ostream& os, const SerDe& sd = SerDe()) const;

  // This is a convenience alias for users
  // The type returned by the following serialize method
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>>;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is a blank space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   * @param instance of a SerDe
   */
  template<typename SerDe = S>
  vector_bytes serialize(unsigned header_size_bytes = 0, const SerDe& sd = SerDe()) const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param instance of an Allocator
   * @return an instance of a sketch
   *
   * Deprecated, to be removed in the next major version
   */
  static req_sketch deserialize(std::istream& is, const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param instance of a SerDe
   * @param instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = S>
  static req_sketch deserialize(std::istream& is, const SerDe& sd = SerDe(), const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param instance of an Allocator
   * @return an instance of a sketch
   *
   * Deprecated, to be removed in the next major version
   */
  static req_sketch deserialize(const void* bytes, size_t size, const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param instance of a SerDe
   * @param instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = S>
  static req_sketch deserialize(const void* bytes, size_t size, const SerDe& sd = SerDe(), const Allocator& allocator = Allocator());

  /**
   * Prints a summary of the sketch.
   * @param print_levels if true include information about levels
   * @param print_items if true include sketch data
   */
  string<Allocator> to_string(bool print_levels = false, bool print_items = false) const;

  class const_iterator;
  const_iterator begin() const;
  const_iterator end() const;

  template<bool inclusive = false>
  quantile_sketch_sorted_view<T, Comparator, Allocator> get_sorted_view(bool cumulative) const;

private:
  Allocator allocator_;
  uint16_t k_;
  bool hra_;
  uint32_t max_nom_size_;
  uint32_t num_retained_;
  uint64_t n_;
  std::vector<Compactor, AllocCompactor> compactors_;
  T* min_value_;
  T* max_value_;

  static const bool LAZY_COMPRESSION = false;

  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t FAMILY = 17;
  static const size_t PREAMBLE_SIZE_BYTES = 8;
  enum flags { RESERVED1, RESERVED2, IS_EMPTY, IS_HIGH_RANK, RAW_ITEMS, IS_LEVEL_ZERO_SORTED };

  static constexpr double FIXED_RSE_FACTOR = 0.084;
  static double relative_rse_factor();

  uint8_t get_num_levels() const;
  void grow();
  void update_max_nom_size();
  void update_num_retained();
  void compress();

  static double get_rank_lb(uint16_t k, uint8_t num_levels, double rank, uint8_t num_std_dev, uint64_t n, bool hra);
  static double get_rank_ub(uint16_t k, uint8_t num_levels, double rank, uint8_t num_std_dev, uint64_t n, bool hra);
  static bool is_exact_rank(uint16_t k, uint8_t num_levels, double rank, uint64_t n, bool hra);

  // for deserialization
  class item_deleter;
  req_sketch(uint16_t k, bool hra, uint64_t n, std::unique_ptr<T, item_deleter> min_value, std::unique_ptr<T, item_deleter> max_value, std::vector<Compactor, AllocCompactor>&& compactors);

  static void check_preamble_ints(uint8_t preamble_ints, uint8_t num_levels);
  static void check_serial_version(uint8_t serial_version);
  static void check_family_id(uint8_t family_id);

  // implementations for floating point types
  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static const TT& get_invalid_value() {
    static TT value = std::numeric_limits<TT>::quiet_NaN();
    return value;
  }

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_value(const TT& value) {
    return !std::isnan(value);
  }

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline void check_split_points(const T* values, uint32_t size) {
    for (uint32_t i = 0; i < size ; i++) {
      if (std::isnan(values[i])) {
        throw std::invalid_argument("Values must not be NaN");
      }
      if ((i < (size - 1)) && !(Comparator()(values[i], values[i + 1]))) {
        throw std::invalid_argument("Values must be unique and monotonically increasing");
      }
    }
  }

  // implementations for all other types
  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static const TT& get_invalid_value() {
    throw std::runtime_error("getting quantiles from empty sketch is not supported for this type of values");
  }

  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_value(const TT&) {
    return true;
  }

  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static inline void check_split_points(const T* values, uint32_t size) {
    for (uint32_t i = 0; i < size ; i++) {
      if ((i < (size - 1)) && !(Comparator()(values[i], values[i + 1]))) {
        throw std::invalid_argument("Values must be unique and monotonically increasing");
      }
    }
  }

  // for type converting constructor
  template<typename TT, typename CC, typename SS, typename AA>
  friend class req_sketch;
};

template<typename T, typename C, typename S, typename A>
class req_sketch<T, C, S, A>::const_iterator: public std::iterator<std::input_iterator_tag, T> {
public:
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  std::pair<const T&, const uint64_t> operator*() const;
private:
  using LevelsIterator = typename std::vector<Compactor, AllocCompactor>::const_iterator;
  LevelsIterator levels_it_;
  LevelsIterator levels_end_;
  const T* compactor_it_;
  friend class req_sketch<T, C, S, A>;
  const_iterator(LevelsIterator begin, LevelsIterator end);
};

} /* namespace datasketches */

#include "req_sketch_impl.hpp"

#endif
