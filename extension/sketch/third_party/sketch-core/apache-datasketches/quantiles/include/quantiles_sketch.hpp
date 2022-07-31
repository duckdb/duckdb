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

#ifndef _QUANTILES_SKETCH_HPP_
#define _QUANTILES_SKETCH_HPP_

#include <functional>
#include <memory>
#include <vector>

#include "quantile_sketch_sorted_view.hpp"
#include "common_defs.hpp"
#include "serde.hpp"

namespace datasketches {

/**
 * This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of real values from a very large stream in a single pass.
 * The analysis is obtained using a getQuantiles(*) function or its inverse functions the
 * Probability Mass Function from getPMF(*) and the Cumulative Distribution Function from getCDF(*).
 *
 * <p>Consider a large stream of one million values such as packet sizes coming into a network node.
 * The absolute rank of any specific size value is simply its index in the hypothetical sorted
 * array of values.
 * The normalized rank (or fractional rank) is the absolute rank divided by the stream size,
 * in this case one million.
 * The value corresponding to the normalized rank of 0.5 represents the 50th percentile or median
 * value of the distribution, or getQuantile(0.5).  Similarly, the 95th percentile is obtained from
 * getQuantile(0.95). Using the getQuantiles(0.0, 1.0) will return the min and max values seen by
 * the sketch.</p>
 *
 * <p>From the min and max values, for example, 1 and 1000 bytes,
 * you can obtain the PMF from getPMF(100, 500, 900) that will result in an array of
 * 4 fractional values such as {.4, .3, .2, .1}, which means that
 * <ul>
 * <li>40% of the values were &lt; 100,</li>
 * <li>30% of the values were &ge; 100 and &lt; 500,</li>
 * <li>20% of the values were &ge; 500 and &lt; 900, and</li>
 * <li>10% of the values were &ge; 900.</li>
 * </ul>
 * A frequency histogram can be obtained by simply multiplying these fractions by getN(),
 * which is the total count of values received.
 * The getCDF(*) works similarly, but produces the cumulative distribution instead.
 *
 * <p>As of November 2021, this implementation produces serialized sketches which are binary-compatible
 * with the equivalent Java implementation only when template parameter T = double
 * (64-bit double precision values).

 * 
 * <p>The accuracy of this sketch is a function of the configured value <i>k</i>, which also affects
 * the overall size of the sketch. Accuracy of this quantile sketch is always with respect to
 * the normalized rank.  A <i>k</i> of 128 produces a normalized, rank error of about 1.7%.
 * For example, the median value returned from getQuantile(0.5) will be between the actual values
 * from the hypothetically sorted array of input values at normalized ranks of 0.483 and 0.517, with
 * a confidence of about 99%.</p>
 *
 * <pre>
Table Guide for DoublesSketch Size in Bytes and Approximate Error:
          K =&gt; |      16      32      64     128     256     512   1,024
    ~ Error =&gt; | 12.145%  6.359%  3.317%  1.725%  0.894%  0.463%  0.239%
             N | Size in Bytes -&gt;
------------------------------------------------------------------------
             0 |       8       8       8       8       8       8       8
             1 |      72      72      72      72      72      72      72
             3 |      72      72      72      72      72      72      72
             7 |     104     104     104     104     104     104     104
            15 |     168     168     168     168     168     168     168
            31 |     296     296     296     296     296     296     296
            63 |     424     552     552     552     552     552     552
           127 |     552     808   1,064   1,064   1,064   1,064   1,064
           255 |     680   1,064   1,576   2,088   2,088   2,088   2,088
           511 |     808   1,320   2,088   3,112   4,136   4,136   4,136
         1,023 |     936   1,576   2,600   4,136   6,184   8,232   8,232
         2,047 |   1,064   1,832   3,112   5,160   8,232  12,328  16,424
         4,095 |   1,192   2,088   3,624   6,184  10,280  16,424  24,616
         8,191 |   1,320   2,344   4,136   7,208  12,328  20,520  32,808
        16,383 |   1,448   2,600   4,648   8,232  14,376  24,616  41,000
        32,767 |   1,576   2,856   5,160   9,256  16,424  28,712  49,192
        65,535 |   1,704   3,112   5,672  10,280  18,472  32,808  57,384
       131,071 |   1,832   3,368   6,184  11,304  20,520  36,904  65,576
       262,143 |   1,960   3,624   6,696  12,328  22,568  41,000  73,768
       524,287 |   2,088   3,880   7,208  13,352  24,616  45,096  81,960
     1,048,575 |   2,216   4,136   7,720  14,376  26,664  49,192  90,152
     2,097,151 |   2,344   4,392   8,232  15,400  28,712  53,288  98,344
     4,194,303 |   2,472   4,648   8,744  16,424  30,760  57,384 106,536
     8,388,607 |   2,600   4,904   9,256  17,448  32,808  61,480 114,728
    16,777,215 |   2,728   5,160   9,768  18,472  34,856  65,576 122,920
    33,554,431 |   2,856   5,416  10,280  19,496  36,904  69,672 131,112
    67,108,863 |   2,984   5,672  10,792  20,520  38,952  73,768 139,304
   134,217,727 |   3,112   5,928  11,304  21,544  41,000  77,864 147,496
   268,435,455 |   3,240   6,184  11,816  22,568  43,048  81,960 155,688
   536,870,911 |   3,368   6,440  12,328  23,592  45,096  86,056 163,880
 1,073,741,823 |   3,496   6,696  12,840  24,616  47,144  90,152 172,072
 2,147,483,647 |   3,624   6,952  13,352  25,640  49,192  94,248 180,264
 4,294,967,295 |   3,752   7,208  13,864  26,664  51,240  98,344 188,456

 * </pre>

 * <p>There is more documentation available on
 * <a href="https://datasketches.apache.org">datasketches.apache.org</a>.</p>
 *
 * <p>This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch
 * described in section 3.2 of the journal version of the paper "Mergeable Summaries"
 * by Agarwal, Cormode, Huang, Phillips, Wei, and Yi.
 * <a href="http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13"></a></p>
 *
 * <p>This algorithm is independent of the distribution of values and
 * requires only that the values be comparable.</p
 *
 * <p>This algorithm intentionally inserts randomness into the sampling process for values that
 * ultimately get retained in the sketch. The results produced by this algorithm are not
 * deterministic. For example, if the same stream is inserted into two different instances of this
 * sketch, the answers obtained from the two sketches may not be identical.</p>
 *
 * <p>Similarly, there may be directional inconsistencies. For example, the resulting array of
 * values obtained from getQuantiles(fractions[]) input into the reverse directional query
 * getPMF(splitPoints[]) may not result in the original fractional values.</p>
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 * @author Alexander Saydakov
 * @author Jon Malkin
 */

namespace quantiles_constants {
  const uint16_t DEFAULT_K = 128;
  const uint16_t MIN_K = 2;
  const uint16_t MAX_K = 1 << 15;
}

template <typename T,
          typename Comparator = std::less<T>, // strict weak ordering function (see C++ named requirements: Compare)
          typename Allocator = std::allocator<T>>
class quantiles_sketch {
public:
  using value_type = T;
  using allocator_type = Allocator;
  using comparator = Comparator;
  using vector_double = std::vector<double, typename std::allocator_traits<Allocator>::template rebind_alloc<double>>;

  explicit quantiles_sketch(uint16_t k = quantiles_constants::DEFAULT_K, const Allocator& allocator = Allocator());
  quantiles_sketch(const quantiles_sketch& other);
  quantiles_sketch(quantiles_sketch&& other) noexcept;
  ~quantiles_sketch();
  quantiles_sketch& operator=(const quantiles_sketch& other);
  quantiles_sketch& operator=(quantiles_sketch&& other) noexcept;

  /**
   * @brief Type converting constructor
   * @param other quantiles sketch of a different type
   * @param allocator instance of an Allocator
   */
  template<typename From, typename FC, typename FA>
  explicit quantiles_sketch(const quantiles_sketch<From, FC, FA>& other, const Allocator& allocator = Allocator());

  /**
   * Updates this sketch with the given data item.
   * @param value an item from a stream of items
   */
  template<typename FwdT>
  void update(FwdT&& value);

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  template<typename FwdSk>
  void merge(FwdSk&& other);

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  bool is_empty() const;

  /**
   * Returns configured parameter k
   * @return parameter k
   */
  uint16_t get_k() const;

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  uint64_t get_n() const;

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items
   */
  uint32_t get_num_retained() const;

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  bool is_estimation_mode() const;

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
   * Returns the allocator for this sketch.
   * @return allocator
   */
  allocator_type get_allocator() const;

  /**
   * Returns an approximation to the value of the data item
   * that would be preceded by the given fraction of a hypothetical sorted
   * version of the input stream so far.
   * <p>
   * Note that this method has a fairly large overhead (microseconds instead of nanoseconds)
   * so it should not be called multiple times to get different quantiles from the same
   * sketch. Instead use get_quantiles(), which pays the overhead only once.
   * <p>
   * For floating point types: if the sketch is empty this returns NaN.
   * For other types: if the sketch is empty this throws runtime_error.
   *
   * @param rank the specified fractional position in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * If rank = 0.0, the true minimum value of the stream is returned.
   * If rank = 1.0, the true maximum value of the stream is returned.
   *
   * @return the approximation to the value at the given rank
   */
  using quantile_return_type = typename quantile_sketch_sorted_view<T, Comparator, Allocator>::quantile_return_type;
  template<bool inclusive = false>
  quantile_return_type get_quantile(double rank) const;

  /**
   * This is a more efficient multiple-query version of get_quantile().
   * <p>
   * This returns an array that could have been generated by using get_quantile() for each
   * fractional rank separately, but would be very inefficient.
   * This method incurs the internal set-up overhead once and obtains multiple quantile values in
   * a single query. It is strongly recommend that this method be used instead of multiple calls
   * to get_quantile().
   *
   * <p>If the sketch is empty this returns an empty vector.
   *
   * @param fractions given array of fractional positions in the hypothetical sorted stream.
   * These are also called normalized ranks or fractional ranks.
   * These fractions must be in the interval [0.0, 1.0], inclusive.
   *
   * @return array of approximations to the given fractions in the same order as given fractions
   * in the input array.
   */
  template<bool inclusive = false>
  std::vector<T, Allocator> get_quantiles(const double* fractions, uint32_t size) const;

  /**
   * This is a multiple-query version of get_quantile() that allows the caller to
   * specify the number of evenly-spaced fractional ranks.
   *
   * <p>If the sketch is empty this returns an empty vector.
   *
   * @param num an integer that specifies the number of evenly-spaced fractional ranks.
   * This must be an integer greater than 0. A value of 1 will return the min value.
   * A value of 2 will return the min and the max value. A value of 3 will return the min,
   * the median and the max value, etc.
   *
   * @return array of approximations to the given number of evenly-spaced fractional ranks.
   */
  template<bool inclusive = false>
  std::vector<T, Allocator> get_quantiles(uint32_t num) const;

  /**
   * Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1,
   * inclusive. When template parameter <em>inclusive=false</em> (the default), only elements strictly
   * less than the provided value are included in the rank estimate. With <em>inclusive=true</em>,
   * the rank estimate includes elements less than or equal to the provided value.
   *
   * <p>The resulting approximation has a probabilistic guarantee that can be obtained from the
   * get_normalized_rank_error(false) function.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param value to be ranked
   * @return an approximate rank of the given value
   */
  template<bool inclusive = false>
  double get_rank(const T& value) const;

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of split points (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * get_normalized_rank_error(true) function.
   *
   * <p>If the sketch is empty this returns an empty vector.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing values
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals.
   * If the template parameter <em>inclusive=false</em> (the default), the definition of an "interval"
   * is inclusive of the left split point and exclusive of the right
   * split point, with the exception that the last interval will include the maximum value.
   * If the template parameter <em>inclusive=true</em>, the definition of an "interval" is exclusive of
   * the left split point and inclusive of the right split point.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream values (the mass) that fall into one of those intervals.
   * When <em>inclusive=false</em> (the default), the definition of an "interval" is inclusive
   * of the left split point and exclusive of the right split point, with the exception that the last
   * interval will include the maximum value. When <em>inclusive=true</em>,
   * an "interval" is exclusive of the left split point and inclusive of the right.
   */
  template<bool inclusive = false>
  vector_double get_PMF(const T* split_points, uint32_t size) const;

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of split points (values).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * get_normalized_rank_error(false) function.
   *
   * <p>If the sketch is empty this returns an empty vector.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing values
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals.
   * If the template parameter <em>inclusive=false</em> (the default), the definition of an "interval" is
   * inclusive of the left split point and exclusive of the right
   * split point, with the exception that the last interval will include the maximum value.
   * If the template parameter <em>inclusive=true</em>, the definition of an "interval" is exclusive of
   * the left split point and inclusive of the right split point.
   * It is not necessary to include either the min or max values in these split points.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the split_points. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array.
   * When <em>inclusive=false</em> (the default), the definition of an "interval" is inclusive
   * of the left split point and exclusive of the right split point, with the exception that the last
   * interval will include the maximum value. When <em>inclusive=true</em>,
   * an "interval" is exclusive of the left split point and inclusive of the right.

   */
  template<bool inclusive = false>
  vector_double get_CDF(const T* split_points, uint32_t size) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for fixed-size arithmetic types (integral and floating point).
   * @param instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename SerDe = serde<T>, typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& serde = SerDe()) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for all other types and can be expensive since every item needs to be looked at.
   * @param instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename SerDe = serde<T>, typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& serde = SerDe()) const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   * @param instance of a SerDe
   */
  template<typename SerDe = serde<T>>
  void serialize(std::ostream& os, const SerDe& serde = SerDe()) const;

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
   * @return serialized sketch as a vector of bytes
   */
  template<typename SerDe = serde<T>>
  vector_bytes serialize(unsigned header_size_bytes = 0, const SerDe& serde = SerDe()) const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param instance of a SerDe
   * @param instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = serde<T>>
  static quantiles_sketch deserialize(std::istream& is, const SerDe& serde = SerDe(), const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param instance of a SerDe
   * @param instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = serde<T>>
  static quantiles_sketch deserialize(const void* bytes, size_t size, const SerDe& serde = SerDe(), const Allocator& allocator = Allocator());

  /**
   * Gets the normalized rank error for this sketch. Constants were derived as the best fit to 99 percentile
   * empirically measured max error in thousands of trials.
   * @param is_pmf if true, returns the "double-sided" normalized rank error for the get_PMF() function.
   *               Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return the normalized rank error for the sketch
   */
  double get_normalized_rank_error(bool is_pmf) const;

  /**
   * Gets the normalized rank error given k and pmf. Constants were derived as the best fit to 99 percentile
   * empirically measured max error in thousands of trials.
   * @param k  the configuration parameter
   * @param is_pmf if true, returns the "double-sided" normalized rank error for the get_PMF() function.
   *               Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return the normalized rank error for the given parameters
   */
  static double get_normalized_rank_error(uint16_t k, bool is_pmf);

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
  using Level = std::vector<T, Allocator>;
  using VectorLevels = std::vector<Level, typename std::allocator_traits<Allocator>::template rebind_alloc<Level>>;

  /* Serialized sketch layout:
   * Long || Start Byte Addr:
   * Addr:
   *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
   *  0   || Preamble_Longs | SerVer | FamID  |  Flags |----- K ---------|---- unused -----|
   *
   *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
   *  1   ||---------------------------Items Seen Count (N)--------------------------------|
   *
   * Long 3 is the start of data, beginning with serialized min and max values, followed by
   * the sketch data buffers.
   */

  static const size_t EMPTY_SIZE_BYTES = 8;
  static const uint8_t SERIAL_VERSION_1 = 1;
  static const uint8_t SERIAL_VERSION_2 = 2;
  static const uint8_t SERIAL_VERSION = 3;
  static const uint8_t FAMILY = 8;

  enum flags { RESERVED0, RESERVED1, IS_EMPTY, IS_COMPACT, IS_SORTED };

  static const uint8_t PREAMBLE_LONGS_SHORT = 1; // for empty
  static const uint8_t PREAMBLE_LONGS_FULL = 2;
  static const size_t DATA_START = 16;

  Allocator allocator_;
  uint16_t k_;
  uint64_t n_;
  uint64_t bit_pattern_;
  Level base_buffer_;
  VectorLevels levels_;
  T* min_value_;
  T* max_value_;
  bool is_sorted_;

  // for deserialization
  class item_deleter;
  class items_deleter;
  quantiles_sketch(uint16_t k, uint64_t n, uint64_t bit_pattern,
      Level&& base_buffer, VectorLevels&& levels,
      std::unique_ptr<T, item_deleter> min_value, std::unique_ptr<T, item_deleter> max_value,
      bool is_sorted, const Allocator& allocator = Allocator());

  void grow_base_buffer();
  void process_full_base_buffer();

  // returns true if size adjusted, else false
  bool grow_levels_if_needed();

  // buffers should be pre-sized to target capacity as appropriate
  template<typename FwdV>
  static void in_place_propagate_carry(uint8_t starting_level, FwdV&& buf_size_k,
                                       Level& buf_size_2k, bool apply_as_update,
                                       quantiles_sketch& sketch);
  static void zip_buffer(Level& buf_in, Level& buf_out);
  static void merge_two_size_k_buffers(Level& arr_in_1, Level& arr_in_2, Level& arr_out);

  template<typename SerDe>
  static Level deserialize_array(std::istream& is, uint32_t num_items, uint32_t capcacity, const SerDe& serde, const Allocator& allocator);
  
  template<typename SerDe>
  static std::pair<Level, size_t> deserialize_array(const void* bytes, size_t size, uint32_t num_items, uint32_t capcacity, const SerDe& serde, const Allocator& allocator);

  static void check_k(uint16_t k);
  static void check_serial_version(uint8_t serial_version);
  static void check_header_validity(uint8_t preamble_longs, uint8_t flags_byte, uint8_t serial_version);
  static void check_family_id(uint8_t family_id);

  static uint32_t compute_retained_items(uint16_t k, uint64_t n);
  static uint32_t compute_base_buffer_items(uint16_t k, uint64_t n);
  static uint64_t compute_bit_pattern(uint16_t k, uint64_t n);
  static uint32_t compute_valid_levels(uint64_t bit_pattern);
  static uint8_t compute_levels_needed(uint16_t k, uint64_t n);

 /**
  * Merges the src sketch into the tgt sketch with equal values of K.
  * src is modified only if elements can be moved out of it.
  */
  template<typename FwdSk>
  static void standard_merge(quantiles_sketch& tgt, FwdSk&& src);

 /**
  * Merges the src sketch into the tgt sketch with a smaller value of K.
  * However, it is required that the ratio of the two K values be a power of 2.
  * I.e., other.get_k() = this.get_k() * 2^(nonnegative integer).
  * src is modified only if elements can be moved out of it.
  */
  template<typename FwdSk>
  static void downsampling_merge(quantiles_sketch& tgt, FwdSk&& src);

  template<typename FwdV>
  static void zip_buffer_with_stride(FwdV&& buf_in, Level& buf_out, uint16_t stride);

  /**
   * Returns the zero-based bit position of the lowest zero bit of <i>bits</i> starting at
   * <i>startingBit</i>. If input is all ones, this returns 64.
   * @param bits the input bits as a long
   * @param starting_bit the zero-based starting bit position. Only the low 6 bits are used.
   * @return the zero-based bit position of the lowest zero bit starting at <i>startingBit</i>.
   */
  static uint8_t lowest_zero_bit_starting_at(uint64_t bits, uint8_t starting_bit);

  // implementations for floating point types
  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static const TT& get_invalid_value() {
    static TT value = std::numeric_limits<TT>::quiet_NaN();
    return value;
  }

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_value(TT value) {
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
  static inline bool check_update_value(TT) {
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
};


template<typename T, typename C, typename A>
class quantiles_sketch<T, C, A>::const_iterator: public std::iterator<std::input_iterator_tag, T> {
public:
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  std::pair<const T&, const uint64_t> operator*() const;
private:
  friend class quantiles_sketch<T, C, A>;
  using Level = std::vector<T, A>;
  using AllocLevel = typename std::allocator_traits<A>::template rebind_alloc<Level>;
  Level base_buffer_;
  std::vector<Level, AllocLevel> levels_;
  int level_;
  uint32_t index_;
  uint32_t bb_count_;
  uint64_t bit_pattern_;
  uint64_t weight_;
  uint16_t k_;
  const_iterator(const Level& base_buffer, const std::vector<Level, AllocLevel>& levels, uint16_t k, uint64_t n, bool is_end);
};

} /* namespace datasketches */

#include "quantiles_sketch_impl.hpp"

#endif // _QUANTILES_SKETCH_HPP_
