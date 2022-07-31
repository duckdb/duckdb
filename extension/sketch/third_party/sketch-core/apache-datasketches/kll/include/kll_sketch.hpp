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

#ifndef KLL_SKETCH_HPP_
#define KLL_SKETCH_HPP_

#include <functional>
#include <memory>
#include <vector>
#include <cmath>

#include "quantile_sketch_sorted_view.hpp"
#include "common_defs.hpp"
#include "serde.hpp"

namespace datasketches {

/*
 * Implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per retained item.
 * See <a href="https://arxiv.org/abs/1603.05346v2">Optimal Quantile Approximation in Streams</a>.
 *
 * <p>This is a stochastic streaming sketch that enables near real-time analysis of the
 * approximate distribution of values from a very large stream in a single pass, requiring only
 * that the values are comparable.
 * The analysis is obtained using <i>get_quantile()</i> or <i>get_quantiles()</i> functions or the
 * inverse functions get_rank(), get_PMF() (Probability Mass Function), and get_CDF()
 * (Cumulative Distribution Function).
 *
 * <p>As of May 2020, this implementation produces serialized sketches which are binary-compatible
 * with the equivalent Java implementation only when template parameter T = float
 * (32-bit single precision values).
 * 
 * <p>Given an input stream of <i>N</i> numeric values, the <i>absolute rank</i> of any specific
 * value is defined as its index <i>(0 to N-1)</i> in the hypothetical sorted stream of all
 * <i>N</i> input values.
 *
 * <p>The <i>normalized rank</i> (<i>rank</i>) of any specific value is defined as its
 * <i>absolute rank</i> divided by <i>N</i>.
 * Thus, the <i>normalized rank</i> is a value between zero and one.
 * In the documentation for this sketch <i>absolute rank</i> is never used so any
 * reference to just <i>rank</i> should be interpreted to mean <i>normalized rank</i>.
 *
 * <p>This sketch is configured with a parameter <i>k</i>, which affects the size of the sketch
 * and its estimation error.
 *
 * <p>The estimation error is commonly called <i>epsilon</i> (or <i>eps</i>) and is a fraction
 * between zero and one. Larger values of <i>k</i> result in smaller values of epsilon.
 * Epsilon is always with respect to the rank and cannot be applied to the
 * corresponding values.
 *
 * <p>The relationship between the normalized rank and the corresponding values can be viewed
 * as a two dimensional monotonic plot with the normalized rank on one axis and the
 * corresponding values on the other axis. If the y-axis is specified as the value-axis and
 * the x-axis as the normalized rank, then <i>y = get_quantile(x)</i> is a monotonically
 * increasing function.
 *
 * <p>The functions <i>get_quantile(rank)</i> and get_quantiles(...) translate ranks into
 * corresponding values. The functions <i>get_rank(value),
 * get_CDF(...) (Cumulative Distribution Function), and get_PMF(...)
 * (Probability Mass Function)</i> perform the opposite operation and translate values into ranks.
 *
 * <p>The <i>getPMF(...)</i> function has about 13 to 47% worse rank error (depending
 * on <i>k</i>) than the other queries because the mass of each "bin" of the PMF has
 * "double-sided" error from the upper and lower edges of the bin as a result of a subtraction,
 * as the errors from the two edges can sometimes add.
 *
 * <p>The default <i>k</i> of 200 yields a "single-sided" epsilon of about 1.33% and a
 * "double-sided" (PMF) epsilon of about 1.65%.
 *
 * <p>A <i>get_quantile(rank)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>v = get_quantile(r)</i> where <i>r</i> is the rank between zero and one.</li>
 * <li>The value <i>v</i> will be a value from the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%. Note that the
 * error is on the rank, not the value.</li>
 * </ul>
 *
 * <p>A <i>get_rank(value)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>r = get_rank(v)</i> where <i>v</i> is a value between the min and max values of
 * the input stream.</li>
 * <li>Let <i>true_rank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%.</li>
 * </ul>
 *
 * <p>A <i>get_PMF()</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>{r1, r2, ..., r(m+1)} = get_PMF(v1, v2, ..., vm)</i> where <i>v1, v2</i> are values
 * between the min and max values of the input stream.
 * <li>Let <i>mass<sub>i</sub> = estimated mass between v<sub>i</sub> and v<sub>i+1</sub></i>.</li>
 * <li>Let <i>trueMass</i> be the true mass between the values of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(true)</i>.</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li>r(m+1) includes the mass of all points larger than vm.</li>
 * </ul>
 *
 * <p>A <i>get_CDF(...)</i> query has the following guarantees;
 * <ul>
 * <li>Let <i>{r1, r2, ..., r(m+1)} = get_CDF(v1, v2, ..., vm)</i> where <i>v1, v2</i> are values
 * between the min and max values of the input stream.
 * <li>Let <i>mass<sub>i</sub> = r<sub>i+1</sub> - r<sub>i</sub></i>.</li>
 * <li>Let <i>trueMass</i> be the true mass between the true ranks of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(true)</i>.</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li>1 - r(m+1) includes the mass of all points larger than vm.</li>
 * </ul>
 *
 * <p>From the above, it might seem like we could make some estimates to bound the
 * <em>value</em> returned from a call to <em>get_quantile()</em>. The sketch, however, does not
 * let us derive error bounds or confidences around values. Because errors are independent, we
 * can approximately bracket a value as shown below, but there are no error estimates available.
 * Additionally, the interval may be quite large for certain distributions.
 * <ul>
 * <li>Let <i>v = get_quantile(r)</i>, the estimated quantile value of rank <i>r</i>.</li>
 * <li>Let <i>eps = get_normalized_rank_error(false)</i>.</li>
 * <li>Let <i>v<sub>lo</sub></i> = estimated quantile value of rank <i>(r - eps)</i>.</li>
 * <li>Let <i>v<sub>hi</sub></i> = estimated quantile value of rank <i>(r + eps)</i>.</li>
 * <li>Then <i>v<sub>lo</sub> &le; v &le; v<sub>hi</sub></i>, with 99% confidence.</li>
 * </ul>
 *
 * author Kevin Lang
 * author Alexander Saydakov
 * author Lee Rhodes
 */

template<typename A> using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
template<typename A> using vector_u8 = std::vector<uint8_t, AllocU8<A>>;
template<typename A> using AllocU32 = typename std::allocator_traits<A>::template rebind_alloc<uint32_t>;
template<typename A> using vector_u32 = std::vector<uint32_t, AllocU32<A>>;
template<typename A> using AllocD = typename std::allocator_traits<A>::template rebind_alloc<double>;
template<typename A> using vector_d = std::vector<double, AllocD<A>>;

namespace kll_constants {
  const uint16_t DEFAULT_K = 200;
}

template <
  typename T,
  typename C = std::less<T>, // strict weak ordering function (see C++ named requirements: Compare)
  typename S = serde<T>, // deprecated, to be removed in the next major version
  typename A = std::allocator<T>
>
class kll_sketch {
  public:
    using value_type = T;
    using comparator = C;

    static const uint8_t DEFAULT_M = 8;
    // TODO: Redundant and deprecated. Will be removed in next major version.
    static const uint16_t DEFAULT_K = kll_constants::DEFAULT_K;
    static const uint16_t MIN_K = DEFAULT_M;
    static const uint16_t MAX_K = (1 << 16) - 1;

    explicit kll_sketch(uint16_t k = kll_constants::DEFAULT_K, const A& allocator = A());
    kll_sketch(const kll_sketch& other);
    kll_sketch(kll_sketch&& other) noexcept;
    ~kll_sketch();
    kll_sketch& operator=(const kll_sketch& other);
    kll_sketch& operator=(kll_sketch&& other);

    /*
     * Type converting constructor.
     * @param other sketch of a different type
     * @param allocator instance of an Allocator
     */
    template<typename TT, typename CC, typename SS, typename AA>
    explicit kll_sketch(const kll_sketch<TT, CC, SS, AA>& other, const A& allocator = A());

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
    T get_min_value() const;

    /**
     * Returns the max value of the stream.
     * For floating point types: if the sketch is empty this returns NaN.
     * For other types: if the sketch is empty this throws runtime_error.
     * @return the max value of the stream
     */
    T get_max_value() const;

    /**
     * Returns an instance of the comparator for this sketch.
     * @return comparator
     */
    C get_comparator() const;

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
     * @param fraction the specified fractional position in the hypothetical sorted stream.
     * These are also called normalized ranks or fractional ranks.
     * If fraction = 0.0, the true minimum value of the stream is returned.
     * If fraction = 1.0, the true maximum value of the stream is returned.
     * If the parameter inclusive=true, the given rank is considered inclusive (includes the weight of an item)
     *
     * @return the approximation to the value at the given fraction
     */
    using quantile_return_type = typename quantile_sketch_sorted_view<T, C, A>::quantile_return_type;
    template<bool inclusive = false>
    quantile_return_type get_quantile(double fraction) const;

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
     * If the parameter inclusive=true, the given fractions are considered inclusive (include weights of items)
     *
     * @return array of approximations to the given fractions in the same order as given fractions
     * in the input array.
     */
    template<bool inclusive = false>
    std::vector<T, A> get_quantiles(const double* fractions, uint32_t size) const;

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
    std::vector<T, A> get_quantiles(uint32_t num) const;

    /**
     * Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1,
     * inclusive.
     * With the template parameter inclusive=true the weight of the given value is included into the rank.
     * Otherwise the rank equals the sum of the weights of all values that are less than the given value
     * according to the comparator C.
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
     * The definition of an "interval" is inclusive of the left split point (or minimum value) and
     * exclusive of the right split point, with the exception that the last interval will include
     * the maximum value.
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
    vector_d<A> get_PMF(const T* split_points, uint32_t size) const;

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
     * The definition of an "interval" is inclusive of the left split point (or minimum value) and
     * exclusive of the right split point, with the exception that the last interval will include
     * the maximum value.
     * It is not necessary to include either the min or max values in these split points.
     *
     * @return an array of m+1 double values, which are a consecutive approximation to the CDF
     * of the input stream given the split_points. The value at array position j of the returned
     * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
     * array.
     */
    template<bool inclusive = false>
    vector_d<A> get_CDF(const T* split_points, uint32_t size) const;

    /**
     * Gets the approximate rank error of this sketch normalized as a fraction between zero and one.
     * @param pmf if true, returns the "double-sided" normalized rank error for the get_PMF() function.
     * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
     * @return if pmf is true, returns the normalized rank error for the get_PMF() function.
     * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
     */
    double get_normalized_rank_error(bool pmf) const;

    /**
     * Computes size needed to serialize the current state of the sketch.
     * This version is for fixed-size arithmetic types (integral and floating point).
     * @param serde instance of a SerDe
     * @return size in bytes needed to serialize this sketch
     */
    template<typename TT = T, typename SerDe = S, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
    size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

    /**
     * Computes size needed to serialize the current state of the sketch.
     * This version is for all other types and can be expensive since every item needs to be looked at.
     * @param serde instance of a SerDe
     * @return size in bytes needed to serialize this sketch
     */
    template<typename TT = T, typename SerDe = S, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
    size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

    /**
     * Returns upper bound on the serialized size of a sketch given a parameter <em>k</em> and stream
     * length. The resulting size is an overestimate to make sure actual sketches don't exceed it.
     * This method can be used if allocation of storage is necessary beforehand, but it is not
     * optimal.
     * This method is for arithmetic types (integral and floating point)
     * @param k parameter that controls size of the sketch and accuracy of estimates
     * @param n stream length
     * @return upper bound on the serialized size
     */
    template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
    static size_t get_max_serialized_size_bytes(uint16_t k, uint64_t n);

    /**
     * Returns upper bound on the serialized size of a sketch given a parameter <em>k</em> and stream
     * length. The resulting size is an overestimate to make sure actual sketches don't exceed it.
     * This method can be used if allocation of storage is necessary beforehand, but it is not
     * optimal.
     * This method is for all other non-arithmetic types, and it takes a max size of an item as input.
     * @param k parameter that controls size of the sketch and accuracy of estimates
     * @param n stream length
     * @param max_item_size_bytes maximum size of an item in bytes
     * @return upper bound on the serialized size
     */
    template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
    static size_t get_max_serialized_size_bytes(uint16_t k, uint64_t n, size_t max_item_size_bytes);

    /**
     * This method serializes the sketch into a given stream in a binary form
     * @param os output stream
     * @param instance of a SerDe
     */
    template<typename SerDe = S>
    void serialize(std::ostream& os, const SerDe& sd = SerDe()) const;

    // This is a convenience alias for users
    // The type returned by the following serialize method
    using vector_bytes = vector_u8<A>;

    /**
     * This method serializes the sketch as a vector of bytes.
     * An optional header can be reserved in front of the sketch.
     * It is a blank space of a given size.
     * This header is used in Datasketches PostgreSQL extension.
     * @param header_size_bytes space to reserve in front of the sketch
     * @param instance of a SerDe
     * @return serialized sketch as a vector of bytes
     */
    template<typename SerDe = S>
    vector_bytes serialize(unsigned header_size_bytes = 0, const SerDe& sd = SerDe()) const;

    /**
     * This method deserializes a sketch from a given stream.
     * @param is input stream
     * @param allocator instance of an Allocator
     * @return an instance of a sketch
     *
     * Deprecated, to be removed in the next major version
     */
    static kll_sketch deserialize(std::istream& is, const A& allocator = A());

    /**
     * This method deserializes a sketch from a given stream.
     * @param is input stream
     * @param serde instance of a SerDe
     * @param allocator instance of an Allocator
     * @return an instance of a sketch
     */
    template<typename SerDe = S>
    static kll_sketch deserialize(std::istream& is, const SerDe& sd = SerDe(), const A& allocator = A());

    /**
     * This method deserializes a sketch from a given array of bytes.
     * @param bytes pointer to the array of bytes
     * @param size the size of the array
     * @param allocator instance of an Allocator
     * @return an instance of a sketch
     *
     * Deprecated, to be removed in the next major version
     */
    static kll_sketch deserialize(const void* bytes, size_t size, const A& allocator = A());

    /**
     * This method deserializes a sketch from a given array of bytes.
     * @param bytes pointer to the array of bytes
     * @param size the size of the array
     * @param serde instance of a SerDe
     * @param allocator instance of an Allocator
     * @return an instance of a sketch
     */
    template<typename SerDe = S>
    static kll_sketch deserialize(const void* bytes, size_t size, const SerDe& sd = SerDe(), const A& allocator = A());

    /*
     * Gets the normalized rank error given k and pmf.
     * k - the configuration parameter
     * pmf - if true, returns the "double-sided" normalized rank error for the get_PMF() function.
     * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
     * Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials
     */
    static double get_normalized_rank_error(uint16_t k, bool pmf);

    /**
     * Prints a summary of the sketch.
     * @param print_levels if true include information about levels
     * @param print_items if true include sketch data
     */
    string<A> to_string(bool print_levels = false, bool print_items = false) const;

    class const_iterator;
    const_iterator begin() const;
    const_iterator end() const;

    template<bool inclusive = false>
    quantile_sketch_sorted_view<T, C, A> get_sorted_view(bool cumulative) const;

    #ifdef KLL_VALIDATION
    uint8_t get_num_levels() { return num_levels_; }
    uint32_t* get_levels() { return levels_; }
    T* get_items() { return items_; }
    #endif

  private:
    /* Serialized sketch layout:
     *  Addr:
     *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
     *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
     *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
     *  1   ||-----------------------------------N------------------------------------------|
     *      ||   23    |   22  |   21   |   20   |   19   |    18   |   17   |      16      |
     *  2   ||---------------data----------------|-unused-|numLevels|-------min K-----------|
     */

    static const size_t EMPTY_SIZE_BYTES = 8;
    static const size_t DATA_START_SINGLE_ITEM = 8;
    static const size_t DATA_START = 20;

    static const uint8_t SERIAL_VERSION_1 = 1;
    static const uint8_t SERIAL_VERSION_2 = 2;
    static const uint8_t FAMILY = 15;

    enum flags { IS_EMPTY, IS_LEVEL_ZERO_SORTED, IS_SINGLE_ITEM };

    static const uint8_t PREAMBLE_INTS_SHORT = 2; // for empty and single item
    static const uint8_t PREAMBLE_INTS_FULL = 5;

    A allocator_;
    uint16_t k_;
    uint8_t m_; // minimum buffer "width"
    uint16_t min_k_; // for error estimation after merging with different k
    uint64_t n_;
    uint8_t num_levels_;
    vector_u32<A> levels_;
    T* items_;
    uint32_t items_size_;
    T* min_value_;
    T* max_value_;
    bool is_level_zero_sorted_;

    // for deserialization
    class item_deleter;
    class items_deleter;
    kll_sketch(uint16_t k, uint16_t min_k, uint64_t n, uint8_t num_levels, vector_u32<A>&& levels,
        std::unique_ptr<T, items_deleter> items, uint32_t items_size, std::unique_ptr<T, item_deleter> min_value,
        std::unique_ptr<T, item_deleter> max_value, bool is_level_zero_sorted);

    // common update code
    inline void update_min_max(const T& value);
    inline uint32_t internal_update();

    // The following code is only valid in the special case of exactly reaching capacity while updating.
    // It cannot be used while merging, while reducing k, or anything else.
    void compress_while_updating(void);

    uint8_t find_level_to_compact() const;
    void add_empty_top_level_to_completely_full_sketch();
    void sort_level_zero();

    template<bool inclusive>
    vector_d<A> get_PMF_or_CDF(const T* split_points, uint32_t size, bool is_CDF) const;
    template<bool inclusive>
    void increment_buckets_unsorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight,
        const T* split_points, uint32_t size, double* buckets) const;
    template<bool inclusive>
    void increment_buckets_sorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight,
        const T* split_points, uint32_t size, double* buckets) const;

    template<typename O> void merge_higher_levels(O&& other, uint64_t final_n);

    template<typename FwdSk>
    void populate_work_arrays(FwdSk&& other, T* workbuf, uint32_t* worklevels, uint8_t provisional_num_levels);

    void assert_correct_total_weight() const;
    uint32_t safe_level_size(uint8_t level) const;
    uint32_t get_num_retained_above_level_zero() const;

    static void check_m(uint8_t m);
    static void check_preamble_ints(uint8_t preamble_ints, uint8_t flags_byte);
    static void check_serial_version(uint8_t serial_version);
    static void check_family_id(uint8_t family_id);

    void check_sorting() const;

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

    // implementations for all other types
    template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
    static const TT& get_invalid_value() {
      throw std::runtime_error("getting quantiles from empty sketch is not supported for this type of value");
    }

    template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
    static inline bool check_update_value(TT) {
      return true;
    }

    // for type converting constructor
    template<typename TT, typename CC, typename SS, typename AA>
    friend class kll_sketch;
};

template<typename T, typename C, typename S, typename A>
class kll_sketch<T, C, S, A>::const_iterator: public std::iterator<std::input_iterator_tag, T> {
public:
  friend class kll_sketch<T, C, S, A>;
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  const std::pair<const T&, const uint64_t> operator*() const;
private:
  const T* items;
  const uint32_t* levels;
  const uint8_t num_levels;
  uint32_t index;
  uint8_t level;
  uint64_t weight;
  const_iterator(const T* items, const uint32_t* levels, const uint8_t num_levels);
};

} /* namespace datasketches */

#include "kll_sketch_impl.hpp"

#endif
