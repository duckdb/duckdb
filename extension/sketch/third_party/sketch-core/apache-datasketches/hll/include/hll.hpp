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

#ifndef _HLL_HPP_
#define _HLL_HPP_

#include "common_defs.hpp"
#include "HllUtil.hpp"

#include <memory>
#include <iostream>
#include <vector>

namespace datasketches {

  /**
 * This is a high performance implementation of Phillipe Flajolet&#8217;s HLL sketch but with
 * significantly improved error behavior.  If the ONLY use case for sketching is counting
 * uniques and merging, the HLL sketch is a reasonable choice, although the highest performing in terms of accuracy for
 * storage space consumed is CPC (Compressed Probabilistic Counting). For large enough counts, this HLL version (with HLL_4) can be 2 to
 * 16 times smaller than the Theta sketch family for the same accuracy.
 *
 * <p>This implementation offers three different types of HLL sketch, each with different
 * trade-offs with accuracy, space and performance. These types are specified with the
 * {@link TgtHllType} parameter.
 *
 * <p>In terms of accuracy, all three types, for the same <i>lg_config_k</i>, have the same error
 * distribution as a function of <i>n</i>, the number of unique values fed to the sketch.
 * The configuration parameter <i>lg_config_k</i> is the log-base-2 of <i>K</i>,
 * where <i>K</i> is the number of buckets or slots for the sketch.
 *
 * <p>During warmup, when the sketch has only received a small number of unique items
 * (up to about 10% of <i>K</i>), this implementation leverages a new class of estimator
 * algorithms with significantly better accuracy.
 *
 * <p>This sketch also offers the capability of operating off-heap. Given a WritableMemory object
 * created by the user, the sketch will perform all of its updates and internal phase transitions
 * in that object, which can actually reside either on-heap or off-heap based on how it is
 * configured. In large systems that must update and merge many millions of sketches, having the
 * sketch operate off-heap avoids the serialization and deserialization costs of moving sketches
 * to and from off-heap memory-mapped files, for example, and eliminates big garbage collection
 * delays.
 *
 * author Jon Malkin
 * author Lee Rhodes
 * author Kevin Lang
 */

  
/**
 * Specifies the target type of HLL sketch to be created. It is a target in that the actual
 * allocation of the HLL array is deferred until sufficient number of items have been received by
 * the warm-up phases.
 *
 * <p>These three target types are isomorphic representations of the same underlying HLL algorithm.
 * Thus, given the same value of <i>lg_config_k</i> and the same input, all three HLL target types
 * will produce identical estimates and have identical error distributions.</p>
 *
 * <p>The memory (and also the serialization) of the sketch during this early warmup phase starts
 * out very small (8 bytes, when empty) and then grows in increments of 4 bytes as required
 * until the full HLL array is allocated.  This transition point occurs at about 10% of K for
 * sketches where lg_config_k is &gt; 8.</p>
 *
 * <ul>
 * <li><b>HLL_8</b> This uses an 8-bit byte per HLL bucket. It is generally the
 * fastest in terms of update time, but has the largest storage footprint of about
 * <i>K</i> bytes.</li>
 *
 * <li><b>HLL_6</b> This uses a 6-bit field per HLL bucket. It is the generally the next fastest
 * in terms of update time with a storage footprint of about <i>3/4 * K</i> bytes.</li>
 *
 * <li><b>HLL_4</b> This uses a 4-bit field per HLL bucket and for large counts may require
 * the use of a small internal auxiliary array for storing statistical exceptions, which are rare.
 * For the values of <i>lg_config_k &gt; 13</i> (<i>K</i> = 8192),
 * this additional array adds about 3% to the overall storage. It is generally the slowest in
 * terms of update time, but has the smallest storage footprint of about
 * <i>K/2 * 1.03</i> bytes.</li>
 * </ul>
 */
enum target_hll_type {
    HLL_4, ///< 4 bits per entry (most compact, size may vary)
    HLL_6, ///< 6 bits per entry (fixed size)
    HLL_8  ///< 8 bits per entry (fastest, fixed size)
};

template<typename A>
class HllSketchImpl;

template<typename A>
class hll_union_alloc;

template<typename A> using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
template<typename A> using vector_u8 = std::vector<uint8_t, AllocU8<A>>;

template<typename A = std::allocator<uint8_t> >
class hll_sketch_alloc final {
  public:
    /**
     * Constructs a new HLL sketch.
     * @param lg_config_k Sketch can hold 2^lg_config_k rows
     * @param tgt_type The HLL mode to use, if/when the sketch reaches that state
     * @param start_full_size Indicates whether to start in HLL mode,
     *        keeping memory use constant (if HLL_6 or HLL_8) at the cost of
     *        starting out using much more memory
     */
    explicit hll_sketch_alloc(uint8_t lg_config_k, target_hll_type tgt_type = HLL_4, bool start_full_size = false, const A& allocator = A());

    /**
     * Copy constructor
     */
    hll_sketch_alloc(const hll_sketch_alloc<A>& that);

    /**
     * Copy constructor to a new target type
     */
    hll_sketch_alloc(const hll_sketch_alloc<A>& that, target_hll_type tgt_type);

    /**
     * Move constructor
     */
    hll_sketch_alloc(hll_sketch_alloc<A>&& that) noexcept;

    /**
     * Reconstructs a sketch from a serialized image on a stream.
     * @param is An input stream with a binary image of a sketch
     */
    static hll_sketch_alloc deserialize(std::istream& is, const A& allocator = A());

    /**
     * Reconstructs a sketch from a serialized image in a byte array.
     * @param is bytes An input array with a binary image of a sketch
     * @param len Length of the input array, in bytes
     */
    static hll_sketch_alloc deserialize(const void* bytes, size_t len, const A& allocator = A());

    //! Class destructor
    virtual ~hll_sketch_alloc();

    //! Copy assignment operator
    hll_sketch_alloc operator=(const hll_sketch_alloc<A>& other);

    //! Move assignment operator
    hll_sketch_alloc operator=(hll_sketch_alloc<A>&& other);

    /**
     * Resets the sketch to an empty state in coupon collection mode.
     * Does not re-use existing internal objects.
     */
    void reset();

    typedef vector_u8<A> vector_bytes; // alias for users

    /**
     * Serializes the sketch to a byte array, compacting data structures
     * where feasible to eliminate unused storage in the serialized image.
     * @param header_size_bytes Allows for PostgreSQL integration
     */
    vector_bytes serialize_compact(unsigned header_size_bytes = 0) const;

    /**
     * Serializes the sketch to a byte array, retaining all internal 
     * data structures in their current form.
     */
    vector_bytes serialize_updatable() const;

    /**
     * Serializes the sketch to an ostream, compacting data structures
     * where feasible to eliminate unused storage in the serialized image.
     * @param os std::ostream to use for output.
     */
    void serialize_compact(std::ostream& os) const;

    /**
     * Serializes the sketch to an ostream, retaining all internal data
     * structures in their current form.
     * @param os std::ostream to use for output.
     */
    void serialize_updatable(std::ostream& os) const;

    /**
     * Human readable summary with optional detail
     * @param summary if true, output the sketch summary
     * @param detail if true, output the internal data array
     * @param auxDetail if true, output the internal Aux array, if it exists.
     * @param all if true, outputs all entries including empty ones
     * @return human readable string with optional detail.
     */
    string<A> to_string(bool summary = true,
                        bool detail = false,
                        bool aux_detail = false,
                        bool all = false) const;

    /**
     * Present the given std::string as a potential unique item.
     * The string is converted to a byte array using UTF8 encoding.
     * If the string is null or empty no update attempt is made and the method returns.
     * @param datum The given string.
     */
    void update(const std::string& datum);

    /**
     * Present the given unsigned 64-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint64_t datum);

    /**
     * Present the given unsigned 32-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint32_t datum);

    /**
     * Present the given unsigned 16-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint16_t datum);

    /**
     * Present the given unsigned 8-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint8_t datum);

    /**
     * Present the given signed 64-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int64_t datum);

    /**
     * Present the given signed 32-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int32_t datum);

    /**
     * Present the given signed 16-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int16_t datum);

    /**
     * Present the given signed 8-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int8_t datum);

    /**
     * Present the given 64-bit floating point value as a potential unique item.
     * @param datum The given double.
     */
    void update(double datum);

    /**
     * Present the given 32-bit floating point value as a potential unique item.
     * @param datum The given float.
     */
    void update(float datum);

    /**
     * Present the given data array as a potential unique item.
     * @param data The given array.
     * @param length_bytes The array length in bytes.
     */
    void update(const void* data, size_t length_bytes);

    /**
     * Returns the current cardinality estimate
     * @return the cardinality estimate
     */
    double get_estimate() const;

    /**
     * This is less accurate than the getEstimate() method
     * and is automatically used when the sketch has gone through
     * union operations where the more accurate HIP estimator cannot
     * be used.
     *
     * This is made public only for error characterization software
     * that exists in separate packages and is not intended for normal
     * use.
     * @return the composite cardinality estimate
     */
    double get_composite_estimate() const;

    /**
     * Returns the approximate lower error bound given the specified
     * number of standard deviations.
     * @param num_std_dev Number of standard deviations, an integer from the set  {1, 2, 3}.
     * @return The approximate lower bound.
     */
    double get_lower_bound(uint8_t num_std_dev) const;

    /**
     * Returns the approximate upper error bound given the specified
     * number of standard deviations.
     * @param num_std_dev Number of standard deviations, an integer from the set  {1, 2, 3}.
     * @return The approximate upper bound.
     */
    double get_upper_bound(uint8_t num_std_dev) const;

    /**
     * Returns sketch's configured lg_k value.
     * @return Configured lg_k value.
     */
    uint8_t get_lg_config_k() const;

    /**
     * Returns the sketch's target HLL mode (from #target_hll_type).
     * @return The sketch's target HLL mode.
     */
    target_hll_type get_target_type() const;

    /**
     * Indicates if the sketch is currently stored compacted.
     * @return True if the sketch is stored in compact form.
     */
    bool is_compact() const;

    /**
     * Indicates if the sketch is currently empty.
     * @return True if the sketch is empty.
     */
    bool is_empty() const;

    /**
     * Returns the size of the sketch serialized in compact form.
     * @return Size of the sketch serialized in compact form, in bytes.
     */
    uint32_t get_compact_serialization_bytes() const;

    /**
     * Returns the size of the sketch serialized without compaction.
     * @return Size of the sketch serialized without compaction, in bytes.
     */
    uint32_t get_updatable_serialization_bytes() const;

    /**
     * Returns the maximum size in bytes that this sketch can grow to
     * given lg_config_k.  However, for the HLL_4 sketch type, this
     * value can be exceeded in extremely rare cases.  If exceeded, it
     * will be larger by only a few percent.
     *
     * @param lg_config_k The Log2 of K for the target HLL sketch. This value must be
     *        between 4 and 21 inclusively.
     * @param tgt_type the desired Hll type
     * @return the maximum size in bytes that this sketch can grow to.
     */
    static uint32_t get_max_updatable_serialization_bytes(uint8_t lg_k, target_hll_type tgt_type);
  
    /**
     * Gets the current (approximate) Relative Error (RE) asymptotic values given several
     * parameters. This is used primarily for testing.
     * @param upper_bound return the RE for the Upper Bound, otherwise for the Lower Bound.
     * @param unioned set true if the sketch is the result of a union operation.
     * @param lg_config_k the configured value for the sketch.
     * @param num_std_dev the given number of Standard Deviations. This must be an integer between
     * 1 and 3, inclusive.
     * @return the current (approximate) RelativeError
     */
    static double get_rel_err(bool upper_bound, bool unioned,
                              uint8_t lg_config_k, uint8_t num_std_dev);

  private:
    explicit hll_sketch_alloc(HllSketchImpl<A>* that);

    void coupon_update(uint32_t coupon);

    std::string type_as_string() const;
    std::string mode_as_string() const;

    hll_mode get_current_mode() const;
    uint8_t get_serialization_version() const;
    bool is_out_of_order_flag() const;
    bool is_estimation_mode() const;

    typedef typename std::allocator_traits<A>::template rebind_alloc<hll_sketch_alloc> AllocHllSketch;

    HllSketchImpl<A>* sketch_impl;
    friend hll_union_alloc<A>;
};

/**
 * This performs union operations for HLL sketches. This union operator is configured with a
 * <i>lgMaxK</i> instead of the normal <i>lg_config_k</i>.
 *
 * <p>This union operator does permit the unioning of sketches with different values of
 * <i>lg_config_k</i>.  The user should be aware that the resulting accuracy of a sketch returned
 * at the end of the unioning process will be a function of the smallest of <i>lg_max_k</i> and
 * <i>lg_config_k</i> that the union operator has seen.
 *
 * <p>This union operator also permits unioning of any of the three different target hll_sketch
 * types.
 *
 * <p>Although the API for this union operator parallels many of the methods of the
 * <i>HllSketch</i>, the behavior of the union operator has some fundamental differences.
 *
 * <p>First, the user cannot specify the #tgt_hll_type as an input parameter.
 * Instead, it is specified for the sketch returned with #get_result(tgt_hll_tyope).
 *
 * <p>Second, the internal effective value of log-base-2 of <i>k</i> for the union operation can
 * change dynamically based on the smallest <i>lg_config_k</i> that the union operation has seen.
 *
 * author Jon Malkin
 * author Lee Rhodes
 * author Kevin Lang
 */
 
template<typename A = std::allocator<uint8_t> >
class hll_union_alloc {
  public:
    /**
     * Construct an hll_union operator with the given maximum log2 of k.
     * @param lg_max_k The maximum size, in log2, of k. The value must
     * be between 7 and 21, inclusive.
     */
    explicit hll_union_alloc(uint8_t lg_max_k, const A& allocator = A());

    /**
     * Returns the current cardinality estimate
     * @return the cardinality estimate
     */
    double get_estimate() const;

    /**
     * This is less accurate than the get_estimate() method
     * and is automatically used when the union has gone through
     * union operations where the more accurate HIP estimator cannot
     * be used.
     *
     * This is made public only for error characterization software
     * that exists in separate packages and is not intended for normal
     * use.
     * @return the composite cardinality estimate
     */
    double get_composite_estimate() const;

    /**
     * Returns the approximate lower error bound given the specified
     * number of standard deviations.
     * @param num_std_dev Number of standard deviations, an integer from the set  {1, 2, 3}.
     * @return The approximate lower bound.
     */
    double get_lower_bound(uint8_t num_std_dev) const;

    /**
     * Returns the approximate upper error bound given the specified
     * number of standard deviations.
     * @param num_std_dev Number of standard deviations, an integer from the set  {1, 2, 3}.
     * @return The approximate upper bound.
     */
    double get_upper_bound(uint8_t num_std_dev) const;

    /**
     * Returns union's configured lg_k value.
     * @return Configured lg_k value.
     */
    uint8_t get_lg_config_k() const;

    /**
     * Returns the union's target HLL mode (from #target_hll_type).
     * @return The union's target HLL mode.
     */
    target_hll_type get_target_type() const;

    /**
     * Indicates if the union is currently empty.
     * @return True if the union is empty.
     */
    bool is_empty() const;

    /**
     * Resets the union to an empty state in coupon collection mode.
     * Does not re-use existing internal objects.
     */
    void reset();

    /**
     * Returns the result of this union operator with the specified
     * #tgt_hll_type.
     * @param The tgt_hll_type enum value of the desired result (Default: HLL_4)
     * @return The result of this union with the specified tgt_hll_type
     */
    hll_sketch_alloc<A> get_result(target_hll_type tgt_type = HLL_4) const;

    /**
     * Update this union operator with the given sketch.
     * @param The given sketch.
     */
    void update(const hll_sketch_alloc<A>& sketch);

    /**
     * Update this union operator with the given temporary sketch.
     * @param The given sketch.
     */
    void update(hll_sketch_alloc<A>&& sketch);
  
    /**
     * Present the given std::string as a potential unique item.
     * The string is converted to a byte array using UTF8 encoding.
     * If the string is null or empty no update attempt is made and the method returns.
     * @param datum The given string.
     */
    void update(const std::string& datum);

    /**
     * Present the given unsigned 64-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint64_t datum);

    /**
     * Present the given unsigned 32-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint32_t datum);

    /**
     * Present the given unsigned 16-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint16_t datum);

    /**
     * Present the given unsigned 8-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(uint8_t datum);

    /**
     * Present the given signed 64-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int64_t datum);

    /**
     * Present the given signed 32-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int32_t datum);

    /**
     * Present the given signed 16-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int16_t datum);

    /**
     * Present the given signed 8-bit integer as a potential unique item.
     * @param datum The given integer.
     */
    void update(int8_t datum);

    /**
     * Present the given 64-bit floating point value as a potential unique item.
     * @param datum The given double.
     */
    void update(double datum);

    /**
     * Present the given 32-bit floating point value as a potential unique item.
     * @param datum The given float.
     */
    void update(float datum);

    /**
     * Present the given data array as a potential unique item.
     * @param data The given array.
     * @param length_bytes The array length in bytes.
     */
    void update(const void* data, size_t length_bytes);

    /**
     * Gets the current (approximate) Relative Error (RE) asymptotic values given several
     * parameters. This is used primarily for testing.
     * @param upper_bound return the RE for the Upper Bound, otherwise for the Lower Bound.
     * @param unioned set true if the sketch is the result of a union operation.
     * @param lg_config_k the configured value for the sketch.
     * @param num_std_dev the given number of Standard Deviations. This must be an integer between
     * 1 and 3, inclusive.
     * @return the current (approximate) RelativeError
     */
    static double get_rel_err(bool upper_bound, bool unioned,
                              uint8_t lg_config_k, uint8_t num_std_dev);

  private:

   /**
    * Union the given source and destination sketches. This method examines the state of
    * the current internal gadget and the incoming sketch and determines the optimal way to
    * perform the union. This may involve swapping, down-sampling, transforming, and / or
    * copying one of the arguments and may completely replace the internals of the union.
    *
    * @param incoming_impl the given incoming sketch, which may not be modified.
    * @param lg_max_k the maximum value of log2 K for this union.
    */
    inline void union_impl(const hll_sketch_alloc<A>& sketch, uint8_t lg_max_k);

    static HllSketchImpl<A>* copy_or_downsample(const HllSketchImpl<A>* src_impl, uint8_t tgt_lg_k);

    void coupon_update(uint32_t coupon);

    hll_mode get_current_mode() const;
    bool is_out_of_order_flag() const;
    bool is_estimation_mode() const;

    // calls couponUpdate on sketch, freeing the old sketch upon changes in hll_mode
    static HllSketchImpl<A>* leak_free_coupon_update(HllSketchImpl<A>* impl, uint32_t coupon);

    uint8_t lg_max_k_;
    hll_sketch_alloc<A> gadget_;
};

/// convenience alias for hll_sketch with default allocator
typedef hll_sketch_alloc<> hll_sketch;

/// convenience alias for hll_union with default allocator
typedef hll_union_alloc<> hll_union;

} // namespace datasketches

#include "hll.private.hpp"

#endif // _HLL_HPP_
