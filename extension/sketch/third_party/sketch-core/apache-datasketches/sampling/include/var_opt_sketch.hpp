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

#ifndef _VAR_OPT_SKETCH_HPP_
#define _VAR_OPT_SKETCH_HPP_

#include "serde.hpp"
#include "common_defs.hpp"

#include <iterator>
#include <vector>


/**
 * This sketch samples data from a stream of items, designed for optimal (minimum) variance when
 * querying the sketch to estimate subset sums of items matchng a provided predicate. Variance
 * optimal (varopt) sampling is related to reservoir sampling, with improved error bounds for
 * subset sum estimation.
 * 
 * author Kevin Lang 
 * author Jon Malkin
 */
namespace datasketches {

template<typename A> using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
template<typename A> using vector_u8 = std::vector<uint8_t, AllocU8<A>>;

/**
 * A struct to hold the result of subset sum queries
 */
struct subset_summary {
  double lower_bound;
  double estimate;
  double upper_bound;
  double total_sketch_weight;
};

template <typename T, typename S, typename A> class var_opt_union; // forward declaration

namespace var_opt_constants {
    const resize_factor DEFAULT_RESIZE_FACTOR = resize_factor::X8;
    const uint32_t MAX_K = ((uint32_t) 1 << 31) - 2; 
}

template<
  typename T,
  typename S = serde<T>, // deprecated, to be removed in the next major version
  typename A = std::allocator<T>
>
class var_opt_sketch {

  public:
    static const resize_factor DEFAULT_RESIZE_FACTOR = var_opt_constants::DEFAULT_RESIZE_FACTOR;
    static const uint32_t MAX_K = var_opt_constants::MAX_K;

    explicit var_opt_sketch(uint32_t k,
      resize_factor rf = var_opt_constants::DEFAULT_RESIZE_FACTOR,
      const A& allocator = A());
    var_opt_sketch(const var_opt_sketch& other);
    var_opt_sketch(var_opt_sketch&& other) noexcept;

    ~var_opt_sketch();

    var_opt_sketch& operator=(const var_opt_sketch& other);
    var_opt_sketch& operator=(var_opt_sketch&& other);

    /**
     * Updates this sketch with the given data item with the given weight.
     * This method takes an lvalue.
     * @param item an item from a stream of items
     * @param weight the weight of the item
     */
    void update(const T& item, double weight=1.0);

    /**
     * Updates this sketch with the given data item with the given weight.
     * This method takes an rvalue.
     * @param item an item from a stream of items
     * @param weight the weight of the item
     */
    void update(T&& item, double weight=1.0);

    /**
     * Returns the configured maximum sample size.
     * @return configured maximum sample size
     */
    inline uint32_t get_k() const;

    /**
     * Returns the length of the input stream.
     * @return stream length
     */
    inline uint64_t get_n() const;

    /**
     * Returns the number of samples currently in the sketch
     * @return stream length
     */
    inline uint32_t get_num_samples() const;
    
    /**
     * Computes an estimated subset sum from the entire stream for objects matching a given
     * predicate. Provides a lower bound, estimate, and upper bound using a target of 2 standard
     * deviations. This is technically a heuristic method and tries to err on the conservative side.
     * @param P a predicate function
     * @return a subset_summary item with estimate, upper and lower bounds,
     *         and total sketch weight
     */
    template<typename P>
    subset_summary estimate_subset_sum(P predicate) const;

    /**
     * Returns true if the sketch is empty.
     * @return empty flag
     */
    inline bool is_empty() const;
    
    /**
     * Resets the sketch to its default, empty state.
     */
    void reset();

    /**
     * Computes size needed to serialize the current state of the sketch.
     * This version is for fixed-size arithmetic types (integral and floating point).
     * @param instance of a SerDe
     * @return size in bytes needed to serialize this sketch
     */
    template<typename TT = T, typename SerDe = S, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
    inline size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

    /**
     * Computes size needed to serialize the current state of the sketch.
     * This version is for all other types and can be expensive since every item needs to be looked at.
     * @param instance of a SerDe
     * @return size in bytes needed to serialize this sketch
     */
    template<typename TT = T, typename SerDe = S, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
    inline size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

    // This is a convenience alias for users
    // The type returned by the following serialize method
    typedef vector_u8<A> vector_bytes;

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
     * This method serializes the sketch into a given stream in a binary form
     * @param os output stream
     * @param instance of a SerDe
     */
    template<typename SerDe = S>
    void serialize(std::ostream& os, const SerDe& sd = SerDe()) const;

    /**
     * This method deserializes a sketch from a given stream.
     * @param is input stream
     * @param instance of an Allocator
     * @return an instance of a sketch
     *
     * Deprecated, to be removed in the next major version
     */
    static var_opt_sketch deserialize(std::istream& is, const A& allocator = A());

    /**
     * This method deserializes a sketch from a given stream.
     * @param is input stream
     * @param instance of a SerDe
     * @param instance of an Allocator
     * @return an instance of a sketch
     */
    template<typename SerDe = S>
    static var_opt_sketch deserialize(std::istream& is, const SerDe& sd = SerDe(), const A& allocator = A());

    /**
     * This method deserializes a sketch from a given array of bytes.
     * @param bytes pointer to the array of bytes
     * @param size the size of the array
     * @param instance of an Allocator
     * @return an instance of a sketch
     *
     * Deprecated, to be removed in the next major version
     */
    static var_opt_sketch deserialize(const void* bytes, size_t size, const A& allocator = A());

    /**
     * This method deserializes a sketch from a given array of bytes.
     * @param bytes pointer to the array of bytes
     * @param size the size of the array
     * @param instance of a SerDe
     * @param instance of an Allocator
     * @return an instance of a sketch
     */
    template<typename SerDe = S>
    static var_opt_sketch deserialize(const void* bytes, size_t size, const SerDe& sd = SerDe(), const A& allocator = A());

    /**
     * Prints a summary of the sketch.
     * @return the summary as a string
     */
    string<A> to_string() const;

    /**
     * Prints the raw sketch items to a string. Calls items_to_stream() internally.
     * Only works for type T with a defined operator<<() and
     * kept separate from to_string() to allow compilation even if
     * T does not have such an operator defined.
     * @return a string with the sketch items
     */
    string<A> items_to_string() const;

    class const_iterator;
    const_iterator begin() const;
    const_iterator end() const;

  private:
    typedef typename std::allocator_traits<A>::template rebind_alloc<double> AllocDouble;
    typedef typename std::allocator_traits<A>::template rebind_alloc<bool> AllocBool;

    static const uint32_t MIN_LG_ARR_ITEMS = 3;

    static const uint8_t PREAMBLE_LONGS_EMPTY  = 1;
    static const uint8_t PREAMBLE_LONGS_WARMUP = 3;
    static const uint8_t PREAMBLE_LONGS_FULL   = 4;
    static const uint8_t SER_VER = 2;
    static const uint8_t FAMILY_ID  = 13;
    static const uint8_t EMPTY_FLAG_MASK  = 4;
    static const uint8_t GADGET_FLAG_MASK = 128;

    // Number of standard deviations to use for subset sum error bounds
    constexpr static const double DEFAULT_KAPPA = 2.0;

    // TODO: should probably rearrange a bit to minimize gaps once aligned
    uint32_t k_;                    // max size of sketch, in items

    uint32_t h_;                    // number of items in heap
    uint32_t m_;                    // number of items in middle region
    uint32_t r_;                    // number of items in reservoir-like region

    uint64_t n_;                    // total number of items processed by sketch
    double total_wt_r_;             // total weight of items in reservoir-like area

    resize_factor rf_;              // resize factor

    uint32_t curr_items_alloc_;     // currently allocated array size
    bool filled_data_;              // true if we've explicitly set all entries in data_

    A allocator_;
    T* data_;                       // stored sampled items
    double* weights_;               // weights for sampled items

    // The next two fields are hidden from the user because they are part of the state of the
    // unioning algorithm, NOT part of a varopt sketch, or even of a varopt "gadget" (our name for
    // the potentially invalid sketch that is maintained by the unioning algorithm). It would make
    // more sense logically for these fields to be declared in the unioning object (whose entire
    // purpose is storing the state of the unioning algorithm) but for reasons of programming
    // convenience we are currently declaring them here. However, that could change in the future.

    // Following int is:
    //  1. Zero (for a varopt sketch)
    //  2. Count of marked items in H region, if part of a unioning algo's gadget
    uint32_t num_marks_in_h_;

    // The following array is absent in a varopt sketch, and notionally present in a gadget
    // (although it really belongs in the unioning object). If the array were to be made explicit,
    // some additional coding would need to be done to ensure that all of the necessary data motion
    // occurs and is properly tracked.
    bool* marks_;

    // used during deserialization to avoid memory leaks upon errors
    class items_deleter;
    class weights_deleter;
    class marks_deleter;

    var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget, const A& allocator);
    var_opt_sketch(uint32_t k, uint32_t h, uint32_t m, uint32_t r, uint64_t n, double total_wt_r, resize_factor rf,
                   uint32_t curr_items_alloc, bool filled_data, std::unique_ptr<T, items_deleter> items,
                   std::unique_ptr<double, weights_deleter> weights, uint32_t num_marks_in_h,
                   std::unique_ptr<bool, marks_deleter> marks, const A& allocator);

    friend class var_opt_union<T,S,A>;
    var_opt_sketch(const var_opt_sketch& other, bool as_sketch, uint64_t adjusted_n);
    var_opt_sketch(T* data, double* weights, size_t len, uint32_t k, uint64_t n, uint32_t h_count, uint32_t r_count, double total_wt_r, const A& allocator);

    string<A> items_to_string(bool print_gap) const;

    // internal-use-only update
    template<typename O>
    inline void update(O&& item, double weight, bool mark);
    
    template<typename O>
    inline void update_warmup_phase(O&& item, double weight, bool mark);
    
    template<typename O>
    inline void update_light(O&& item, double weight, bool mark);
    
    template<typename O>
    inline void update_heavy_r_eq1(O&& item, double weight, bool mark);
    
    template<typename O>
    inline void update_heavy_general(O&& item, double weight, bool mark);

    inline double get_tau() const;
    inline double peek_min() const;
    inline bool is_marked(uint32_t idx) const;
    
    inline uint32_t pick_random_slot_in_r() const;
    inline uint32_t choose_delete_slot(double wt_cand, uint32_t num_cand) const;
    inline uint32_t choose_weighted_delete_slot(double wt_cand, uint32_t num_cand) const;

    template<typename O>
    inline void push(O&& item, double wt, bool mark);
    inline void transition_from_warmup();
    inline void convert_to_heap();
    inline void restore_towards_leaves(uint32_t slot_in);
    inline void restore_towards_root(uint32_t slot_in);
    inline void pop_min_to_m_region();
    void grow_candidate_set(double wt_cands, uint32_t num_cands);    
    void decrease_k_by_1();
    void strip_marks();
    void force_set_k(uint32_t k); // used to resolve union gadget into sketch
    void downsample_candidate_set(double wt_cands, uint32_t num_cands);
    inline void swap_values(uint32_t src, uint32_t dst);
    void grow_data_arrays();
    void allocate_data_arrays(uint32_t tgt_size, bool use_marks);

    // validation
    static void check_preamble_longs(uint8_t preamble_longs, uint8_t flags);
    static void check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver);
    static uint32_t validate_and_get_target_size(uint32_t preamble_longs, uint32_t k, uint64_t n,
                                                 uint32_t h, uint32_t r, resize_factor rf);

    // things to move to common and be shared among sketches
    static uint32_t get_adjusted_size(uint32_t max_size, uint32_t resize_target);
    static uint32_t starting_sub_multiple(uint32_t lg_target, uint32_t lg_rf, uint32_t lg_min);
    static inline double pseudo_hypergeometric_ub_on_p(uint64_t n, uint32_t k, double sampling_rate);
    static inline double pseudo_hypergeometric_lb_on_p(uint64_t n, uint32_t k, double sampling_rate);
    static bool is_power_of_2(uint32_t v);
    static uint32_t to_log_2(uint32_t v);
    static inline uint32_t next_int(uint32_t max_value);
    static inline double next_double_exclude_zero();

    class iterator;
};

template<typename T, typename S, typename A>
class var_opt_sketch<T, S, A>::const_iterator : public std::iterator<std::input_iterator_tag, T> {
public:
  const_iterator(const const_iterator& other);
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  const std::pair<const T&, const double> operator*() const;

private:
  friend class var_opt_sketch<T,S,A>;
  friend class var_opt_union<T,S,A>;

  // default iterator over full sketch
  const_iterator(const var_opt_sketch<T,S,A>& sk, bool is_end);
  
  // iterates over only one of the H or R region, optionally applying weight correction
  // to R region (can correct for numerical precision issues)
  const_iterator(const var_opt_sketch<T,S,A>& sk, bool is_end, bool use_r_region);

  bool get_mark() const;

  const var_opt_sketch<T,S,A>* sk_;
  double cum_r_weight_; // used for weight correction
  double r_item_wt_;
  size_t idx_;
  const size_t final_idx_;
//  bool weight_correction_;
};

// non-const iterator for internal use
template<typename T, typename S, typename A>
class var_opt_sketch<T, S, A>::iterator : public std::iterator<std::input_iterator_tag, T> {
public:
  iterator(const iterator& other);
  iterator& operator++();
  iterator& operator++(int);
  bool operator==(const iterator& other) const;
  bool operator!=(const iterator& other) const;
  std::pair<T&, double> operator*();

private:
  friend class var_opt_sketch<T,S,A>;
  friend class var_opt_union<T,S,A>;
  
  // iterates over only one of the H or R region, applying weight correction
  // if iterating over R region (can correct for numerical precision issues)
  iterator(const var_opt_sketch<T,S,A>& sk, bool is_end, bool use_r_region);

  bool get_mark() const;

  const var_opt_sketch<T,S,A>* sk_;
  double cum_r_weight_; // used for weight correction
  double r_item_wt_;
  size_t idx_;
  const size_t final_idx_;
};


} // namespace datasketches

#include "var_opt_sketch_impl.hpp"

#endif // _VAR_OPT_SKETCH_HPP_
