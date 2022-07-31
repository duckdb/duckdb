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

#ifndef THETA_UPDATE_SKETCH_BASE_HPP_
#define THETA_UPDATE_SKETCH_BASE_HPP_

#include <vector>
#include <climits>
#include <cmath>

#include "common_defs.hpp"
#include "MurmurHash3.h"
#include "theta_comparators.hpp"
#include "theta_constants.hpp"

namespace datasketches {

template<
  typename Entry,
  typename ExtractKey,
  typename Allocator
>
struct theta_update_sketch_base {
  using resize_factor = theta_constants::resize_factor;
  using comparator = compare_by_key<ExtractKey>;

  theta_update_sketch_base(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p,
      uint64_t theta, uint64_t seed, const Allocator& allocator, bool is_empty = true);
  theta_update_sketch_base(const theta_update_sketch_base& other);
  theta_update_sketch_base(theta_update_sketch_base&& other) noexcept;
  ~theta_update_sketch_base();
  theta_update_sketch_base& operator=(const theta_update_sketch_base& other);
  theta_update_sketch_base& operator=(theta_update_sketch_base&& other);

  using iterator = Entry*;

  inline uint64_t hash_and_screen(const void* data, size_t length);

  inline std::pair<iterator, bool> find(uint64_t key) const;
  static inline std::pair<iterator, bool> find(Entry* entries, uint8_t lg_size, uint64_t key);


  template<typename FwdEntry>
  inline void insert(iterator it, FwdEntry&& entry);

  iterator begin() const;
  iterator end() const;

  // resize threshold = 0.5 tuned for speed
  static constexpr double RESIZE_THRESHOLD = 0.5;
  // hash table rebuild threshold = 15/16
  static constexpr double REBUILD_THRESHOLD = 15.0 / 16.0;

  static constexpr uint8_t STRIDE_HASH_BITS = 7;
  static constexpr uint32_t STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

  Allocator allocator_;
  bool is_empty_;
  uint8_t lg_cur_size_;
  uint8_t lg_nom_size_;
  resize_factor rf_;
  float p_;
  uint32_t num_entries_;
  uint64_t theta_;
  uint64_t seed_;
  Entry* entries_;

  void resize();
  void rebuild();
  void trim();
  void reset();

  static inline uint32_t get_capacity(uint8_t lg_cur_size, uint8_t lg_nom_size);
  static inline uint32_t get_stride(uint64_t key, uint8_t lg_size);
  static void consolidate_non_empty(Entry* entries, size_t size, size_t num);
};

// builder

template<typename Derived, typename Allocator>
class theta_base_builder {
public:
  // TODO: Redundant and deprecated. Will be removed in next major version release.
  using resize_factor = theta_constants::resize_factor;
  static const uint8_t MIN_LG_K = theta_constants::MIN_LG_K;
  static const uint8_t MAX_LG_K = theta_constants::MAX_LG_K;
  // TODO: The following defaults are redundant and deprecated. Will be removed in the
  //       next major version release
  static const uint8_t DEFAULT_LG_K = theta_constants::DEFAULT_LG_K;
  static const resize_factor DEFAULT_RESIZE_FACTOR = theta_constants::DEFAULT_RESIZE_FACTOR;

  /**
   * Creates and instance of the builder with default parameters.
   */
  theta_base_builder(const Allocator& allocator);

  /**
   * Set log2(k), where k is a nominal number of entries in the sketch
   * @param lg_k base 2 logarithm of nominal number of entries
   * @return this builder
   */
  Derived& set_lg_k(uint8_t lg_k);

  /**
   * Set resize factor for the internal hash table (defaults to 8)
   * @param rf resize factor
   * @return this builder
   */
  Derived& set_resize_factor(resize_factor rf);

  /**
   * Set sampling probability (initial theta). The default is 1, so the sketch retains
   * all entries until it reaches the limit, at which point it goes into the estimation mode
   * and reduces the effective sampling probability (theta) as necessary.
   * @param p sampling probability
   * @return this builder
   */
  Derived& set_p(float p);

  /**
   * Set the seed for the hash function. Should be used carefully if needed.
   * Sketches produced with different seed are not compatible
   * and cannot be mixed in set operations.
   * @param seed hash seed
   * @return this builder
   */
  Derived& set_seed(uint64_t seed);

protected:
  Allocator allocator_;
  uint8_t lg_k_;
  resize_factor rf_;
  float p_;
  uint64_t seed_;

  uint64_t starting_theta() const;
  uint8_t starting_lg_size() const;
};

// key extractor

struct trivial_extract_key {
  template<typename T>
  auto operator()(T&& entry) const -> decltype(std::forward<T>(entry)) {
    return std::forward<T>(entry);
  }
};

// key not zero

template<typename Entry, typename ExtractKey>
class key_not_zero {
public:
  bool operator()(const Entry& entry) const {
    return ExtractKey()(entry) != 0;
  }
};

template<typename Key, typename Entry, typename ExtractKey>
class key_not_zero_less_than {
public:
  explicit key_not_zero_less_than(const Key& key): key(key) {}
  bool operator()(const Entry& entry) const {
    return ExtractKey()(entry) != 0 && ExtractKey()(entry) < this->key;
  }
private:
  Key key;
};

// MurMur3 hash functions

static inline uint64_t compute_hash(const void* data, size_t length, uint64_t seed) {
  HashState hashes;
  MurmurHash3_x64_128(data, length, seed, hashes);
  return (hashes.h1 >> 1); // Java implementation does unsigned shift >>> to make values positive
}

// iterators

template<typename Entry, typename ExtractKey>
class theta_iterator: public std::iterator<std::input_iterator_tag, Entry> {
public:
  theta_iterator(Entry* entries, uint32_t size, uint32_t index);
  theta_iterator& operator++();
  theta_iterator operator++(int);
  bool operator==(const theta_iterator& other) const;
  bool operator!=(const theta_iterator& other) const;
  Entry& operator*() const;

private:
  Entry* entries_;
  uint32_t size_;
  uint32_t index_;
};

template<typename Entry, typename ExtractKey>
class theta_const_iterator: public std::iterator<std::input_iterator_tag, Entry> {
public:
  theta_const_iterator(const Entry* entries, uint32_t size, uint32_t index);
  theta_const_iterator& operator++();
  theta_const_iterator operator++(int);
  bool operator==(const theta_const_iterator& other) const;
  bool operator!=(const theta_const_iterator& other) const;
  const Entry& operator*() const;

private:
  const Entry* entries_;
  uint32_t size_;
  uint32_t index_;
};

// double value canonicalization for compatibility with Java
static inline int64_t canonical_double(double value) {
  union {
    int64_t long_value;
    double double_value;
  } long_double_union;

  if (value == 0.0) {
    long_double_union.double_value = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(value)) {
    long_double_union.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  } else {
    long_double_union.double_value = value;
  }
  return long_double_union.long_value;
}

} /* namespace datasketches */

#include "theta_update_sketch_base_impl.hpp"

#endif
