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

#ifndef FREQUENT_ITEMS_SKETCH_HPP_
#define FREQUENT_ITEMS_SKETCH_HPP_

#include <memory>
#include <vector>
#include <iostream>
#include <functional>
#include <type_traits>

#include "reverse_purge_hash_map.hpp"
#include "common_defs.hpp"
#include "serde.hpp"

namespace datasketches {

/*
 * Based on Java implementation here:
 * https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/frequencies/ItemsSketch.java
 * author Alexander Saydakov
 */

enum frequent_items_error_type { NO_FALSE_POSITIVES, NO_FALSE_NEGATIVES };

// type W for weight must be an arithmetic type (integral or floating point)
template<
  typename T,
  typename W = uint64_t,
  typename H = std::hash<T>,
  typename E = std::equal_to<T>,
  typename S = serde<T>, // deprecated, to be removed in the next major version
  typename A = std::allocator<T>
>
class frequent_items_sketch {
public:

  static const uint8_t LG_MIN_MAP_SIZE = 3;

  /**
   * Construct this sketch with parameters lg_max_map_size and lg_start_map_size.
   *
   * @param lg_max_map_size Log2 of the physical size of the internal hash map managed by this
   * sketch. The maximum capacity of this internal hash map is 0.75 times 2^lg_max_map_size.
   * Both the ultimate accuracy and size of this sketch are functions of lg_max_map_size.
   *
   * @param lg_start_map_size Log2 of the starting physical size of the internal hash
   * map managed by this sketch.
   */
  explicit frequent_items_sketch(uint8_t lg_max_map_size, uint8_t lg_start_map_size = LG_MIN_MAP_SIZE, const A& allocator = A());

  /**
   * Update this sketch with an item and a positive weight (frequency count).
   * @param item for which the weight should be increased (lvalue)
   * @param weight the amount by which the weight of the item should be increased
   * A count of zero is a no-op, and a negative count will throw an exception.
   */
  void update(const T& item, W weight = 1);

  /**
   * Update this sketch with an item and a positive weight (frequency count).
   * @param item for which the weight should be increased (rvalue)
   * @param weight the amount by which the weight of the item should be increased
   * A count of zero is a no-op, and a negative count will throw an exception.
   */
  void update(T&& item, W weight = 1);

  /**
   * This function merges the other sketch into this one.
   * The other sketch may be of a different size.
   * @param other sketch to be merged into this (lvalue)
   */
  void merge(const frequent_items_sketch& other);

  /**
   * This function merges the other sketch into this one.
   * The other sketch may be of a different size.
   * @param other sketch to be merged into this (rvalue)
   */
  void merge(frequent_items_sketch&& other);

  /**
   * @return true if this sketch is empty
   */
  bool is_empty() const;

  /**
   * @return the number of active items in the sketch
   */
  uint32_t get_num_active_items() const;

  /**
   * Returns the sum of the weights (frequencies) in the stream seen so far by the sketch
   *
   * @return the total weight of all items in the stream seen so far by the sketch
   */
  W get_total_weight() const;

  /**
   * Returns the estimate of the weight (frequency) of the given item.
   * Note: The true frequency of a item would be the sum of the counts as a result of the
   * two update functions.
   *
   * @param item the given item
   * @return the estimate of the weight (frequency) of the given item
   */
  W get_estimate(const T& item) const;

  /**
   * Returns the guaranteed lower bound weight (frequency) of the given item.
   *
   * @param item the given item.
   * @return the guaranteed lower bound weight of the given item. That is, a number which
   * is guaranteed to be no larger than the real weight.
   */
  W get_lower_bound(const T& item) const;

  /**
   * Returns the guaranteed upper bound weight (frequency) of the given item.
   *
   * @param item the given item
   * @return the guaranteed upper bound weight of the given item. That is, a number which
   * is guaranteed to be no smaller than the real frequency.
   */
  W get_upper_bound(const T& item) const;

  /**
   * @return An upper bound on the maximum error of get_estimate(item) for any item.
   * This is equivalent to the maximum distance between the upper bound and the lower bound
   * for any item.
   */
  W get_maximum_error() const;

  /**
   * Returns epsilon value of this sketch.
   * This is just the value <i>3.5 / max_map_size</i>.
   * @return epsilon used by the sketch to compute error.
   */
  double get_epsilon() const;

  /**
   * Returns epsilon used to compute <i>a priori</i> error.
   * This is just the value <i>3.5 / maxMapSize</i>.
   * @param maxMapSize the planned map size to be used when constructing this sketch.
   * @return epsilon used to compute <i>a priori</i> error.
   */
  static double get_epsilon(uint8_t lg_max_map_size);

  /**
   * Returns the estimated <i>a priori</i> error given the max_map_size for the sketch and the
   * estimated_total_stream_weight.
   * @param lg_max_map_size the planned map size to be used when constructing this sketch.
   * @param estimated_total_stream_weight the estimated total stream weight.
   * @return the estimated <i>a priori</i> error.
   */
  static double get_apriori_error(uint8_t lg_max_map_size, W estimated_total_weight);

  class row;
  typedef typename std::vector<row, typename std::allocator_traits<A>::template rebind_alloc<row>> vector_row; // alias for users

  /**
   * Returns an array of rows that include frequent items, estimates, upper and lower bounds
   * given an error_type and using get_maximum_error() as a threshold.
   *
   * <p>The method first examines all active items in the sketch (items that have a counter).
   *
   * <p>If <i>error_type = NO_FALSE_NEGATIVES</i>, this will include an item in the result
   * list if get_upper_bound(item) &gt; threshold.
   * There will be no false negatives, i.e., no Type II error.
   * There may be items in the set with true frequencies less than the threshold
   * (false positives).</p>
   *
   * <p>If <i>error_type = NO_FALSE_POSITIVES</i>, this will include an item in the result
   * list if get_lower_bound(item) &gt; threshold.
   * There will be no false positives, i.e., no Type I error.
   * There may be items omitted from the set with true frequencies greater than the
   * threshold (false negatives).</p>
   *
   * @param error_type determines whether no false positives or no false negatives are desired.
   * @return an array of frequent items
   */
  vector_row get_frequent_items(frequent_items_error_type err_type) const;

  /**
   * Returns an array of rows that include frequent items, estimates, upper and lower bounds
   * given an error_type and a threshold.
   *
   * <p>The method first examines all active items in the sketch (items that have a counter).
   *
   * <p>If <i>error_type = NO_FALSE_NEGATIVES</i>, this will include an item in the result
   * list if get_upper_bound(item) &gt; threshold.
   * There will be no false negatives, i.e., no Type II error.
   * There may be items in the set with true frequencies less than the threshold
   * (false positives).</p>
   *
   * <p>If <i>error_type = NO_FALSE_POSITIVES</i>, this will include an item in the result
   * list if get_lower_bound(item) &gt; threshold.
   * There will be no false positives, i.e., no Type I error.
   * There may be items omitted from the set with true frequencies greater than the
   * threshold (false negatives).</p>
   *
   * @param error_type determines whether no false positives or no false negatives are desired.
   * @param threshold to include items in the result list
   * @return an array of frequent items
   */
  vector_row get_frequent_items(frequent_items_error_type err_type, W threshold) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This can be expensive since every item needs to be looked at.
   * @param instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename SerDe = S>
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
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<A>::template rebind_alloc<uint8_t>>;

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
   * @param instance of an Allocator
   * @return an instance of the sketch
   *
   * Deprecated, to be removed in the next major version
   */
  static frequent_items_sketch deserialize(std::istream& is, const A& allocator = A());

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param instance of a SerDe
   * @param instance of an Allocator
   * @return an instance of the sketch
   */
  template<typename SerDe = S>
  static frequent_items_sketch deserialize(std::istream& is, const SerDe& sd = SerDe(), const A& allocator = A());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param instance of an Allocator
   * @return an instance of the sketch
   *
   * Deprecated, to be removed in the next major version
   */
  static frequent_items_sketch deserialize(const void* bytes, size_t size, const A& allocator = A());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param instance of a SerDe
   * @param instance of an Allocator
   * @return an instance of the sketch
   */
  template<typename SerDe = S>
  static frequent_items_sketch deserialize(const void* bytes, size_t size, const SerDe& sd = SerDe(), const A& allocator = A());

  /**
   * Returns a human readable summary of this sketch
   * @param print_items if true include the list of items retained by the sketch
   */
  string<A> to_string(bool print_items = false) const;

private:
  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t FAMILY_ID = 10;
  static const uint8_t PREAMBLE_LONGS_EMPTY = 1;
  static const uint8_t PREAMBLE_LONGS_NONEMPTY = 4;
  static constexpr double EPSILON_FACTOR = 3.5;
  enum flags { IS_EMPTY };
  W total_weight;
  W offset;
  reverse_purge_hash_map<T, W, H, E, A> map;
  static void check_preamble_longs(uint8_t preamble_longs, bool is_empty);
  static void check_serial_version(uint8_t serial_version);
  static void check_family_id(uint8_t family_id);
  static void check_size(uint8_t lg_cur_size, uint8_t lg_max_size);

  // version for integral signed type
  template<typename WW = W, typename std::enable_if<std::is_integral<WW>::value && std::is_signed<WW>::value, int>::type = 0>
  static inline void check_weight(WW weight);

  // version for integral unsigned type
  template<typename WW = W, typename std::enable_if<std::is_integral<WW>::value && std::is_unsigned<WW>::value, int>::type = 0>
  static inline void check_weight(WW weight);

  // version for floating point type
  template<typename WW = W, typename std::enable_if<std::is_floating_point<WW>::value, int>::type = 0>
  static inline void check_weight(WW weight);

  // for deserialize
  class items_deleter;
};

template<typename T, typename W, typename H, typename E, typename S, typename A>
class frequent_items_sketch<T, W, H, E, S, A>::row {
public:
  row(const T* item, W weight, W offset):
    item(item), weight(weight), offset(offset) {}
  const T& get_item() const { return *item; }
  W get_estimate() const { return weight + offset; }
  W get_lower_bound() const { return weight; }
  W get_upper_bound() const { return weight + offset; }
private:
  const T* item;
  W weight;
  W offset;
};

}

#include "frequent_items_sketch_impl.hpp"

# endif
