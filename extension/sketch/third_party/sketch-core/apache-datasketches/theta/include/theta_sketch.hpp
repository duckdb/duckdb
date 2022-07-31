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

#ifndef THETA_SKETCH_HPP_
#define THETA_SKETCH_HPP_

#include "theta_update_sketch_base.hpp"

namespace datasketches {

template<typename Allocator = std::allocator<uint64_t>>
class base_theta_sketch_alloc {
public:

  virtual ~base_theta_sketch_alloc() = default;

  /**
   * @return allocator
   */
  virtual Allocator get_allocator() const = 0;

  /**
   * @return true if this sketch represents an empty set (not the same as no retained entries!)
   */
  virtual bool is_empty() const = 0;

  /**
   * @return estimate of the distinct count of the input stream
   */
  double get_estimate() const;

  /**
   * Returns the approximate lower error bound given a number of standard deviations.
   * This parameter is similar to the number of standard deviations of the normal distribution
   * and corresponds to approximately 67%, 95% and 99% confidence intervals.
   * @param num_std_devs number of Standard Deviations (1, 2 or 3)
   * @return the lower bound
   */
  double get_lower_bound(uint8_t num_std_devs) const;

  /**
   * Returns the approximate upper error bound given a number of standard deviations.
   * This parameter is similar to the number of standard deviations of the normal distribution
   * and corresponds to approximately 67%, 95% and 99% confidence intervals.
   * @param num_std_devs number of Standard Deviations (1, 2 or 3)
   * @return the upper bound
   */
  double get_upper_bound(uint8_t num_std_devs) const;

  /**
   * @return true if the sketch is in estimation mode (as opposed to exact mode)
   */
  bool is_estimation_mode() const;

  /**
   * @return theta as a fraction from 0 to 1 (effective sampling rate)
   */
  double get_theta() const;

  /**
   * @return theta as a positive integer between 0 and LLONG_MAX
   */
  virtual uint64_t get_theta64() const = 0;

  /**
   * @return the number of retained entries in the sketch
   */
  virtual uint32_t get_num_retained() const = 0;

  /**
   * @return hash of the seed that was used to hash the input
   */
  virtual uint16_t get_seed_hash() const = 0;

  /**
   * @return true if retained entries are ordered
   */
  virtual bool is_ordered() const = 0;

  /**
   * Provides a human-readable summary of this sketch as a string
   * @param print_items if true include the list of items retained by the sketch
   * @return sketch summary as a string
   */
  virtual string<Allocator> to_string(bool print_items = false) const;

protected:
  virtual void print_specifics(std::ostringstream& os) const = 0;
  virtual void print_items(std::ostringstream& os) const = 0;
};

template<typename Allocator = std::allocator<uint64_t>>
class theta_sketch_alloc: public base_theta_sketch_alloc<Allocator> {
public:
  using Entry = uint64_t;
  using ExtractKey = trivial_extract_key;
  using iterator = theta_iterator<Entry, ExtractKey>;
  using const_iterator = theta_const_iterator<Entry, ExtractKey>;

  virtual ~theta_sketch_alloc() = default;

  /**
   * Iterator over hash values in this sketch.
   * @return begin iterator
   */
  virtual iterator begin() = 0;

  /**
   * Iterator pointing past the valid range.
   * Not to be incremented or dereferenced.
   * @return end iterator
   */
  virtual iterator end() = 0;

  /**
   * Const iterator over hash values in this sketch.
   * @return begin iterator
   */
  virtual const_iterator begin() const = 0;

  /**
   * Const iterator pointing past the valid range.
   * Not to be incremented or dereferenced.
   * @return end iterator
   */
  virtual const_iterator end() const = 0;

protected:
  virtual void print_items(std::ostringstream& os) const;
};

// forward declaration
template<typename A> class compact_theta_sketch_alloc;

template<typename Allocator = std::allocator<uint64_t>>
class update_theta_sketch_alloc: public theta_sketch_alloc<Allocator> {
public:
  using Base = theta_sketch_alloc<Allocator>;
  using Entry = typename Base::Entry;
  using ExtractKey = typename Base::ExtractKey;
  using iterator = typename Base::iterator;
  using const_iterator = typename Base::const_iterator;
  using theta_table = theta_update_sketch_base<Entry, ExtractKey, Allocator>;
  using resize_factor = typename theta_table::resize_factor;

  // No constructor here. Use builder instead.
  class builder;

  update_theta_sketch_alloc(const update_theta_sketch_alloc&) = default;
  update_theta_sketch_alloc(update_theta_sketch_alloc&&) noexcept = default;
  virtual ~update_theta_sketch_alloc() = default;
  update_theta_sketch_alloc& operator=(const update_theta_sketch_alloc&) = default;
  update_theta_sketch_alloc& operator=(update_theta_sketch_alloc&&) = default;

  virtual Allocator get_allocator() const;
  virtual bool is_empty() const;
  virtual bool is_ordered() const;
  virtual uint16_t get_seed_hash() const;
  virtual uint64_t get_theta64() const;
  virtual uint32_t get_num_retained() const;

  /**
   * @return configured nominal number of entries in the sketch
   */
  uint8_t get_lg_k() const;

  /**
   * @return configured resize factor of the sketch
   */
  resize_factor get_rf() const;

  /**
   * Update this sketch with a given string.
   * @param value string to update the sketch with
   */
  void update(const std::string& value);

  /**
   * Update this sketch with a given unsigned 64-bit integer.
   * @param value uint64_t to update the sketch with
   */
  void update(uint64_t value);

  /**
   * Update this sketch with a given signed 64-bit integer.
   * @param value int64_t to update the sketch with
   */
  void update(int64_t value);

  /**
   * Update this sketch with a given unsigned 32-bit integer.
   * For compatibility with Java implementation.
   * @param value uint32_t to update the sketch with
   */
  void update(uint32_t value);

  /**
   * Update this sketch with a given signed 32-bit integer.
   * For compatibility with Java implementation.
   * @param value int32_t to update the sketch with
   */
  void update(int32_t value);

  /**
   * Update this sketch with a given unsigned 16-bit integer.
   * For compatibility with Java implementation.
   * @param value uint16_t to update the sketch with
   */
  void update(uint16_t value);

  /**
   * Update this sketch with a given signed 16-bit integer.
   * For compatibility with Java implementation.
   * @param value int16_t to update the sketch with
   */
  void update(int16_t value);

  /**
   * Update this sketch with a given unsigned 8-bit integer.
   * For compatibility with Java implementation.
   * @param value uint8_t to update the sketch with
   */
  void update(uint8_t value);

  /**
   * Update this sketch with a given signed 8-bit integer.
   * For compatibility with Java implementation.
   * @param value int8_t to update the sketch with
   */
  void update(int8_t value);

  /**
   * Update this sketch with a given double-precision floating point value.
   * For compatibility with Java implementation.
   * @param value double to update the sketch with
   */
  void update(double value);

  /**
   * Update this sketch with a given floating point value.
   * For compatibility with Java implementation.
   * @param value float to update the sketch with
   */
  void update(float value);

  /**
   * Update this sketch with given data of any type.
   * This is a "universal" update that covers all cases above,
   * but may produce different hashes.
   * Be very careful to hash input values consistently using the same approach
   * both over time and on different platforms
   * and while passing sketches between C++ environment and Java environment.
   * Otherwise two sketches that should represent overlapping sets will be disjoint
   * For instance, for signed 32-bit values call update(int32_t) method above,
   * which does widening conversion to int64_t, if compatibility with Java is expected
   * @param data pointer to the data
   * @param length of the data in bytes
   */
  void update(const void* data, size_t length);

  /**
   * Remove retained entries in excess of the nominal size k (if any)
   */
  void trim();

  /**
   * Reset the sketch to the initial empty state
   */
  void reset();

  /**
   * Converts this sketch to a compact sketch (ordered or unordered).
   * @param ordered optional flag to specify if ordered sketch should be produced
   * @return compact sketch
   */
  compact_theta_sketch_alloc<Allocator> compact(bool ordered = true) const;

  virtual iterator begin();
  virtual iterator end();
  virtual const_iterator begin() const;
  virtual const_iterator end() const;

private:
  theta_table table_;

  // for builder
  update_theta_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p,
      uint64_t theta, uint64_t seed, const Allocator& allocator);

  virtual void print_specifics(std::ostringstream& os) const;
};

// compact sketch

template<typename Allocator = std::allocator<uint64_t>>
class compact_theta_sketch_alloc: public theta_sketch_alloc<Allocator> {
public:
  using Base = theta_sketch_alloc<Allocator>;
  using iterator = typename Base::iterator;
  using const_iterator = typename Base::const_iterator;
  using AllocBytes = typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>;
  using vector_bytes = std::vector<uint8_t, AllocBytes>;

  static const uint8_t SERIAL_VERSION = 3;
  static const uint8_t SKETCH_TYPE = 3;

  // Instances of this type can be obtained:
  // - by compacting an update_theta_sketch_alloc
  // - as a result of a set operation
  // - by deserializing a previously serialized compact sketch

  template<typename Other>
  compact_theta_sketch_alloc(const Other& other, bool ordered);
  compact_theta_sketch_alloc(const compact_theta_sketch_alloc&) = default;
  compact_theta_sketch_alloc(compact_theta_sketch_alloc&&) noexcept = default;
  virtual ~compact_theta_sketch_alloc() = default;
  compact_theta_sketch_alloc& operator=(const compact_theta_sketch_alloc&) = default;
  compact_theta_sketch_alloc& operator=(compact_theta_sketch_alloc&&) = default;

  virtual Allocator get_allocator() const;
  virtual bool is_empty() const;
  virtual bool is_ordered() const;
  virtual uint64_t get_theta64() const;
  virtual uint32_t get_num_retained() const;
  virtual uint16_t get_seed_hash() const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is an uninitialized space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   */
  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  virtual iterator begin();
  virtual iterator end();
  virtual const_iterator begin() const;
  virtual const_iterator end() const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of the sketch
   */
  static compact_theta_sketch_alloc deserialize(std::istream& is,
      uint64_t seed = DEFAULT_SEED, const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of the sketch
   */
  static compact_theta_sketch_alloc deserialize(const void* bytes, size_t size,
      uint64_t seed = DEFAULT_SEED, const Allocator& allocator = Allocator());

  // for internal use
  compact_theta_sketch_alloc(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<uint64_t, Allocator>&& entries);

private:
  enum flags { IS_BIG_ENDIAN, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED };

  bool is_empty_;
  bool is_ordered_;
  uint16_t seed_hash_;
  uint64_t theta_;
  std::vector<uint64_t, Allocator> entries_;

  virtual void print_specifics(std::ostringstream& os) const;
};

template<typename Allocator>
class update_theta_sketch_alloc<Allocator>::builder: public theta_base_builder<builder, Allocator> {
public:
    builder(const Allocator& allocator = Allocator());
    update_theta_sketch_alloc build() const;
};

// This is to wrap a buffer containing a serialized compact sketch and use it in a set operation avoiding some cost of deserialization.
// It does not take the ownership of the buffer.

template<typename Allocator = std::allocator<uint64_t>>
class wrapped_compact_theta_sketch_alloc : public base_theta_sketch_alloc<Allocator> {
public:
  using const_iterator = const uint64_t*;

  Allocator get_allocator() const;
  bool is_empty() const;
  bool is_ordered() const;
  uint64_t get_theta64() const;
  uint32_t get_num_retained() const;
  uint16_t get_seed_hash() const;

  const_iterator begin() const;
  const_iterator end() const;

  /**
   * This method wraps a serialized compact sketch as an array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the sketch
   * @return an instance of the sketch
   */
  static const wrapped_compact_theta_sketch_alloc wrap(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED, bool dump_on_error = false);

protected:
  virtual void print_specifics(std::ostringstream& os) const;
  virtual void print_items(std::ostringstream& os) const;

private:
  bool is_empty_;
  bool is_ordered_;
  uint16_t seed_hash_;
  uint32_t num_entries_;
  uint64_t theta_;
  const uint64_t* entries_;

  wrapped_compact_theta_sketch_alloc(bool is_empty, bool is_ordered, uint16_t seed_hash, uint32_t num_entries,
      uint64_t theta, const uint64_t* entries);
};

// aliases with default allocator for convenience
using theta_sketch = theta_sketch_alloc<std::allocator<uint64_t>>;
using update_theta_sketch = update_theta_sketch_alloc<std::allocator<uint64_t>>;
using compact_theta_sketch = compact_theta_sketch_alloc<std::allocator<uint64_t>>;
using wrapped_compact_theta_sketch = wrapped_compact_theta_sketch_alloc<std::allocator<uint64_t>>;

} /* namespace datasketches */

#include "theta_sketch_impl.hpp"

#endif
