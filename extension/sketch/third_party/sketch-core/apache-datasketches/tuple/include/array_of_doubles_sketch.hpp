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

#ifndef ARRAY_OF_DOUBLES_SKETCH_HPP_
#define ARRAY_OF_DOUBLES_SKETCH_HPP_

#include <vector>
#include <memory>

#include "serde.hpp"
#include "tuple_sketch.hpp"

namespace datasketches {

// This sketch is equivalent of ArrayOfDoublesSketch in Java

// This simple array of double is faster than std::vector and should be sufficient for this application
template<typename Allocator = std::allocator<double>>
class aod {
public:
  explicit aod(uint8_t size, const Allocator& allocator = Allocator()):
  allocator_(allocator), size_(size), array_(allocator_.allocate(size_)) {
    std::fill(array_, array_ + size_, 0);
  }
  aod(const aod& other):
    allocator_(other.allocator_),
    size_(other.size_),
    array_(allocator_.allocate(size_))
  {
    std::copy(other.array_, other.array_ + size_, array_);
  }
  aod(aod&& other) noexcept:
    allocator_(std::move(other.allocator_)),
    size_(other.size_),
    array_(other.array_)
  {
    other.array_ = nullptr;
  }
  ~aod() {
    if (array_ != nullptr) allocator_.deallocate(array_, size_);
  }
  aod& operator=(const aod& other) {
    aod copy(other);
    std::swap(allocator_, copy.allocator_);
    std::swap(size_, copy.size_);
    std::swap(array_, copy.array_);
    return *this;
  }
  aod& operator=(aod&& other) {
    std::swap(allocator_, other.allocator_);
    std::swap(size_, other.size_);
    std::swap(array_, other.array_);
    return *this;
  }
  double& operator[](size_t index) { return array_[index]; }
  double operator[](size_t index) const { return array_[index]; }
  uint8_t size() const { return size_; }
  double* data() { return array_; }
  const double* data() const { return array_; }
  bool operator==(const aod& other) const {
    for (uint8_t i = 0; i < size_; ++i) if (array_[i] != other.array_[i]) return false;
    return true;
  }
private:
  Allocator allocator_;
  uint8_t size_;
  double* array_;
};

template<typename A = std::allocator<double>>
class array_of_doubles_update_policy {
public:
  array_of_doubles_update_policy(uint8_t num_values = 1, const A& allocator = A()):
    allocator_(allocator), num_values_(num_values) {}
  aod<A> create() const {
    return aod<A>(num_values_, allocator_);
  }
  template<typename InputVector> // to allow any type with indexed access (such as double*)
  void update(aod<A>& summary, const InputVector& update) const {
    for (uint8_t i = 0; i < num_values_; ++i) summary[i] += update[i];
  }
  uint8_t get_num_values() const {
    return num_values_;
  }

private:
  A allocator_;
  uint8_t num_values_;
};

// forward declaration
template<typename A> class compact_array_of_doubles_sketch_alloc;

template<typename A> using AllocAOD = typename std::allocator_traits<A>::template rebind_alloc<aod<A>>;

template<typename A = std::allocator<double>>
class update_array_of_doubles_sketch_alloc: public update_tuple_sketch<aod<A>, aod<A>, array_of_doubles_update_policy<A>, AllocAOD<A>> {
public:
  using Base = update_tuple_sketch<aod<A>, aod<A>, array_of_doubles_update_policy<A>, AllocAOD<A>>;
  using resize_factor = typename Base::resize_factor;

  class builder;

  compact_array_of_doubles_sketch_alloc<A> compact(bool ordered = true) const;
  uint8_t get_num_values() const;

private:
  // for builder
  update_array_of_doubles_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta,
      uint64_t seed, const array_of_doubles_update_policy<A>& policy, const A& allocator);
};

// alias with the default allocator for convenience
using update_array_of_doubles_sketch = update_array_of_doubles_sketch_alloc<>;

template<typename A>
class update_array_of_doubles_sketch_alloc<A>::builder: public tuple_base_builder<builder, array_of_doubles_update_policy<A>, A> {
public:
  builder(const array_of_doubles_update_policy<A>& policy = array_of_doubles_update_policy<A>(), const A& allocator = A());
  update_array_of_doubles_sketch_alloc<A> build() const;
};

template<typename A = std::allocator<double>>
class compact_array_of_doubles_sketch_alloc: public compact_tuple_sketch<aod<A>, AllocAOD<A>> {
public:
  using Base = compact_tuple_sketch<aod<A>, AllocAOD<A>>;
  using Entry = typename Base::Entry;
  using AllocEntry = typename Base::AllocEntry;
  using AllocU64 = typename Base::AllocU64;
  using vector_bytes = typename Base::vector_bytes;

  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t SKETCH_FAMILY = 9;
  static const uint8_t SKETCH_TYPE = 3;
  enum flags { UNUSED1, UNUSED2, IS_EMPTY, HAS_ENTRIES, IS_ORDERED };

  template<typename Sketch>
  compact_array_of_doubles_sketch_alloc(const Sketch& other, bool ordered = true);

  uint8_t get_num_values() const;

  void serialize(std::ostream& os) const;
  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  static compact_array_of_doubles_sketch_alloc deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED, const A& allocator = A());
  static compact_array_of_doubles_sketch_alloc deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED,
      const A& allocator = A());

  // for internal use
  compact_array_of_doubles_sketch_alloc(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<Entry, AllocEntry>&& entries, uint8_t num_values);
  compact_array_of_doubles_sketch_alloc(uint8_t num_values, Base&& base);
private:
  uint8_t num_values_;
};

// alias with the default allocator for convenience
using compact_array_of_doubles_sketch = compact_array_of_doubles_sketch_alloc<>;

} /* namespace datasketches */

#include "array_of_doubles_sketch_impl.hpp"

#endif
