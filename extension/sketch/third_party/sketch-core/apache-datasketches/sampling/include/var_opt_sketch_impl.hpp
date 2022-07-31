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

#ifndef _VAR_OPT_SKETCH_IMPL_HPP_
#define _VAR_OPT_SKETCH_IMPL_HPP_

#include <memory>
#include <sstream>
#include <cmath>
#include <random>
#include <algorithm>
#include <stdexcept>

#include "var_opt_sketch.hpp"
#include "serde.hpp"
#include "bounds_binomial_proportions.hpp"
#include "count_zeros.hpp"
#include "memory_operations.hpp"
#include "ceiling_power_of_2.hpp"

namespace datasketches {

/**
 * Implementation code for the VarOpt sketch.
 * 
 * author Kevin Lang 
 * author Jon Malkin
 */
template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(uint32_t k, resize_factor rf, const A& allocator) :
  var_opt_sketch<T,S,A>(k, rf, false, allocator) {}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(const var_opt_sketch& other) :
  k_(other.k_),
  h_(other.h_),
  m_(other.m_),
  r_(other.r_),
  n_(other.n_),
  total_wt_r_(other.total_wt_r_),
  rf_(other.rf_),
  curr_items_alloc_(other.curr_items_alloc_),
  filled_data_(other.filled_data_),
  allocator_(other.allocator_),
  data_(nullptr),
  weights_(nullptr),
  num_marks_in_h_(other.num_marks_in_h_),
  marks_(nullptr)
  {
    data_ = allocator_.allocate(curr_items_alloc_);
    // skip gap or anything unused at the end
    for (size_t i = 0; i < h_; ++i)
      new (&data_[i]) T(other.data_[i]);
    for (size_t i = h_ + 1; i < h_ + r_ + 1; ++i)
      new (&data_[i]) T(other.data_[i]);

    // we skipped the gap
    filled_data_ = false;

    weights_ = AllocDouble(allocator_).allocate(curr_items_alloc_);
    // doubles so can successfully copy regardless of the internal state
    std::copy(other.weights_, other.weights_ + curr_items_alloc_, weights_);

    if (other.marks_ != nullptr) {
      marks_ = AllocBool(allocator_).allocate(curr_items_alloc_);
      std::copy(other.marks_, other.marks_ + curr_items_alloc_, marks_);
    }
  }

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(const var_opt_sketch& other, bool as_sketch, uint64_t adjusted_n) :
  k_(other.k_),
  h_(other.h_),
  m_(other.m_),
  r_(other.r_),
  n_(adjusted_n),
  total_wt_r_(other.total_wt_r_),
  rf_(other.rf_),
  curr_items_alloc_(other.curr_items_alloc_),
  filled_data_(other.filled_data_),
  allocator_(other.allocator_),
  data_(nullptr),
  weights_(nullptr),
  num_marks_in_h_(other.num_marks_in_h_),
  marks_(nullptr)
  {
    data_ = allocator_.allocate(curr_items_alloc_);
    // skip gap or anything unused at the end
    for (size_t i = 0; i < h_; ++i)
      new (&data_[i]) T(other.data_[i]);
    for (size_t i = h_ + 1; i < h_ + r_ + 1; ++i)
      new (&data_[i]) T(other.data_[i]);
    
    // we skipped the gap
    filled_data_ = false;

    weights_ = AllocDouble(allocator_).allocate(curr_items_alloc_);
    // doubles so can successfully copy regardless of the internal state
    std::copy(other.weights_, other.weights_ + curr_items_alloc_, weights_);

    if (!as_sketch && other.marks_ != nullptr) {
      marks_ = AllocBool(allocator_).allocate(curr_items_alloc_);
      std::copy(other.marks_, other.marks_ + curr_items_alloc_, marks_);
    }
  }

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(T* data, double* weights, size_t len,
    uint32_t k, uint64_t n, uint32_t h_count, uint32_t r_count, double total_wt_r, const A& allocator) :
  k_(k),
  h_(h_count),
  m_(0),
  r_(r_count),
  n_(n),
  total_wt_r_(total_wt_r),
  rf_(var_opt_constants::DEFAULT_RESIZE_FACTOR),
  curr_items_alloc_(len),
  filled_data_(n > k),
  allocator_(allocator),
  data_(data),
  weights_(weights),
  num_marks_in_h_(0),
  marks_(nullptr)
  {}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(var_opt_sketch&& other) noexcept :
  k_(other.k_),
  h_(other.h_),
  m_(other.m_),
  r_(other.r_),
  n_(other.n_),
  total_wt_r_(other.total_wt_r_),
  rf_(other.rf_),
  curr_items_alloc_(other.curr_items_alloc_),
  filled_data_(other.filled_data_),
  allocator_(other.allocator_),
  data_(other.data_),
  weights_(other.weights_),
  num_marks_in_h_(other.num_marks_in_h_),
  marks_(other.marks_)
  {
    other.data_ = nullptr;
    other.weights_ = nullptr;
    other.marks_ = nullptr;
  }

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget, const A& allocator) :
  k_(k), h_(0), m_(0), r_(0), n_(0), total_wt_r_(0.0), rf_(rf), allocator_(allocator) {
  if (k == 0 || k_ > MAX_K) {
    throw std::invalid_argument("k must be at least 1 and less than 2^31 - 1");
  }

  uint32_t ceiling_lg_k = to_log_2(ceiling_power_of_2(k_));
  uint32_t initial_lg_size = starting_sub_multiple(ceiling_lg_k, rf_, MIN_LG_ARR_ITEMS);
  curr_items_alloc_ = get_adjusted_size(k_, 1 << initial_lg_size);
  if (curr_items_alloc_ == k_) { // if full size, need to leave 1 for the gap
    ++curr_items_alloc_;
  }

  allocate_data_arrays(curr_items_alloc_, is_gadget);
  num_marks_in_h_ = 0;
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::var_opt_sketch(uint32_t k, uint32_t h, uint32_t m, uint32_t r, uint64_t n, double total_wt_r, resize_factor rf,
                                      uint32_t curr_items_alloc, bool filled_data, std::unique_ptr<T, items_deleter> items,
                                      std::unique_ptr<double, weights_deleter> weights, uint32_t num_marks_in_h,
                                      std::unique_ptr<bool, marks_deleter> marks, const A& allocator) :
  k_(k),
  h_(h),
  m_(m),
  r_(r),
  n_(n),
  total_wt_r_(total_wt_r),
  rf_(rf),
  curr_items_alloc_(curr_items_alloc),
  filled_data_(filled_data),
  allocator_(allocator),
  data_(items.release()),
  weights_(weights.release()),
  num_marks_in_h_(num_marks_in_h),
  marks_(marks.release())
{}


template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::~var_opt_sketch() {
  if (data_ != nullptr) {
    if (filled_data_) {
      // destroy everything
      const size_t num_to_destroy = std::min(k_ + 1, curr_items_alloc_);
      for (size_t i = 0; i < num_to_destroy; ++i) {
        allocator_.destroy(data_ + i);
      }
    } else {
      // skip gap or anything unused at the end
      for (size_t i = 0; i < h_; ++i) {
        allocator_.destroy(data_+ i);
      }
    
      for (size_t i = h_ + 1; i < h_ + r_ + 1; ++i) {
        allocator_.destroy(data_ + i);
      }
    }
    allocator_.deallocate(data_, curr_items_alloc_);
  }

  if (weights_ != nullptr) {
    AllocDouble(allocator_).deallocate(weights_, curr_items_alloc_);
  }
  
  if (marks_ != nullptr) {
    AllocBool(allocator_).deallocate(marks_, curr_items_alloc_);
  }
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>& var_opt_sketch<T,S,A>::operator=(const var_opt_sketch& other) {
  var_opt_sketch<T,S,A> sk_copy(other);
  std::swap(k_, sk_copy.k_);
  std::swap(h_, sk_copy.h_);
  std::swap(m_, sk_copy.m_);
  std::swap(r_, sk_copy.r_);
  std::swap(n_, sk_copy.n_);
  std::swap(total_wt_r_, sk_copy.total_wt_r_);
  std::swap(rf_, sk_copy.rf_);
  std::swap(curr_items_alloc_, sk_copy.curr_items_alloc_);
  std::swap(filled_data_, sk_copy.filled_data_);
  std::swap(allocator_, sk_copy.allocator_);
  std::swap(data_, sk_copy.data_);
  std::swap(weights_, sk_copy.weights_);
  std::swap(num_marks_in_h_, sk_copy.num_marks_in_h_);
  std::swap(marks_, sk_copy.marks_);
  return *this;
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>& var_opt_sketch<T,S,A>::operator=(var_opt_sketch&& other) {
  std::swap(k_, other.k_);
  std::swap(h_, other.h_);
  std::swap(m_, other.m_);
  std::swap(r_, other.r_);
  std::swap(n_, other.n_);
  std::swap(total_wt_r_, other.total_wt_r_);
  std::swap(rf_, other.rf_);
  std::swap(curr_items_alloc_, other.curr_items_alloc_);
  std::swap(filled_data_, other.filled_data_);
  std::swap(allocator_, other.allocator_);
  std::swap(data_, other.data_);
  std::swap(weights_, other.weights_);
  std::swap(num_marks_in_h_, other.num_marks_in_h_);
  std::swap(marks_, other.marks_);
  return *this;
}

/*
 * An empty sketch requires 8 bytes.
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 * </pre>
 *
 * A non-empty sketch requires 24 bytes of preamble for an under-full sample; once there are
 * at least k items the sketch uses 32 bytes of preamble.
 *
 * The count of items seen is limited to 48 bits (~256 trillion) even though there are adjacent
 * unused preamble bits. The acceptance probability for an item is a double in the range [0,1),
 * limiting us to 53 bits of randomness due to details of the IEEE floating point format. To
 * ensure meaningful probabilities as the items seen count approaches capacity, we intentionally
 * use slightly fewer bits.
 * 
 * Following the header are weights for the heavy items, then marks in the event this is a gadget.
 * The serialized items come last.
 * 
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||---------------------------Items Seen Count (N)--------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||-------------Item Count in H---------------|-------Item Count in R-------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||-------------------------------Total Weight in R-------------------------------|
 * </pre>
 */

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename S, typename A>
template<typename TT, typename SerDe, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t var_opt_sketch<T,S,A>::get_serialized_size_bytes(const SerDe&) const {
  if (is_empty()) { return PREAMBLE_LONGS_EMPTY << 3; }
  size_t num_bytes = (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL) << 3;
  num_bytes += h_ * sizeof(double);    // weights
  if (marks_ != nullptr) {             // marks
    num_bytes += (h_ / 8) + (h_ % 8 > 0);
  }
  num_bytes += (h_ + r_) * sizeof(TT); // the actual items
  return num_bytes;
}

// implementation for all other types
template<typename T, typename S, typename A>
template<typename TT, typename SerDe, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t var_opt_sketch<T,S,A>::get_serialized_size_bytes(const SerDe& sd) const {
  if (is_empty()) { return PREAMBLE_LONGS_EMPTY << 3; }
  size_t num_bytes = (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL) << 3;
  num_bytes += h_ * sizeof(double);    // weights
  if (marks_ != nullptr) {             // marks
    num_bytes += (h_ / 8) + (h_ % 8 > 0);
  }
  // must iterate over the items
  for (auto it: *this)
    num_bytes += sd.size_of_item(it.first);
  return num_bytes;
}

template<typename T, typename S, typename A>
template<typename SerDe>
std::vector<uint8_t, AllocU8<A>> var_opt_sketch<T,S,A>::serialize(unsigned header_size_bytes, const SerDe& sd) const {
  const size_t size = header_size_bytes + get_serialized_size_bytes(sd);
  std::vector<uint8_t, AllocU8<A>> bytes(size, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;
  uint8_t* end_ptr = ptr + size;

  bool empty = is_empty();
  uint8_t preLongs = (empty ? PREAMBLE_LONGS_EMPTY
                                  : (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL));
  uint8_t first_byte = (preLongs & 0x3F) | ((static_cast<uint8_t>(rf_)) << 6);
  uint8_t flags = (marks_ != nullptr ? GADGET_FLAG_MASK : 0);

  if (empty) {
    flags |= EMPTY_FLAG_MASK;
  }

  // first prelong
  uint8_t ser_ver(SER_VER);
  uint8_t family(FAMILY_ID);
  ptr += copy_to_mem(first_byte, ptr);
  ptr += copy_to_mem(ser_ver, ptr);
  ptr += copy_to_mem(family, ptr);
  ptr += copy_to_mem(flags, ptr);
  ptr += copy_to_mem(k_, ptr);

  if (!empty) {
    // second and third prelongs
    ptr += copy_to_mem(n_, ptr);
    ptr += copy_to_mem(h_, ptr);
    ptr += copy_to_mem(r_, ptr);

    // fourth prelong, if needed
    if (r_ > 0) {
      ptr += copy_to_mem(total_wt_r_, ptr);
    }

    // first h_ weights
    ptr += copy_to_mem(weights_, ptr, h_ * sizeof(double));

    // first h_ marks as packed bytes iff we have a gadget
    if (marks_ != nullptr) {
      uint8_t val = 0;
      for (uint32_t i = 0; i < h_; ++i) {
        if (marks_[i]) {
          val |= 0x1 << (i & 0x7);
        }

        if ((i & 0x7) == 0x7) {
          ptr += copy_to_mem(val, ptr);
          val = 0;
        }
      }

      // write out any remaining values
      if ((h_ & 0x7) > 0) {
        ptr += copy_to_mem(val, ptr);
      }
    }

    // write the sample items, skipping the gap. Either h_ or r_ may be 0
    ptr += sd.serialize(ptr, end_ptr - ptr, data_, h_);
    ptr += sd.serialize(ptr, end_ptr - ptr, &data_[h_ + 1], r_);
  }
  
  size_t bytes_written = ptr - bytes.data();
  if (bytes_written != size) {
    throw std::logic_error("serialized size mismatch: " + std::to_string(bytes_written) + " != " + std::to_string(size));
  }

  return bytes;
}

template<typename T, typename S, typename A>
template<typename SerDe>
void var_opt_sketch<T,S,A>::serialize(std::ostream& os, const SerDe& sd) const {
  const bool empty = (h_ == 0) && (r_ == 0);

  const uint8_t preLongs = (empty ? PREAMBLE_LONGS_EMPTY
                                  : (r_ == 0 ? PREAMBLE_LONGS_WARMUP : PREAMBLE_LONGS_FULL));
  const uint8_t first_byte = (preLongs & 0x3F) | ((static_cast<uint8_t>(rf_)) << 6);
  uint8_t flags = (marks_ != nullptr ? GADGET_FLAG_MASK : 0);

  if (empty) {
    flags |= EMPTY_FLAG_MASK;
  }

  // first prelong
  const uint8_t ser_ver(SER_VER);
  const uint8_t family(FAMILY_ID);
  write(os, first_byte);
  write(os, ser_ver);
  write(os, family);
  write(os, flags);
  write(os, k_);

  if (!empty) {
    // second and third prelongs
    write(os, n_);
    write(os, h_);
    write(os, r_);
    
    // fourth prelong, if needed
    if (r_ > 0) {
      write(os, total_wt_r_);
    }

    // write the first h_ weights
    write(os, weights_, h_ * sizeof(double));

    // write the first h_ marks as packed bytes iff we have a gadget
    if (marks_ != nullptr) {
      uint8_t val = 0;
      for (uint32_t i = 0; i < h_; ++i) {
        if (marks_[i]) {
          val |= 0x1 << (i & 0x7);
        }

        if ((i & 0x7) == 0x7) {
          write(os, val);
          val = 0;
        }
      }

      // write out any remaining values
      if ((h_ & 0x7) > 0) {
        write(os, val);
      }
    }

    // write the sample items, skipping the gap. Either h_ or r_ may be 0
    sd.serialize(os, data_, h_);
    sd.serialize(os, &data_[h_ + 1], r_);
  }
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A> var_opt_sketch<T,S,A>::deserialize(const void* bytes, size_t size, const A& allocator) {
  return deserialize(bytes, size, S(), allocator);
}

template<typename T, typename S, typename A>
template<typename SerDe>
var_opt_sketch<T,S,A> var_opt_sketch<T,S,A>::deserialize(const void* bytes, size_t size, const SerDe& sd, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* base = ptr;
  const char* end_ptr = ptr + size;
  uint8_t first_byte;
  ptr += copy_from_mem(ptr, first_byte);
  uint8_t preamble_longs = first_byte & 0x3f;
  resize_factor rf = static_cast<resize_factor>((first_byte >> 6) & 0x03);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t flags;
  ptr += copy_from_mem(ptr, flags);
  uint32_t k;
  ptr += copy_from_mem(ptr, k);

  check_preamble_longs(preamble_longs, flags);
  check_family_and_serialization_version(family_id, serial_version);
  ensure_minimum_memory(size, preamble_longs << 3);

  const bool is_empty = flags & EMPTY_FLAG_MASK;
  const bool is_gadget = flags & GADGET_FLAG_MASK;

  if (is_empty) {
    return var_opt_sketch<T,S,A>(k, rf, is_gadget, allocator);
  }

  // second and third prelongs
  uint64_t n;
  uint32_t h, r;
  ptr += copy_from_mem(ptr, n);
  ptr += copy_from_mem(ptr, h);
  ptr += copy_from_mem(ptr, r);

  const uint32_t array_size = validate_and_get_target_size(preamble_longs, k, n, h, r, rf);
  
  // current_items_alloc_ is set but validate R region weight (4th prelong), if needed, before allocating
  double total_wt_r = 0.0;
  if (preamble_longs == PREAMBLE_LONGS_FULL) {
    ptr += copy_from_mem(ptr, total_wt_r);
    if (std::isnan(total_wt_r) || r == 0 || total_wt_r <= 0.0) {
      throw std::invalid_argument("Possible corruption: deserializing in full mode but r = 0 or invalid R weight. "
       "Found r = " + std::to_string(r) + ", R region weight = " + std::to_string(total_wt_r));
    }
  } else {
    total_wt_r = 0.0;
  }

  // read the first h_ weights, fill in rest of array with -1.0
  check_memory_size(ptr - base + (h * sizeof(double)), size);
  std::unique_ptr<double, weights_deleter> weights(AllocDouble(allocator).allocate(array_size),
      weights_deleter(array_size, allocator));
  double* wts = weights.get(); // to avoid lots of .get() calls -- do not delete
  ptr += copy_from_mem(ptr, wts, h * sizeof(double));
  for (size_t i = 0; i < h; ++i) {
    if (!(wts[i] > 0.0)) {
      throw std::invalid_argument("Possible corruption: Non-positive weight when deserializing: " + std::to_string(wts[i]));
    }
  }
  std::fill(wts + h, wts + array_size, -1.0);
  
  // read the first h_ marks as packed bytes iff we have a gadget
  uint32_t num_marks_in_h = 0;
  std::unique_ptr<bool, marks_deleter> marks(nullptr, marks_deleter(array_size, allocator));
  if (is_gadget) {
    uint8_t val = 0;
    marks = std::unique_ptr<bool, marks_deleter>(AllocBool(allocator).allocate(array_size), marks_deleter(array_size, allocator));
    const size_t size_marks = (h / 8) + (h % 8 > 0 ? 1 : 0);
    check_memory_size(ptr - base + size_marks, size);
    for (uint32_t i = 0; i < h; ++i) {
     if ((i & 0x7) == 0x0) { // should trigger on first iteration
        ptr += copy_from_mem(ptr, val);
      }
      marks.get()[i] = ((val >> (i & 0x7)) & 0x1) == 1;
      num_marks_in_h += (marks.get()[i] ? 1 : 0);
    }
  }

  // read the sample items, skipping the gap. Either h_ or r_ may be 0
  items_deleter deleter(array_size, allocator);
  std::unique_ptr<T, items_deleter> items(A(allocator).allocate(array_size), deleter);
  
  ptr += sd.deserialize(ptr, end_ptr - ptr, items.get(), h);
  items.get_deleter().set_h(h); // serde didn't throw, so the items are now valid
  
  ptr += sd.deserialize(ptr, end_ptr - ptr, &(items.get()[h + 1]), r);
  items.get_deleter().set_r(r); // serde didn't throw, so the items are now valid

  return var_opt_sketch(k, h, (r > 0 ? 1 : 0), r, n, total_wt_r, rf, array_size, false,
                        std::move(items), std::move(weights), num_marks_in_h, std::move(marks), allocator);
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A> var_opt_sketch<T,S,A>::deserialize(std::istream& is, const A& allocator) {
  return deserialize(is, S(), allocator);
}

template<typename T, typename S, typename A>
template<typename SerDe>
var_opt_sketch<T,S,A> var_opt_sketch<T,S,A>::deserialize(std::istream& is, const SerDe& sd, const A& allocator) {
  const auto first_byte = read<uint8_t>(is);
  uint8_t preamble_longs = first_byte & 0x3f;
  const resize_factor rf = static_cast<resize_factor>((first_byte >> 6) & 0x03);
  const auto serial_version = read<uint8_t>(is);
  const auto family_id = read<uint8_t>(is);
  const auto flags = read<uint8_t>(is);
  const auto k = read<uint32_t>(is);

  check_preamble_longs(preamble_longs, flags);
  check_family_and_serialization_version(family_id, serial_version);

  const bool is_empty = flags & EMPTY_FLAG_MASK;
  const bool is_gadget = flags & GADGET_FLAG_MASK;

  if (is_empty) {
    if (!is.good())
      throw std::runtime_error("error reading from std::istream"); 
    else
      return var_opt_sketch<T,S,A>(k, rf, is_gadget, allocator);
  }

  // second and third prelongs
  const auto n = read<uint64_t>(is);
  const auto h = read<uint32_t>(is);
  const auto r = read<uint32_t>(is);

  const uint32_t array_size = validate_and_get_target_size(preamble_longs, k, n, h, r, rf);

  // current_items_alloc_ is set but validate R region weight (4th prelong), if needed, before allocating
  double total_wt_r = 0.0;
  if (preamble_longs == PREAMBLE_LONGS_FULL) { 
    total_wt_r = read<double>(is);
    if (std::isnan(total_wt_r) || r == 0 || total_wt_r <= 0.0) {
      throw std::invalid_argument("Possible corruption: deserializing in full mode but r = 0 or invalid R weight. "
       "Found r = " + std::to_string(r) + ", R region weight = " + std::to_string(total_wt_r));
    }
  }

  // read the first h weights, fill remainder with -1.0
  std::unique_ptr<double, weights_deleter> weights(AllocDouble(allocator).allocate(array_size),
      weights_deleter(array_size, allocator));
  double* wts = weights.get(); // to avoid lots of .get() calls -- do not delete
  read(is, wts, h * sizeof(double));
  for (size_t i = 0; i < h; ++i) {
    if (!(wts[i] > 0.0)) {
      throw std::invalid_argument("Possible corruption: Non-positive weight when deserializing: " + std::to_string(wts[i]));
    }
  }
  std::fill(wts + h, wts + array_size, -1.0);

  // read the first h_ marks as packed bytes iff we have a gadget
  uint32_t num_marks_in_h = 0;
  std::unique_ptr<bool, marks_deleter> marks(nullptr, marks_deleter(array_size, allocator));
  if (is_gadget) {
    marks = std::unique_ptr<bool, marks_deleter>(AllocBool(allocator).allocate(array_size), marks_deleter(array_size, allocator));
    uint8_t val = 0;
    for (uint32_t i = 0; i < h; ++i) {
      if ((i & 0x7) == 0x0) { // should trigger on first iteration
        val = read<uint8_t>(is);
      }
      marks.get()[i] = ((val >> (i & 0x7)) & 0x1) == 1;
      num_marks_in_h += (marks.get()[i] ? 1 : 0);
    }
  }

  // read the sample items, skipping the gap. Either h or r may be 0
  items_deleter deleter(array_size, allocator);
  std::unique_ptr<T, items_deleter> items(A(allocator).allocate(array_size), deleter);

  sd.deserialize(is, items.get(), h); // aka &data_[0]
  items.get_deleter().set_h(h); // serde didn't throw, so the items are now valid

  sd.deserialize(is, &(items.get()[h + 1]), r);
  items.get_deleter().set_r(r); // serde didn't throw, so the items are now valid

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return var_opt_sketch(k, h, (r > 0 ? 1 : 0), r, n, total_wt_r, rf, array_size, false,
                        std::move(items), std::move(weights), num_marks_in_h, std::move(marks), allocator);
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T,S,A>::is_empty() const {
  return (h_ == 0 && r_ == 0);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::reset() {
  const uint32_t prev_alloc = curr_items_alloc_;
  const uint32_t ceiling_lg_k = to_log_2(ceiling_power_of_2(k_));
  const uint32_t initial_lg_size = starting_sub_multiple(ceiling_lg_k, rf_, MIN_LG_ARR_ITEMS);
  curr_items_alloc_ = get_adjusted_size(k_, 1 << initial_lg_size);
  if (curr_items_alloc_ == k_) { // if full size, need to leave 1 for the gap
    ++curr_items_alloc_;
  }

  if (filled_data_) {
    // destroy everything
    const size_t num_to_destroy = std::min(k_ + 1, prev_alloc);
    for (size_t i = 0; i < num_to_destroy; ++i) 
      allocator_.destroy(data_ + i);
  } else {
    // skip gap or anything unused at the end
    for (size_t i = 0; i < h_; ++i)
      allocator_.destroy(data_+ i);
    
    for (size_t i = h_ + 1; i < h_ + r_ + 1; ++i)
      allocator_.destroy(data_ + i);
  }

  if (curr_items_alloc_ < prev_alloc) {
    const bool is_gadget = (marks_ != nullptr);
  
    allocator_.deallocate(data_, prev_alloc);
    AllocDouble(allocator_).deallocate(weights_, prev_alloc);
  
    if (marks_ != nullptr)
      AllocBool(allocator_).deallocate(marks_, prev_alloc);

    allocate_data_arrays(curr_items_alloc_, is_gadget);
  }
  
  n_ = 0;
  h_ = 0;
  m_ = 0;
  r_ = 0;
  num_marks_in_h_ = 0;
  total_wt_r_ = 0.0;
  filled_data_ = false;
}

template<typename T, typename S, typename A>
uint64_t var_opt_sketch<T,S,A>::get_n() const {
  return n_;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::get_k() const {
  return k_;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::get_num_samples() const {
  const uint32_t num_in_sketch = h_ + r_;
  return (num_in_sketch < k_ ? num_in_sketch : k_);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update(const T& item, double weight) {
  update(item, weight, false);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::update(T&& item, double weight) {
  update(std::move(item), weight, false);
}

template<typename T, typename S, typename A>
string<A> var_opt_sketch<T,S,A>::to_string() const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### VarOpt SUMMARY:" << std::endl;
  os << "   k            : " << k_ << std::endl;
  os << "   h            : " << h_ << std::endl;
  os << "   r            : " << r_ << std::endl;
  os << "   weight_r     : " << total_wt_r_ << std::endl;
  os << "   Current size : " << curr_items_alloc_ << std::endl;
  os << "   Resize factor: " << (1 << rf_) << std::endl;
  os << "### END SKETCH SUMMARY" << std::endl;
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename S, typename A>
string<A> var_opt_sketch<T,S,A>::items_to_string() const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Sketch Items" << std::endl;
  int idx = 0;
  for (auto record : *this) {
    os << idx << ": " << record.first << "\twt = " << record.second << std::endl;
    ++idx;
  }
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename S, typename A>
string<A> var_opt_sketch<T,S,A>::items_to_string(bool print_gap) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Sketch Items" << std::endl;
  const uint32_t array_length = (n_ < k_ ? n_ : k_ + 1);
  for (uint32_t i = 0, display_idx = 0; i < array_length; ++i) {
    if (i == h_ && print_gap) {
      os << i << ": GAP" << std::endl;
      ++display_idx;
    } else {
      os << i << ": " << data_[i] << "\twt = ";
      if (weights_[i] == -1.0) {
        os << get_tau() << "\t(-1.0)" << std::endl;
      } else {
        os << weights_[i] << std::endl;
      }
      ++display_idx;
    }
  }
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename S, typename A>
template<typename O>
void var_opt_sketch<T,S,A>::update(O&& item, double weight, bool mark) {
  if (weight < 0.0 || std::isnan(weight) || std::isinf(weight)) {
    throw std::invalid_argument("Item weights must be nonnegative and finite. Found: "
                                + std::to_string(weight));
  } else if (weight == 0.0) {
    return;
  }
  ++n_;

  if (r_ == 0) {
    // exact mode
    update_warmup_phase(std::forward<O>(item), weight, mark);
  } else {
    // sketch is in estimation mode so we can make the following check,
    // although very conservative to check every time
    if ((h_ != 0) && (peek_min() < get_tau()))
      throw std::logic_error("sketch not in valid estimation mode");

    // what tau would be if deletion candidates turn out to be R plus the new item
    // note: (r_ + 1) - 1 is intentional
    const double hypothetical_tau = (weight + total_wt_r_) / ((r_ + 1) - 1);

    // is new item's turn to be considered for reservoir?
    const double condition1 = (h_ == 0) || (weight <= peek_min());

    // is new item light enough for reservoir?
    const double condition2 = weight < hypothetical_tau;
  
    if (condition1 && condition2) {
      update_light(std::forward<O>(item), weight, mark);
    } else if (r_ == 1) {
      update_heavy_r_eq1(std::forward<O>(item), weight, mark);
    } else {
      update_heavy_general(std::forward<O>(item), weight, mark);
    }
  }
}

template<typename T, typename S, typename A>
template<typename O>
void var_opt_sketch<T,S,A>::update_warmup_phase(O&& item, double weight, bool mark) {
  // seems overly cautious
  if (r_ > 0 || m_ != 0 || h_ > k_) throw std::logic_error("invalid sketch state during warmup");

  if (h_ >= curr_items_alloc_) {
    grow_data_arrays();
  }

  // store items as they come in until full
  new (&data_[h_]) T(std::forward<O>(item));
  weights_[h_] = weight;
  if (marks_ != nullptr) {
    marks_[h_] = mark;
  }
  ++h_;
  num_marks_in_h_ += mark ? 1 : 0;

  // check if need to heapify
  if (h_ > k_) {
    filled_data_ = true;
    transition_from_warmup();
  }
}

/* In the "light" case the new item has weight <= old_tau, so
   would appear to the right of the R items in a hypothetical reverse-sorted
   list. It is easy to prove that it is light enough to be part of this
   round's downsampling */
template<typename T, typename S, typename A>
template<typename O>
void var_opt_sketch<T,S,A>::update_light(O&& item, double weight, bool mark) {
  if (r_ == 0 || (r_ + h_) != k_) throw std::logic_error("invalid sketch state during light warmup");

  const uint32_t m_slot = h_; // index of the gap, which becomes the M region
  if (filled_data_) {
    data_[m_slot] = std::forward<O>(item);
  } else {
    new (&data_[m_slot]) T(std::forward<O>(item));
    filled_data_ = true;
  }
  weights_[m_slot] = weight;
  if (marks_ != nullptr) { marks_[m_slot] = mark; }
  ++m_;
  
  grow_candidate_set(total_wt_r_ + weight, r_ + 1);
}

/* In the "heavy" case the new item has weight > old_tau, so would
   appear to the left of items in R in a hypothetical reverse-sorted list and
   might or might not be light enough be part of this round's downsampling.
   [After first splitting off the R=1 case] we greatly simplify the code by
   putting the new item into the H heap whether it needs to be there or not.
   In other words, it might go into the heap and then come right back out,
   but that should be okay because pseudo_heavy items cannot predominate
   in long streams unless (max wt) / (min wt) > o(exp(N)) */
template<typename T, typename S, typename A>
template<typename O>
void var_opt_sketch<T,S,A>::update_heavy_general(O&& item, double weight, bool mark) {
  if (r_ < 2 || m_ != 0 || (r_ + h_) != k_) throw std::logic_error("invalid sketch state during heavy general update");

  // put into H, although may come back out momentarily
  push(std::forward<O>(item), weight, mark);

  grow_candidate_set(total_wt_r_, r_);
}

/* The analysis of this case is similar to that of the general heavy case.
   The one small technical difference is that since R < 2, we must grab an M item
   to have a valid starting point for continue_by_growing_candidate_set () */
template<typename T, typename S, typename A>
template<typename O>
void var_opt_sketch<T,S,A>::update_heavy_r_eq1(O&& item, double weight, bool mark) {
  if (r_ != 1 || m_ != 0 || (r_ + h_) != k_) throw std::logic_error("invalid sketch state during heavy r=1 update");

  push(std::forward<O>(item), weight, mark);  // new item into H
  pop_min_to_m_region();     // pop lightest back into M

  // Any set of two items is downsample-able to one item,
  // so the two lightest items are a valid starting point for the following
  const uint32_t m_slot = k_ - 1; // array is k+1, 1 in R, so slot before is M
  grow_candidate_set(weights_[m_slot] + total_wt_r_, 2);
}

/**
 * Decreases sketch's value of k by 1, updating stored values as needed.
 *
 * <p>Subject to certain pre-conditions, decreasing k causes tau to increase. This fact is used by
 * the unioning algorithm to force "marked" items out of H and into the reservoir region.</p>
 */
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::decrease_k_by_1() {
  if (k_ <= 1) {
    throw std::logic_error("Cannot decrease k below 1 in union");
  }

  if ((h_ == 0) && (r_ == 0)) {
    // exact mode, but no data yet; this reduction is somewhat gratuitous
    --k_;
  } else if ((h_ > 0) && (r_ == 0)) {
    // exact mode, but we have some data
    --k_;
    if (h_ > k_) {
      transition_from_warmup();
    }
  } else if ((h_ > 0) && (r_ > 0)) {
    // reservoir mode, but we have some exact samples.
    // Our strategy will be to pull an item out of H (which we are allowed to do since it's
    // still just data), reduce k, and then re-insert the item

    // first, slide the R zone to the left by 1, temporarily filling the gap
    const uint32_t old_gap_idx = h_;
    const uint32_t old_final_r_idx = (h_ + 1 + r_) - 1;
    //if (old_final_r_idx != k_) throw std::logic_error("gadget in invalid state");
    
    swap_values(old_final_r_idx, old_gap_idx);

    // now we pull an item out of H; any item is ok, but if we grab the rightmost and then
    // reduce h_, the heap invariant will be preserved (and the gap will be restored), plus
    // the push() of the item that will probably happen later will be cheap.

    const uint32_t pulled_idx = h_ - 1;
    double pulled_weight = weights_[pulled_idx];
    bool pulled_mark = marks_[pulled_idx];
    // will move the pulled item below; don't do antying to it here

    if (pulled_mark) { --num_marks_in_h_; }
    weights_[pulled_idx] = -1.0; // to make bugs easier to spot

    --h_;
    --k_;
    --n_; // will be re-incremented with the update

    update(std::move(data_[pulled_idx]), pulled_weight, pulled_mark);
  } else if ((h_ == 0) && (r_ > 0)) {
    // pure reservoir mode, so can simply eject a randomly chosen sample from the reservoir
    if (r_ < 2) throw std::logic_error("r_ too small for pure reservoir mode");

    const uint32_t r_idx_to_delete = 1 + next_int(r_); // 1 for the gap
    const uint32_t rightmost_r_idx = (1 + r_) - 1;
    swap_values(r_idx_to_delete, rightmost_r_idx);
    weights_[rightmost_r_idx] = -1.0;

    --k_;
    --r_;
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::allocate_data_arrays(uint32_t tgt_size, bool use_marks) {
  filled_data_ = false;

  data_ = allocator_.allocate(tgt_size);
  weights_ = AllocDouble(allocator_).allocate(tgt_size);

  if (use_marks) {
    marks_ = AllocBool(allocator_).allocate(tgt_size);
  } else {
    marks_ = nullptr;
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::grow_data_arrays() {
  const uint32_t prev_size = curr_items_alloc_;
  curr_items_alloc_ = get_adjusted_size(k_, curr_items_alloc_ << rf_);
  if (curr_items_alloc_ == k_) {
    ++curr_items_alloc_;
  }

  if (prev_size < curr_items_alloc_) {
    filled_data_ = false;

    T* tmp_data = allocator_.allocate(curr_items_alloc_);
    double* tmp_weights = AllocDouble(allocator_).allocate(curr_items_alloc_);

    for (uint32_t i = 0; i < prev_size; ++i) {
      new (&tmp_data[i]) T(std::move(data_[i]));
      allocator_.destroy(data_ + i);
      tmp_weights[i] = weights_[i];
    }

    allocator_.deallocate(data_, prev_size);
    AllocDouble(allocator_).deallocate(weights_, prev_size);

    data_ = tmp_data;
    weights_ = tmp_weights;

    if (marks_ != nullptr) {
      bool* tmp_marks = AllocBool(allocator_).allocate(curr_items_alloc_);
      for (uint32_t i = 0; i < prev_size; ++i) {
        tmp_marks[i] = marks_[i];
      }
      AllocBool(allocator_).deallocate(marks_, prev_size);
      marks_ = tmp_marks;
    }
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::transition_from_warmup() {
  // Move the 2 lightest items from H to M
  // But the lighter really belongs in R, so update counts to reflect that
  convert_to_heap();
  pop_min_to_m_region();
  pop_min_to_m_region();
  --m_;
  ++r_;

  if (h_ != (k_ -1) || m_ != 1 || r_ != 1)
    throw std::logic_error("invalid state for transitioning from warmup");

  // Update total weight in R and then, having grabbed the value, overwrite
  // in weight_ array to help make bugs more obvious
  total_wt_r_ = weights_[k_]; // only one item, known location
  weights_[k_] = -1.0;

  // The two lightest items are ncessarily downsample-able to one item,
  // and are therefore a valid initial candidate set
  grow_candidate_set(weights_[k_ - 1] + total_wt_r_, 2);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::convert_to_heap() {
  if (h_ < 2) {
    return; // nothing to do
  }

  const uint32_t last_slot = h_ - 1;
  const int last_non_leaf = ((last_slot + 1) / 2) - 1;
  
  for (int j = last_non_leaf; j >= 0; --j) {
    restore_towards_leaves(j);
  }

  // validates heap, used for initial debugging
  //for (uint32_t j = h_ - 1; j >= 1; --j) {
  //  uint32_t p = ((j + 1) / 2) - 1;
  //  if (weights_[p] > weights_[j]) throw std::logic_error("invalid heap");
  //}
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::restore_towards_leaves(uint32_t slot_in) {
  const uint32_t last_slot = h_ - 1;
  if (h_ == 0 || slot_in > last_slot) throw std::logic_error("invalid heap state");

  uint32_t slot = slot_in;
  uint32_t child = (2 * slot_in) + 1; // might be invalid, need to check

  while (child <= last_slot) {
    uint32_t child2 = child + 1; // might also be invalid
    if ((child2 <= last_slot) && (weights_[child2] < weights_[child])) {
      // siwtch to other child if it's both valid and smaller
      child = child2;
    }

    if (weights_[slot] <= weights_[child]) {
      // invariant holds so we're done
      break;
    }

    // swap and continue
    swap_values(slot, child);

    slot = child;
    child = (2 * slot) + 1; // might be invalid, checked on next loop
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::restore_towards_root(uint32_t slot_in) {
  uint32_t slot = slot_in;
  uint32_t p = (((slot + 1) / 2) - 1); // valid if slot >= 1
  while ((slot > 0) && (weights_[slot] < weights_[p])) {
    swap_values(slot, p);
    slot = p;
    p = (((slot + 1) / 2) - 1); // valid if slot >= 1
  }
}

template<typename T, typename S, typename A>
template<typename O>
void var_opt_sketch<T,S,A>::push(O&& item, double wt, bool mark) {
  if (filled_data_) {
    data_[h_] = std::forward<O>(item);
  } else {
    new (&data_[h_]) T(std::forward<O>(item));
    filled_data_ = true;
  }
  weights_[h_] = wt;
  if (marks_ != nullptr) {
    marks_[h_] = mark;
    num_marks_in_h_ += (mark ? 1 : 0);
  }
  ++h_;

  restore_towards_root(h_ - 1); // need use old h_, but want accurate h_
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::pop_min_to_m_region() {
  if (h_ == 0 || (h_ + m_ + r_ != k_ + 1))
    throw std::logic_error("invalid heap state popping min to M region");

  if (h_ == 1) {
    // just update bookkeeping
    ++m_;
    --h_;
  } else {
    // main case
    uint32_t tgt = h_ - 1; // last slot, will swap with root
    swap_values(0, tgt);
    ++m_;
    --h_;

    restore_towards_leaves(0);
  }

  if (is_marked(h_)) {
    --num_marks_in_h_;
  }
}


template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::swap_values(uint32_t src, uint32_t dst) {
  std::swap(data_[src], data_[dst]);
  std::swap(weights_[src], weights_[dst]);

  if (marks_ != nullptr) {
    std::swap(marks_[src], marks_[dst]);
  }
}

/* When entering here we should be in a well-characterized state where the
   new item has been placed in either h or m and we have a valid but not necessarily
   maximal sampling plan figured out. The array is completely full at this point.
   Everyone in h and m has an explicit weight. The candidates are right-justified
   and are either just the r set or the r set + exactly one m item. The number
   of cands is at least 2. We will now grow the candidate set as much as possible
   by pulling sufficiently light items from h to m.
*/
template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::grow_candidate_set(double wt_cands, uint32_t num_cands) {
  if ((h_ + m_ + r_ != k_ + 1) || (num_cands < 1) || (num_cands != m_ + r_) || (m_ >= 2))
    throw std::logic_error("invariant violated when growing candidate set");

  while (h_ > 0) {
    const double next_wt = peek_min();
    const double next_tot_wt = wt_cands + next_wt;

    // test for strict lightness of next prospect (denominator multiplied through)
    // ideally: (next_wt * (next_num_cands-1) < next_tot_wt)
    //          but can use num_cands directly
    if ((next_wt * num_cands) < next_tot_wt) {
      wt_cands = next_tot_wt;
      ++num_cands;
      pop_min_to_m_region(); // adjusts h_ and m_
    } else {
      break;
    }
  }

  downsample_candidate_set(wt_cands, num_cands);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::downsample_candidate_set(double wt_cands, uint32_t num_cands) {
  if (num_cands < 2 || h_ + num_cands != k_ + 1)
    throw std::logic_error("invalid num_cands when downsampling");

  // need this before overwriting anything
  const uint32_t delete_slot = choose_delete_slot(wt_cands, num_cands);
  const uint32_t leftmost_cand_slot = h_;
  if (delete_slot < leftmost_cand_slot || delete_slot > k_)
    throw std::logic_error("invalid delete slot index when downsampling");

  // Overwrite weights for items from M moving into R,
  // to make bugs more obvious. Also needed so anyone reading the
  // weight knows if it's invalid without checking h_ and m_
  const uint32_t stop_idx = leftmost_cand_slot + m_;
  for (uint32_t j = leftmost_cand_slot; j < stop_idx; ++j) {
    weights_[j] = -1.0;
  }

  // The next two lines work even when delete_slot == leftmost_cand_slot
  data_[delete_slot] = std::move(data_[leftmost_cand_slot]);
  // cannot set data_[leftmost_cand_slot] to null since not uisng T*

  m_ = 0;
  r_ = num_cands - 1;
  total_wt_r_ = wt_cands;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::choose_delete_slot(double wt_cands, uint32_t num_cands) const {
  if (r_ == 0) throw std::logic_error("choosing delete slot while in exact mode");

  if (m_ == 0) {
    // this happens if we insert a really heavy item
    return pick_random_slot_in_r();
  } else if (m_ == 1) {
    // check if we keep th item in M or pick oen from R
    // p(keep) = (num_cand - 1) * wt_M / wt_cand
    double wt_m_cand = weights_[h_]; // slot of item in M is h_
    if ((wt_cands * next_double_exclude_zero()) < ((num_cands - 1) * wt_m_cand)) {
      return pick_random_slot_in_r(); // keep item in M
    } else {
      return h_; // indext of item in M
    }
  } else {
    // general case
    const uint32_t delete_slot = choose_weighted_delete_slot(wt_cands, num_cands);
    const uint32_t first_r_slot = h_ + m_;
    if (delete_slot == first_r_slot) {
      return pick_random_slot_in_r();
    } else {
      return delete_slot;
    }
  }
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::choose_weighted_delete_slot(double wt_cands, uint32_t num_cands) const {
  if (m_ < 1) throw std::logic_error("must have weighted delete slot");

  const uint32_t offset = h_;
  const uint32_t final_m = (offset + m_) - 1;
  const uint32_t num_to_keep = num_cands - 1;

  double left_subtotal = 0.0;
  double right_subtotal = -1.0 * wt_cands * next_double_exclude_zero();

  for (uint32_t i = offset; i <= final_m; ++i) {
    left_subtotal += num_to_keep * weights_[i];
    right_subtotal += wt_cands;

    if (left_subtotal < right_subtotal) {
      return i;
    }
  }

  // this slot tells caller that we need to delete out of R
  return final_m + 1;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::pick_random_slot_in_r() const {
  if (r_ == 0) throw std::logic_error("r_ = 0 when picking slot in R region");
  const uint32_t offset = h_ + m_;
  if (r_ == 1) {
    return offset;
  } else {
    return offset + next_int(r_);
  }
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::peek_min() const {
  if (h_ == 0) throw std::logic_error("h_ = 0 when checking min in H region");
  return weights_[0];
}

template<typename T, typename S, typename A>
inline bool var_opt_sketch<T,S,A>::is_marked(uint32_t idx) const {
  return marks_ == nullptr ? false : marks_[idx];
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::get_tau() const {
  return r_ == 0 ? std::nan("1") : (total_wt_r_ / r_);
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::strip_marks() {
  if (marks_ == nullptr) throw std::logic_error("request to strip marks from non-gadget");
  num_marks_in_h_ = 0;
  AllocBool(allocator_).deallocate(marks_, curr_items_alloc_);
  marks_ = nullptr;
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::check_preamble_longs(uint8_t preamble_longs, uint8_t flags) {
  const bool is_empty(flags & EMPTY_FLAG_MASK);
  
  if (is_empty) {
    if (preamble_longs != PREAMBLE_LONGS_EMPTY) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_EMPTY) + " for an empty sketch. Found: "
        + std::to_string(preamble_longs));
    }
  } else {
    if (preamble_longs != PREAMBLE_LONGS_WARMUP
        && preamble_longs != PREAMBLE_LONGS_FULL) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_WARMUP) + " or "
        + std::to_string(PREAMBLE_LONGS_FULL)
        + " for a non-empty sketch. Found: " + std::to_string(preamble_longs));
    }
  }
}

template<typename T, typename S, typename A>
void var_opt_sketch<T,S,A>::check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver) {
  if (family_id == FAMILY_ID) {
    if (ser_ver != SER_VER) {
      throw std::invalid_argument("Possible corruption: VarOpt serialization version must be "
        + std::to_string(SER_VER) + ". Found: " + std::to_string(ser_ver));
    }
    return;
  }
  // TODO: extend to handle reservoir sampling

  throw std::invalid_argument("Possible corruption: VarOpt family id must be "
    + std::to_string(FAMILY_ID) + ". Found: " + std::to_string(family_id));
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T, S, A>::validate_and_get_target_size(uint32_t preamble_longs, uint32_t k, uint64_t n,
                                                               uint32_t h, uint32_t r, resize_factor rf) {
  if (k == 0 || k > MAX_K) {
    throw std::invalid_argument("k must be at least 1 and less than 2^31 - 1");
  }

  uint32_t array_size;

  if (n <= k) {
    if (preamble_longs != PREAMBLE_LONGS_WARMUP) {
      throw std::invalid_argument("Possible corruption: deserializing with n <= k but not in warmup mode. "
       "Found n = " + std::to_string(n) + ", k = " + std::to_string(k));
    }
    if (n != h) {
      throw std::invalid_argument("Possible corruption: deserializing in warmup mode but n != h. "
       "Found n = " + std::to_string(n) + ", h = " + std::to_string(h));
    }
    if (r > 0) {
      throw std::invalid_argument("Possible corruption: deserializing in warmup mode but r > 0. "
       "Found r = " + std::to_string(r));
    }

    const uint32_t ceiling_lg_k = to_log_2(ceiling_power_of_2(k));
    const uint32_t min_lg_size = to_log_2(ceiling_power_of_2(h));
    const uint32_t initial_lg_size = starting_sub_multiple(ceiling_lg_k, rf, min_lg_size);
    array_size = get_adjusted_size(k, 1 << initial_lg_size);
    if (array_size == k) { // if full size, need to leave 1 for the gap
      ++array_size;
    }
  } else { // n > k
    if (preamble_longs != PREAMBLE_LONGS_FULL) { 
      throw std::invalid_argument("Possible corruption: deserializing with n > k but not in full mode. "
       "Found n = " + std::to_string(n) + ", k = " + std::to_string(k));
    }
    if (h + r != k) {
      throw std::invalid_argument("Possible corruption: deserializing in full mode but h + r != n. "
       "Found h = " + std::to_string(h) + ", r = " + std::to_string(r) + ", n = " + std::to_string(n));
    }

    array_size = k + 1;
  }

  return array_size;
}

template<typename T, typename S, typename A>
template<typename P>
subset_summary var_opt_sketch<T, S, A>::estimate_subset_sum(P predicate) const {
  if (n_ == 0) {
    return {0.0, 0.0, 0.0, 0.0};
  }

  double total_wt_h = 0.0;
  double h_true_wt = 0.0;
  size_t idx = 0;
  for (; idx < h_; ++idx) {
    double wt = weights_[idx];
    total_wt_h += wt;
    if (predicate(data_[idx])) {
      h_true_wt += wt;
    }
  }

  // if only heavy items, we have an exact answer
  if (r_ == 0) {
    return {h_true_wt, h_true_wt, h_true_wt, h_true_wt};
  }

  // since r_ > 0, we know we have samples
  const uint64_t num_samples = n_ - h_;
  double effective_sampling_rate = r_ / static_cast<double>(num_samples);
  if (effective_sampling_rate < 0.0 || effective_sampling_rate > 1.0)
    throw std::logic_error("invalid sampling rate outside [0.0, 1.0]");

  uint32_t r_true_count = 0;
  ++idx; // skip the gap
  for (; idx < (k_ + 1); ++idx) {
    if (predicate(data_[idx])) {
      ++r_true_count;
    }
  }

  double lb_true_fraction = pseudo_hypergeometric_lb_on_p(r_, r_true_count, effective_sampling_rate);
  double estimated_true_fraction = (1.0 * r_true_count) / r_;
  double ub_true_fraction = pseudo_hypergeometric_ub_on_p(r_, r_true_count, effective_sampling_rate);

  return {  h_true_wt + (total_wt_r_ * lb_true_fraction),
            h_true_wt + (total_wt_r_ * estimated_true_fraction),
            h_true_wt + (total_wt_r_ * ub_true_fraction),
            total_wt_h + total_wt_r_
         };
}

template<typename T, typename S, typename A>
class var_opt_sketch<T, S, A>::items_deleter {
  public:
  items_deleter(uint32_t num, const A& allocator) : num(num), h_count(0), r_count(0), allocator(allocator) {}
  void set_h(uint32_t h) { h_count = h; }
  void set_r(uint32_t r) { r_count = r; }  
  void operator() (T* ptr) {
    if (h_count > 0) {
      for (size_t i = 0; i < h_count; ++i) {
        ptr[i].~T();
      }
    }
    if (r_count > 0) {
      uint32_t end = h_count + r_count + 1;
      for (size_t i = h_count + 1; i < end; ++i) {
        ptr[i].~T();
      }
    }
    if (ptr != nullptr) {
      allocator.deallocate(ptr, num);
    }
  }
  private:
  uint32_t num;
  uint32_t h_count;
  uint32_t r_count;
  A allocator;
};

template<typename T, typename S, typename A>
class var_opt_sketch<T, S, A>::weights_deleter {
  public:
  weights_deleter(uint32_t num, const A& allocator) : num(num), allocator(allocator) {}
  void operator() (double* ptr) {
    if (ptr != nullptr) {
      allocator.deallocate(ptr, num);
    }
  }
  private:
  uint32_t num;
  AllocDouble allocator;
};

template<typename T, typename S, typename A>
class var_opt_sketch<T, S, A>::marks_deleter {
  public:
  marks_deleter(uint32_t num, const A& allocator) : num(num), allocator(allocator) {}
  void operator() (bool* ptr) {
    if (ptr != nullptr) {
      allocator.deallocate(ptr, 1);
    }
  }
  private:
  uint32_t num;
  AllocBool allocator;
};


template<typename T, typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator var_opt_sketch<T, S, A>::begin() const {
  return var_opt_sketch<T, S, A>::const_iterator(*this, false);
}

template<typename T, typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator var_opt_sketch<T, S, A>::end() const {
  return var_opt_sketch<T, S, A>::const_iterator(*this, true);
}

// -------- var_opt_sketch::const_iterator implementation ---------

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::const_iterator::const_iterator(const var_opt_sketch<T,S,A>& sk, bool is_end) :
  sk_(&sk),
  cum_r_weight_(0.0),
  r_item_wt_(sk.get_tau()),
  final_idx_(sk.r_ > 0 ? sk.h_ + sk.r_ + 1 : sk.h_)
{
  // index logic easier to read if not inline
  if (is_end) {
    idx_ = final_idx_;
    sk_ = nullptr;
  } else {
    idx_ = (sk.h_ == 0 && sk.r_ > 0 ? 1 : 0); // skip if gap is at start
  }

  // should only apply if sketch is empty
  if (idx_ == final_idx_) { sk_ = nullptr; }
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::const_iterator::const_iterator(const var_opt_sketch<T,S,A>& sk, bool is_end, bool use_r_region) :
  sk_(&sk),
  cum_r_weight_(0.0),
  r_item_wt_(sk.get_tau()),
  final_idx_(sk.h_ + (use_r_region ? 1 + sk.r_ : 0))
{
  if (use_r_region) {
    idx_ = sk.h_ + 1 + (is_end ? sk.r_ : 0);
  } else { // H region
    // gap at start only if h_ == 0, so index always starts at 0
    idx_ = (is_end ? sk.h_ : 0);
  }
  
  // unlike in full iterator case, may happen even if sketch is not empty
  if (idx_ == final_idx_) { sk_ = nullptr; }
}


template<typename T,  typename S, typename A>
var_opt_sketch<T, S, A>::const_iterator::const_iterator(const const_iterator& other) :
  sk_(other.sk_),
  cum_r_weight_(other.cum_r_weight_),
  r_item_wt_(other.r_item_wt_),
  idx_(other.idx_),
  final_idx_(other.final_idx_)
{}

template<typename T,  typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator& var_opt_sketch<T, S, A>::const_iterator::operator++() {
  ++idx_;
  
  if (idx_ == final_idx_) {
    sk_ = nullptr;
    return *this;
  } else if (idx_ == sk_->h_ && sk_->r_ > 0) { // check for the gap
    ++idx_;
  }
  if (idx_ > sk_->h_) { cum_r_weight_ += r_item_wt_; }
  return *this;
}

template<typename T,  typename S, typename A>
typename var_opt_sketch<T, S, A>::const_iterator& var_opt_sketch<T, S, A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::const_iterator::operator==(const const_iterator& other) const {
  if (sk_ != other.sk_) return false;
  if (sk_ == nullptr) return true; // end (and we know other.sk_ is also null)
  return idx_ == other.idx_;
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename S, typename A>
const std::pair<const T&, const double> var_opt_sketch<T, S, A>::const_iterator::operator*() const {
  double wt;
  if (idx_ < sk_->h_) {
    wt = sk_->weights_[idx_];
  } else {
    wt = r_item_wt_;
  }
  return std::pair<const T&, const double>(sk_->data_[idx_], wt);
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::const_iterator::get_mark() const {
  return sk_->marks_ == nullptr ? false : sk_->marks_[idx_];
}


// -------- var_opt_sketch::iterator implementation ---------

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A>::iterator::iterator(const var_opt_sketch<T,S,A>& sk, bool is_end, bool use_r_region) :
  sk_(&sk),
  cum_r_weight_(0.0),
  r_item_wt_(sk.get_tau()),
  final_idx_(sk.h_ + (use_r_region ? 1 + sk.r_ : 0))
{
  if (use_r_region) {
    idx_ = sk.h_ + 1 + (is_end ? sk.r_ : 0);
  } else { // H region
    // gap at start only if h_ == 0, so index always starts at 0
    idx_ = (is_end ? sk.h_ : 0);
  }
  
  // unlike in full iterator case, may happen even if sketch is not empty
  if (idx_ == final_idx_) { sk_ = nullptr; }
}

template<typename T,  typename S, typename A>
var_opt_sketch<T, S, A>::iterator::iterator(const iterator& other) :
  sk_(other.sk_),
  cum_r_weight_(other.cum_r_weight_),
  r_item_wt_(other.r_item_wt_),
  idx_(other.idx_),
  final_idx_(other.final_idx_)
{}

template<typename T,  typename S, typename A>
typename var_opt_sketch<T, S, A>::iterator& var_opt_sketch<T, S, A>::iterator::operator++() {
  ++idx_;
  
  if (idx_ == final_idx_) {
    sk_ = nullptr;
    return *this;
  } else if (idx_ == sk_->h_ && sk_->r_ > 0) { // check for the gap
    ++idx_;
  }
  if (idx_ > sk_->h_) { cum_r_weight_ += r_item_wt_; }
  return *this;
}

template<typename T,  typename S, typename A>
typename var_opt_sketch<T, S, A>::iterator& var_opt_sketch<T, S, A>::iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::iterator::operator==(const iterator& other) const {
  if (sk_ != other.sk_) return false;
  if (sk_ == nullptr) return true; // end (and we know other.sk_ is also null)
  return idx_ == other.idx_;
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::iterator::operator!=(const iterator& other) const {
  return !operator==(other);
}

template<typename T, typename S, typename A>
std::pair<T&, double> var_opt_sketch<T, S, A>::iterator::operator*() {
  double wt;
  if (idx_ < sk_->h_) {
    wt = sk_->weights_[idx_];
  } else if (idx_ == final_idx_ - 1) {
    wt = sk_->total_wt_r_ - cum_r_weight_;
  } else {
    wt = r_item_wt_;
  }
  return std::pair<T&, double>(sk_->data_[idx_], wt);
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T, S, A>::iterator::get_mark() const {
  return sk_->marks_ == nullptr ? false : sk_->marks_[idx_];
}

/**
 * Checks if target sampling allocation is more than 50% of max sampling size.
 * If so, returns max sampling size, otherwise passes through target size.
 */
template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::get_adjusted_size(uint32_t max_size, uint32_t resize_target) {
  if (max_size - (resize_target << 1) < 0L) {
    return max_size;
  }
  return resize_target;
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::starting_sub_multiple(uint32_t lg_target, uint32_t lg_rf, uint32_t lg_min) {
  return (lg_target <= lg_min)
          ? lg_min : (lg_rf == 0) ? lg_target
          : (lg_target - lg_min) % lg_rf + lg_min;
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::pseudo_hypergeometric_ub_on_p(uint64_t n, uint32_t k, double sampling_rate) {
  const double adjusted_kappa = DEFAULT_KAPPA * sqrt(1 - sampling_rate);
  return bounds_binomial_proportions::approximate_upper_bound_on_p(n, k, adjusted_kappa);
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::pseudo_hypergeometric_lb_on_p(uint64_t n, uint32_t k, double sampling_rate) {
  const double adjusted_kappa = DEFAULT_KAPPA * sqrt(1 - sampling_rate);
  return bounds_binomial_proportions::approximate_lower_bound_on_p(n, k, adjusted_kappa);
}

template<typename T, typename S, typename A>
bool var_opt_sketch<T,S,A>::is_power_of_2(uint32_t v) {
  return v && !(v & (v - 1));
}

template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::to_log_2(uint32_t v) {
  if (is_power_of_2(v)) {
    return count_trailing_zeros_in_u32(v);
  } else {
    throw std::invalid_argument("Attempt to compute integer log2 of non-positive or non-power of 2");
  }
}

// Returns an integer in the range [0, max_value) -- excludes max_value
template<typename T, typename S, typename A>
uint32_t var_opt_sketch<T,S,A>::next_int(uint32_t max_value) {
  std::uniform_int_distribution<uint32_t> dist(0, max_value - 1);
  return dist(random_utils::rand);
}

template<typename T, typename S, typename A>
double var_opt_sketch<T,S,A>::next_double_exclude_zero() {
  double r = random_utils::next_double(random_utils::rand);
  while (r == 0.0) {
    r = random_utils::next_double(random_utils::rand);
  }
  return r;
}

}

// namespace datasketches

#endif // _VAR_OPT_SKETCH_IMPL_HPP_
