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

#ifndef _VAR_OPT_UNION_IMPL_HPP_
#define _VAR_OPT_UNION_IMPL_HPP_

#include "var_opt_union.hpp"

#include <cmath>
#include <sstream>
#include <stdexcept>

namespace datasketches {

template<typename T, typename S, typename A>
var_opt_union<T,S,A>::var_opt_union(uint32_t max_k, const A& allocator) :
  n_(0),
  outer_tau_numer_(0.0),
  outer_tau_denom_(0),
  max_k_(max_k),
  gadget_(max_k, var_opt_sketch<T,S,A>::DEFAULT_RESIZE_FACTOR, true, allocator)
{}

template<typename T, typename S, typename A>
var_opt_union<T,S,A>::var_opt_union(const var_opt_union& other) :
  n_(other.n_),
  outer_tau_numer_(other.outer_tau_numer_),
  outer_tau_denom_(other.outer_tau_denom_),
  max_k_(other.max_k_),
  gadget_(other.gadget_)
{}

template<typename T, typename S, typename A>
var_opt_union<T,S,A>::var_opt_union(var_opt_union&& other) noexcept :
  n_(other.n_),
  outer_tau_numer_(other.outer_tau_numer_),
  outer_tau_denom_(other.outer_tau_denom_),
  max_k_(other.max_k_),
  gadget_(std::move(other.gadget_))
{}

template<typename T, typename S, typename A>
var_opt_union<T,S,A>::var_opt_union(uint64_t n, double outer_tau_numer, uint64_t outer_tau_denom,
                                    uint32_t max_k, var_opt_sketch<T,S,A>&& gadget) :
  n_(n),
  outer_tau_numer_(outer_tau_numer),
  outer_tau_denom_(outer_tau_denom),
  max_k_(max_k),
  gadget_(gadget)
{}

template<typename T, typename S, typename A>
var_opt_union<T,S,A>::~var_opt_union() {}

template<typename T, typename S, typename A>
var_opt_union<T,S,A>& var_opt_union<T,S,A>::operator=(const var_opt_union& other) {
  var_opt_union<T,S,A> union_copy(other);
  std::swap(n_, union_copy.n_);
  std::swap(outer_tau_numer_, union_copy.outer_tau_numer_);
  std::swap(outer_tau_denom_, union_copy.outer_tau_denom_);
  std::swap(max_k_, union_copy.max_k_);
  std::swap(gadget_, union_copy.gadget_);
  return *this;
}

template<typename T, typename S, typename A>
var_opt_union<T,S,A>& var_opt_union<T,S,A>::operator=(var_opt_union&& other) {
  std::swap(n_, other.n_);
  std::swap(outer_tau_numer_, other.outer_tau_numer_);
  std::swap(outer_tau_denom_, other.outer_tau_denom_);
  std::swap(max_k_, other.max_k_);
  std::swap(gadget_, other.gadget_);
  return *this;
}

/*
 * An empty union requires 8 bytes.
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
 *  2   ||------------------------Outer Tau Numerator (double)---------------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||----------------------Outer Tau Denominator (uint64_t)-------------------------|
 * </pre>
 */

template<typename T, typename S, typename A>
var_opt_union<T,S,A> var_opt_union<T,S,A>::deserialize(std::istream& is, const A& allocator) {
  return deserialize(is, S(), allocator);
}

template<typename T, typename S, typename A>
template<typename SerDe>
var_opt_union<T,S,A> var_opt_union<T,S,A>::deserialize(std::istream& is, const SerDe& sd, const A& allocator) {
  const auto preamble_longs = read<uint8_t>(is);
  const auto serial_version = read<uint8_t>(is);
  const auto family_id = read<uint8_t>(is);
  const auto flags = read<uint8_t>(is);
  const auto max_k = read<uint32_t>(is);

  check_preamble_longs(preamble_longs, flags);
  check_family_and_serialization_version(family_id, serial_version);

  if (max_k == 0 || max_k > MAX_K) {
    throw std::invalid_argument("k must be at least 1 and less than 2^31 - 1");
  }

  bool is_empty = flags & EMPTY_FLAG_MASK;
  
  if (is_empty) {
    if (!is.good())
      throw std::runtime_error("error reading from std::istream"); 
    else
      return var_opt_union<T,S,A>(max_k);
  }

  const auto items_seen = read<uint64_t>(is);
  const auto outer_tau_numer = read<double>(is);
  const auto outer_tau_denom = read<uint64_t>(is);

  var_opt_sketch<T,S,A> gadget = var_opt_sketch<T,S,A>::deserialize(is, sd, allocator);

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return var_opt_union<T,S,A>(items_seen, outer_tau_numer, outer_tau_denom, max_k, std::move(gadget));
}

template<typename T, typename S, typename A>
var_opt_union<T,S,A> var_opt_union<T,S,A>::deserialize(const void* bytes, size_t size, const A& allocator) {
  return deserialize(bytes, size, S(), allocator);
}

template<typename T, typename S, typename A>
template<typename SerDe>
var_opt_union<T,S,A> var_opt_union<T,S,A>::deserialize(const void* bytes, size_t size, const SerDe& sd, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  ptr += copy_from_mem(ptr, preamble_longs);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t flags;
  ptr += copy_from_mem(ptr, flags);
  uint32_t max_k;
  ptr += copy_from_mem(ptr, max_k);

  check_preamble_longs(preamble_longs, flags);
  check_family_and_serialization_version(family_id, serial_version);

  if (max_k == 0 || max_k > MAX_K) {
    throw std::invalid_argument("k must be at least 1 and less than 2^31 - 1");
  }

  bool is_empty = flags & EMPTY_FLAG_MASK;

  if (is_empty) {
    return var_opt_union<T,S,A>(max_k);
  }

  uint64_t items_seen;
  ptr += copy_from_mem(ptr, items_seen);
  double outer_tau_numer;
  ptr += copy_from_mem(ptr, outer_tau_numer);
  uint64_t outer_tau_denom;
  ptr += copy_from_mem(ptr, outer_tau_denom);

  const size_t gadget_size = size - (PREAMBLE_LONGS_NON_EMPTY << 3);
  var_opt_sketch<T,S,A> gadget = var_opt_sketch<T,S,A>::deserialize(ptr, gadget_size, sd, allocator);

  return var_opt_union<T,S,A>(items_seen, outer_tau_numer, outer_tau_denom, max_k, std::move(gadget));
}

template<typename T, typename S, typename A>
template<typename SerDe>
size_t var_opt_union<T,S,A>::get_serialized_size_bytes(const SerDe& sd) const {
  if (n_ == 0) {
    return PREAMBLE_LONGS_EMPTY << 3;
  } else {
    return (PREAMBLE_LONGS_NON_EMPTY << 3) + gadget_.get_serialized_size_bytes(sd);
  }
}

template<typename T, typename S, typename A>
template<typename SerDe>
void var_opt_union<T,S,A>::serialize(std::ostream& os, const SerDe& sd) const {
  bool empty = (n_ == 0);

  const uint8_t serialization_version(SER_VER);
  const uint8_t family_id(FAMILY_ID);

  uint8_t preamble_longs;
  uint8_t flags;
  if (empty) {
    preamble_longs = PREAMBLE_LONGS_EMPTY;
    flags = EMPTY_FLAG_MASK;
  } else {
    preamble_longs = PREAMBLE_LONGS_NON_EMPTY;
    flags = 0;
  }

  write(os, preamble_longs);
  write(os, serialization_version);
  write(os, family_id);
  write(os, flags);
  write(os, max_k_);

  if (!empty) {
    write(os, n_);
    write(os, outer_tau_numer_);
    write(os, outer_tau_denom_);
    gadget_.serialize(os, sd);
  }
}

template<typename T, typename S, typename A>
template<typename SerDe>
std::vector<uint8_t, AllocU8<A>> var_opt_union<T,S,A>::serialize(unsigned header_size_bytes, const SerDe& sd) const {
  const size_t size = header_size_bytes + get_serialized_size_bytes(sd);
  std::vector<uint8_t, AllocU8<A>> bytes(size, 0, gadget_.allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;

  const bool empty = n_ == 0;

  const uint8_t serialization_version(SER_VER);
  const uint8_t family_id(FAMILY_ID);

  uint8_t preamble_longs;
  uint8_t flags;

  if (empty) {
    preamble_longs = PREAMBLE_LONGS_EMPTY;
    flags = EMPTY_FLAG_MASK;
  } else {
    preamble_longs = PREAMBLE_LONGS_NON_EMPTY;
    flags = 0;
  }

  // first prelong
  ptr += copy_to_mem(preamble_longs, ptr);
  ptr += copy_to_mem(serialization_version, ptr);
  ptr += copy_to_mem(family_id, ptr);
  ptr += copy_to_mem(flags, ptr);
  ptr += copy_to_mem(max_k_, ptr);

  if (!empty) {
    ptr += copy_to_mem(n_, ptr);
    ptr += copy_to_mem(outer_tau_numer_, ptr);
    ptr += copy_to_mem(outer_tau_denom_, ptr);

    auto gadget_bytes = gadget_.serialize(0, sd);
    ptr += copy_to_mem(gadget_bytes.data(), ptr, gadget_bytes.size() * sizeof(uint8_t));
  }

  return bytes;
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::reset() {
  n_ = 0;
  outer_tau_numer_ = 0.0;
  outer_tau_denom_ = 0;
  gadget_.reset();
}

template<typename T, typename S, typename A>
string<A> var_opt_union<T,S,A>::to_string() const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### VarOpt Union SUMMARY:" << std::endl;
  os << "   n             : " << n_ << std::endl;
  os << "   Max k         : " << max_k_ << std::endl;
  os << "   Gadget Summary:" << std::endl;
  os << gadget_.to_string();
  os << "### END VarOpt Union SUMMARY" << std::endl;
  return string<A>(os.str().c_str(), gadget_.allocator_);
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::update(const var_opt_sketch<T,S,A>& sk) {
  merge_items(sk);
  resolve_tau(sk);
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::update(var_opt_sketch<T,S,A>&& sk) {
  merge_items(std::move(sk));
  resolve_tau(sk); // don't need items, so ok even if they've been moved out
}

template<typename T, typename S, typename A>
double var_opt_union<T,S,A>::get_outer_tau() const {
  if (outer_tau_denom_ == 0) {
    return 0.0;
  } else {
    return outer_tau_numer_ / outer_tau_denom_;
  }
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::merge_items(const var_opt_sketch<T,S,A>& sketch) {
  if (sketch.n_ == 0) {
    return;
  }

  n_ += sketch.n_;

  // H region const_iterator
  typename var_opt_sketch<T,S,A>::const_iterator h_itr(sketch, false, false);
  typename var_opt_sketch<T,S,A>::const_iterator h_end(sketch, true, false);
  while (h_itr != h_end) {
    std::pair<const T&, const double> sample = *h_itr;
    gadget_.update(sample.first, sample.second, false);
    ++h_itr;
  }

  // Weight-correcting R region iterator (const_iterator doesn't do the correction)
  typename var_opt_sketch<T,S,A>::iterator r_itr(sketch, false, true);
  typename var_opt_sketch<T,S,A>::iterator r_end(sketch, true, true);
  while (r_itr != r_end) {
    std::pair<const T&, const double> sample = *r_itr;
    gadget_.update(sample.first, sample.second, true);
    ++r_itr;
  }
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::merge_items(var_opt_sketch<T,S,A>&& sketch) {
  if (sketch.n_ == 0) {
    return;
  }

  n_ += sketch.n_;

  // H region iterator
  typename var_opt_sketch<T,S,A>::iterator h_itr(sketch, false, false);
  typename var_opt_sketch<T,S,A>::iterator h_end(sketch, true, false);
  while (h_itr != h_end) {
    std::pair<T&, double> sample = *h_itr;
    gadget_.update(std::move(sample.first), sample.second, false);
    ++h_itr;
  }

  // Weight-correcting R region iterator
  typename var_opt_sketch<T,S,A>::iterator r_itr(sketch, false, true);
  typename var_opt_sketch<T,S,A>::iterator r_end(sketch, true, true);
  while (r_itr != r_end) {
    std::pair<T&, double> sample = *r_itr;
    gadget_.update(std::move(sample.first), sample.second, true);
    ++r_itr;
  }
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::resolve_tau(const var_opt_sketch<T,S,A>& sketch) {
  if (sketch.r_ > 0) {
    const double sketch_tau = sketch.get_tau();
    const double outer_tau = get_outer_tau();

    if (outer_tau_denom_ == 0) {
      // detect first estimation mode sketch and grab its tau
      outer_tau_numer_ = sketch.total_wt_r_;
      outer_tau_denom_ = sketch.r_;
    } else if (sketch_tau > outer_tau) {
      // switch to a bigger value of outer_tau
      outer_tau_numer_ = sketch.total_wt_r_;
      outer_tau_denom_ = sketch.r_;
    } else if (sketch_tau == outer_tau) {
      // Ok if previous equality test isn't quite perfect. Mistakes in either direction should
      // be fairly benign.
      // Without conceptually changing outer_tau, update number and denominator. In particular,
      // add the total weight of the incoming reservoir to the running total.
      outer_tau_numer_ += sketch.total_wt_r_;
      outer_tau_denom_ += sketch.r_;
    }

    // do nothing if sketch's tau is smaller than outer_tau
  }
}

template<typename T, typename S, typename A>
var_opt_sketch<T,S,A> var_opt_union<T,S,A>::get_result() const {
  // If no marked items in H, gadget is already valid mathematically. We can return what is
  // basically just a copy of the gadget.
  if (gadget_.num_marks_in_h_ == 0) {
    return simple_gadget_coercer();
  } else {
    // Copy of gadget. This may produce needless copying in the
    // pseudo-exact case below, but should simplify the code without
    // needing to make the gadget a pointer
    var_opt_sketch<T,S,A> gcopy(gadget_, false, n_);

    // At this point, we know that marked items are present in H. So:
    //   1. Result will necessarily be in estimation mode
    //   2. Marked items currently in H need to be absorbed into reservoir (R)
    const bool is_pseudo_exact = detect_and_handle_subcase_of_pseudo_exact(gcopy);
    if (!is_pseudo_exact) {
      // continue with main logic
      migrate_marked_items_by_decreasing_k(gcopy);
    }
    // sub-case was already detected and handled, so return the result
    return gcopy;
  }
}

/**
 * When there are no marked items in H, the gadget is mathematically equivalent to a valid
 * varopt sketch. This method simply returns a copy (without perserving marks).
 *
 * @return A shallow copy of the gadget as valid varopt sketch
 */
template<typename T, typename S, typename A>
var_opt_sketch<T,S,A> var_opt_union<T,S,A>::simple_gadget_coercer() const {
  if (gadget_.num_marks_in_h_ != 0) throw std::logic_error("simple gadget coercer only applies if no marks");
  return var_opt_sketch<T,S,A>(gadget_, true, n_);
}

// this is a condition checked in detect_and_handle_subcase_of_pseudo_exact()
template<typename T, typename S, typename A>
bool var_opt_union<T,S,A>::there_exist_unmarked_h_items_lighter_than_target(double threshold) const {
  for (uint32_t i = 0; i < gadget_.h_; ++i) {
    if ((gadget_.weights_[i] < threshold) && !gadget_.marks_[i]) {
      return true;
    }
  }
  return false;
}

template<typename T, typename S, typename A>
bool var_opt_union<T,S,A>::detect_and_handle_subcase_of_pseudo_exact(var_opt_sketch<T,S,A>& sk) const {
  // gadget is seemingly exact
  const bool condition1 = gadget_.r_ == 0;

  // but there are marked items in H, so only _pseudo_ exact
  const bool condition2 = gadget_.num_marks_in_h_ > 0;

  // if gadget is pseudo-exact and the number of marks equals outer_tau_denom, then we can deduce
  // from the bookkeeping logic of resolve_tau() that all estimation mode input sketches must
  // have had the same tau, so we can throw all of the marked items into a common reservoir.
  const bool condition3 = gadget_.num_marks_in_h_ == outer_tau_denom_;

  if (!(condition1 && condition2 && condition3)) {
    return false;
  } else {

    // explicitly enforce rule that items in H should not be lighter than the sketch's tau
    const bool anti_condition4 = there_exist_unmarked_h_items_lighter_than_target(gadget_.get_tau());
    if (anti_condition4) {
      return false;
    } else {
      // conditions 1 through 4 hold
      mark_moving_gadget_coercer(sk);
      return true;
    }
  }
}

/**
 * This coercer directly transfers marked items from the gadget's H into the result's R.
 * Deciding whether that is a valid thing to do is the responsibility of the caller. Currently,
 * this is only used for a subcase of pseudo-exact, but later it might be used by other
 * subcases as well.
 *
 * @param sk Copy of the gadget, modified with marked items moved to the reservoir
 */
template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::mark_moving_gadget_coercer(var_opt_sketch<T,S,A>& sk) const {
  const uint32_t result_k = gadget_.h_ + gadget_.r_;

  uint32_t result_h = 0;
  uint32_t result_r = 0;
  size_t next_r_pos = result_k; // = (result_k+1)-1, to fill R region from back to front

  typedef typename std::allocator_traits<A>::template rebind_alloc<double> AllocDouble;
  double* wts = AllocDouble().allocate(result_k + 1);
  T* data     = A().allocate(result_k + 1);
    
  // insert R region items, ignoring weights
  // Currently (May 2017) this next block is unreachable; this coercer is used only in the
  // pseudo-exact case in which case there are no items natively in R, only marked items in H
  // that will be moved into R as part of the coercion process.
  // Addedndum (Jan 2020): Cleanup at end of method assumes R count is 0
  const size_t final_idx = gadget_.get_num_samples();
  for (size_t idx = gadget_.h_ + 1; idx <= final_idx; ++idx) {
    A().construct(&data[next_r_pos], T(gadget_.data_[idx]));
    wts[next_r_pos]  = gadget_.weights_[idx];
    ++result_r;
    --next_r_pos;
  }
  
  double transferred_weight = 0;

  // insert H region items
  for (size_t idx = 0; idx < gadget_.h_; ++idx) {
    if (gadget_.marks_[idx]) {
      A().construct(&data[next_r_pos], T(gadget_.data_[idx]));
      wts[next_r_pos] = -1.0;
      transferred_weight += gadget_.weights_[idx];
      ++result_r;
      --next_r_pos;
    } else {
      A().construct(&data[result_h], T(gadget_.data_[idx]));
      wts[result_h] = gadget_.weights_[idx];
      ++result_h;
    }
  }

  if (result_h + result_r != result_k) throw std::logic_error("H + R counts must equal k");
  if (fabs(transferred_weight - outer_tau_numer_) > 1e-10) {
    throw std::logic_error("uexpected mismatch in transferred weight");
  }

  const double result_r_weight = gadget_.total_wt_r_ + transferred_weight;
  const uint64_t result_n = n_;
    
  // explicitly set weight value for the gap
  wts[result_h] = -1.0;

  // clean up arrays in input sketch, replace with new values
  typedef typename std::allocator_traits<A>::template rebind_alloc<bool> AllocBool;
  AllocBool().deallocate(sk.marks_, sk.curr_items_alloc_);
  AllocDouble().deallocate(sk.weights_, sk.curr_items_alloc_);
  for (size_t i = 0; i < result_k; ++i) { A().destroy(sk.data_ + i); } // assumes everything in H region, no gap
  A().deallocate(sk.data_, sk.curr_items_alloc_);

  sk.data_ = data;
  sk.weights_ = wts;
  sk.marks_ = nullptr;
  sk.num_marks_in_h_ = 0;
  sk.curr_items_alloc_ = result_k + 1;
  sk.k_ = result_k;
  sk.n_ = result_n;
  sk.h_ = result_h;
  sk.r_ = result_r;
  sk.total_wt_r_ = result_r_weight;
}

// this is basically a continuation of get_result(), but modifying the input gadget copy
template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::migrate_marked_items_by_decreasing_k(var_opt_sketch<T,S,A>& gcopy) const {
  const uint32_t r_count = gcopy.r_;
  const uint32_t h_count = gcopy.h_;
  const uint32_t k = gcopy.k_;

  // should be ensured by caller
  if (gcopy.num_marks_in_h_ == 0) throw std::logic_error("unexpectedly found no marked items to migrate");
  // either full (of samples), in pseudo-exact mode, or both
  if ((r_count != 0) && ((h_count + r_count) != k)) throw std::logic_error("invalid gadget state");

  // if non-full and pseudo-exact, change k so that gcopy is full
  if ((r_count == 0) && (h_count < k)) {
    gcopy.k_ = h_count; // may leve extra space allocated but that's ok
  }

  // Now k equals the number of samples, so reducing k will increase tau.
  // Also, we know that there are at least 2 samples because 0 or 1 would have been handled
  // by the earlier logic in get_result()
  gcopy.decrease_k_by_1();

  // gcopy is now in estimation mode, just like the final result must be (due to marked items)
  if (gcopy.get_tau() == 0.0) throw std::logic_error("gadget must be in sampling mode");

  // keep reducing k until all marked items have been absorbed into the reservoir
  while (gcopy.num_marks_in_h_ > 0) {
    // gcopy.k_ >= 2 because h_ and r_ are both at least 1, but checked in next method anyway
    gcopy.decrease_k_by_1();
  }

  gcopy.strip_marks();
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::check_preamble_longs(uint8_t preamble_longs, uint8_t flags) {
  bool is_empty(flags & EMPTY_FLAG_MASK);
  
  if (is_empty) {
    if (preamble_longs != PREAMBLE_LONGS_EMPTY) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_EMPTY) + " for an empty sketch. Found: "
        + std::to_string(preamble_longs));
    }
  } else {
    if (preamble_longs != PREAMBLE_LONGS_NON_EMPTY) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_NON_EMPTY)
        + " for a non-empty sketch. Found: " + std::to_string(preamble_longs));
    }
  }
}

template<typename T, typename S, typename A>
void var_opt_union<T,S,A>::check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver) {
  if (family_id == FAMILY_ID) {
    if (ser_ver != SER_VER) {
      throw std::invalid_argument("Possible corruption: VarOpt Union serialization version must be "
        + std::to_string(SER_VER) + ". Found: " + std::to_string(ser_ver));
    }
    return;
  }
  // TODO: extend to handle reservoir sampling

  throw std::invalid_argument("Possible corruption: VarOpt Union family id must be "
    + std::to_string(FAMILY_ID) + ". Found: " + std::to_string(family_id));
}

} // namespace datasketches

#endif // _VAR_OPT_UNION_IMPL_HPP_
