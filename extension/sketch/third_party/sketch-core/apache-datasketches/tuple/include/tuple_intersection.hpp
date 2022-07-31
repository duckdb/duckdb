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

#ifndef TUPLE_INTERSECTION_HPP_
#define TUPLE_INTERSECTION_HPP_

#include "tuple_sketch.hpp"
#include "theta_intersection_base.hpp"

namespace datasketches {

/*
// for types with defined + operation
template<typename Summary>
struct example_intersection_policy {
  void operator()(Summary& summary, const Summary& other) const {
    summary += other;
  }
  void operator()(Summary& summary, Summary&& other) const {
    summary += other;
  }
};
*/

template<
  typename Summary,
  typename Policy,
  typename Allocator = std::allocator<Summary>
>
class tuple_intersection {
public:
  using Entry = std::pair<uint64_t, Summary>;
  using ExtractKey = pair_extract_key<uint64_t, Summary>;
  using Sketch = tuple_sketch<Summary, Allocator>;
  using CompactSketch = compact_tuple_sketch<Summary, Allocator>;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;

  // reformulate the external policy that operates on Summary
  // in terms of operations on Entry
  struct internal_policy {
    internal_policy(const Policy& policy): policy_(policy) {}
    void operator()(Entry& internal_entry, const Entry& incoming_entry) const {
      policy_(internal_entry.second, incoming_entry.second);
    }
    void operator()(Entry& internal_entry, Entry&& incoming_entry) const {
      policy_(internal_entry.second, std::move(incoming_entry.second));
    }
    const Policy& get_policy() const { return policy_; }
    Policy policy_;
  };

  using State = theta_intersection_base<Entry, ExtractKey, internal_policy, Sketch, CompactSketch, AllocEntry>;

  explicit tuple_intersection(uint64_t seed = DEFAULT_SEED, const Policy& policy = Policy(), const Allocator& allocator = Allocator());

  /**
   * Updates the intersection with a given sketch.
   * The intersection can be viewed as starting from the "universe" set, and every update
   * can reduce the current set to leave the overlapping subset only.
   * @param sketch represents input set for the intersection
   */
  template<typename FwdSketch>
  void update(FwdSketch&& sketch);

  /**
   * Produces a copy of the current state of the intersection.
   * If update() was not called, the state is the infinite "universe",
   * which is considered an undefined state, and throws an exception.
   * @param ordered optional flag to specify if ordered sketch should be produced
   * @return the result of the intersection
   */
  CompactSketch get_result(bool ordered = true) const;

  /**
   * Returns true if the state of the intersection is defined (not infinite "universe").
   * @return true if the state is valid
   */
  bool has_result() const;

protected:
  State state_;
};

} /* namespace datasketches */

#include "tuple_intersection_impl.hpp"

#endif
