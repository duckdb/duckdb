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

#ifndef TUPLE_A_NOT_B_HPP_
#define TUPLE_A_NOT_B_HPP_

#include "tuple_sketch.hpp"
#include "theta_set_difference_base.hpp"

namespace datasketches {

template<
  typename Summary,
  typename Allocator = std::allocator<Summary>
>
class tuple_a_not_b {
public:
  using Entry = std::pair<uint64_t, Summary>;
  using ExtractKey = pair_extract_key<uint64_t, Summary>;
  using CompactSketch = compact_tuple_sketch<Summary, Allocator>;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;
  using State = theta_set_difference_base<Entry, ExtractKey, CompactSketch, AllocEntry>;

  explicit tuple_a_not_b(uint64_t seed = DEFAULT_SEED, const Allocator& allocator = Allocator());

  /**
   * Computes the a-not-b set operation given two sketches.
   * @return the result of a-not-b
   */
  template<typename FwdSketch, typename Sketch>
  CompactSketch compute(FwdSketch&& a, const Sketch& b, bool ordered = true) const;

private:
  State state_;
};

} /* namespace datasketches */

#include "tuple_a_not_b_impl.hpp"

#endif
