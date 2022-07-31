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

#ifndef THETA_SET_DIFFERENCE_BASE_HPP_
#define THETA_SET_DIFFERENCE_BASE_HPP_

#include "theta_comparators.hpp"
#include "theta_update_sketch_base.hpp"

namespace datasketches {

template<
  typename Entry,
  typename ExtractKey,
  typename CompactSketch,
  typename Allocator
>
class theta_set_difference_base {
public:
  using comparator = compare_by_key<ExtractKey>;
  using AllocU64 = typename std::allocator_traits<Allocator>::template rebind_alloc<uint64_t>;
  using hash_table = theta_update_sketch_base<uint64_t, trivial_extract_key, AllocU64>;

  theta_set_difference_base(uint64_t seed, const Allocator& allocator = Allocator());

  template<typename FwdSketch, typename Sketch>
  CompactSketch compute(FwdSketch&& a, const Sketch& b, bool ordered) const;

private:
  Allocator allocator_;
  uint16_t seed_hash_;
};

} /* namespace datasketches */

#include "theta_set_difference_base_impl.hpp"

#endif
