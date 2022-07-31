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

#ifndef ARRAY_OF_DOUBLES_UNION_HPP_
#define ARRAY_OF_DOUBLES_UNION_HPP_

#include <vector>
#include <memory>

#include "array_of_doubles_sketch.hpp"
#include "tuple_union.hpp"

namespace datasketches {

template<typename A = std::allocator<double>>
struct array_of_doubles_union_policy_alloc {
  array_of_doubles_union_policy_alloc(uint8_t num_values = 1): num_values_(num_values) {}

  void operator()(aod<A>& summary, const aod<A>& other) const {
    for (size_t i = 0; i < summary.size(); ++i) {
      summary[i] += other[i];
    }
  }

  uint8_t get_num_values() const {
    return num_values_;
  }
private:
  uint8_t num_values_;
};

using array_of_doubles_union_policy = array_of_doubles_union_policy_alloc<>;

template<typename Allocator = std::allocator<double>>
class array_of_doubles_union_alloc: public tuple_union<aod<Allocator>, array_of_doubles_union_policy_alloc<Allocator>, AllocAOD<Allocator>> {
public:
  using Policy = array_of_doubles_union_policy_alloc<Allocator>;
  using Base = tuple_union<aod<Allocator>, Policy, AllocAOD<Allocator>>;
  using CompactSketch = compact_array_of_doubles_sketch_alloc<Allocator>;
  using resize_factor = theta_constants::resize_factor;

  class builder;

  CompactSketch get_result(bool ordered = true) const;

private:
  // for builder
  array_of_doubles_union_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta, uint64_t seed, const Policy& policy, const Allocator& allocator);
};

template<typename Allocator>
class array_of_doubles_union_alloc<Allocator>::builder: public tuple_base_builder<builder, array_of_doubles_union_policy_alloc<Allocator>, Allocator> {
public:
  builder(const array_of_doubles_union_policy_alloc<Allocator>& policy = array_of_doubles_union_policy_alloc<Allocator>(), const Allocator& allocator = Allocator());
  array_of_doubles_union_alloc<Allocator> build() const;
};

// alias with default allocator
using array_of_doubles_union = array_of_doubles_union_alloc<>;

} /* namespace datasketches */

#include "array_of_doubles_union_impl.hpp"

#endif
