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

#ifndef ARRAY_OF_DOUBLES_A_NOT_B_HPP_
#define ARRAY_OF_DOUBLES_A_NOT_B_HPP_

#include <vector>
#include <memory>

#include "array_of_doubles_sketch.hpp"
#include "tuple_a_not_b.hpp"

namespace datasketches {

template<typename Allocator = std::allocator<double>>
class array_of_doubles_a_not_b_alloc: tuple_a_not_b<aod<Allocator>, AllocAOD<Allocator>> {
public:
  using Summary = aod<Allocator>;
  using AllocSummary = AllocAOD<Allocator>;
  using Base = tuple_a_not_b<Summary, AllocSummary>;
  using CompactSketch = compact_array_of_doubles_sketch_alloc<Allocator>;

  explicit array_of_doubles_a_not_b_alloc(uint64_t seed = DEFAULT_SEED, const Allocator& allocator = Allocator());

  template<typename FwdSketch, typename Sketch>
  CompactSketch compute(FwdSketch&& a, const Sketch& b, bool ordered = true) const;
};

// alias with the default allocator for convenience
using array_of_doubles_a_not_b = array_of_doubles_a_not_b_alloc<>;

} /* namespace datasketches */

#include "array_of_doubles_a_not_b_impl.hpp"

#endif
