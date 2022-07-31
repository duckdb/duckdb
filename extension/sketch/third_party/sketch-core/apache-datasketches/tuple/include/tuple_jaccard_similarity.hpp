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

#ifndef TUPLE_JACCARD_SIMILARITY_HPP_
#define TUPLE_JACCARD_SIMILARITY_HPP_

#include "theta_jaccard_similarity_base.hpp"
#include "tuple_union.hpp"
#include "tuple_intersection.hpp"

namespace datasketches {

template<
  typename Summary,
  typename IntersectionPolicy,
  typename UnionPolicy = default_union_policy<Summary>,
  typename Allocator = std::allocator<Summary>>
using tuple_jaccard_similarity = jaccard_similarity_base<tuple_union<Summary, UnionPolicy, Allocator>, tuple_intersection<Summary, IntersectionPolicy, Allocator>, pair_extract_key<uint64_t, Summary>>;

} /* namespace datasketches */

# endif
