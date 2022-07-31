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

#ifndef THETA_JACCARD_SIMILARITY_HPP_
#define THETA_JACCARD_SIMILARITY_HPP_

#include "theta_jaccard_similarity_base.hpp"
#include "theta_union.hpp"
#include "theta_intersection.hpp"

namespace datasketches {

template<typename Allocator = std::allocator<uint64_t>>
using theta_jaccard_similarity_alloc = jaccard_similarity_base<theta_union_alloc<Allocator>, theta_intersection_alloc<Allocator>, trivial_extract_key>;

// alias with default allocator for convenience
using theta_jaccard_similarity = theta_jaccard_similarity_alloc<std::allocator<uint64_t>>;

} /* namespace datasketches */

# endif
