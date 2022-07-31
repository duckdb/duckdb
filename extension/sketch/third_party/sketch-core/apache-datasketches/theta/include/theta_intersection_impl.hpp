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

#ifndef THETA_INTERSECTION_IMPL_HPP_
#define THETA_INTERSECTION_IMPL_HPP_

namespace datasketches {

template<typename A>
theta_intersection_alloc<A>::theta_intersection_alloc(uint64_t seed, const A& allocator):
state_(seed, nop_policy(), allocator)
{}

template<typename A>
template<typename SS>
void theta_intersection_alloc<A>::update(SS&& sketch) {
  state_.update(std::forward<SS>(sketch));
}

template<typename A>
auto theta_intersection_alloc<A>::get_result(bool ordered) const -> CompactSketch {
  return state_.get_result(ordered);
}

template<typename A>
bool theta_intersection_alloc<A>::has_result() const {
  return state_.has_result();
}

} /* namespace datasketches */

# endif
