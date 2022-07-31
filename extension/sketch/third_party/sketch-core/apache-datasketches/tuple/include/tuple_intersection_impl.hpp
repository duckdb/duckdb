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

namespace datasketches {

template<typename S, typename P, typename A>
tuple_intersection<S, P, A>::tuple_intersection(uint64_t seed, const P& policy, const A& allocator):
state_(seed, internal_policy(policy), allocator)
{}

template<typename S, typename P, typename A>
template<typename SS>
void tuple_intersection<S, P, A>::update(SS&& sketch) {
  state_.update(std::forward<SS>(sketch));
}

template<typename S, typename P, typename A>
auto tuple_intersection<S, P, A>::get_result(bool ordered) const -> CompactSketch {
  return state_.get_result(ordered);
}

template<typename S, typename P, typename A>
bool tuple_intersection<S, P, A>::has_result() const {
  return state_.has_result();
}

} /* namespace datasketches */
