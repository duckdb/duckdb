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

#ifndef THETA_UNION_IMPL_HPP_
#define THETA_UNION_IMPL_HPP_

namespace datasketches {

template<typename A>
theta_union_alloc<A>::theta_union_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta, uint64_t seed, const A& allocator):
state_(lg_cur_size, lg_nom_size, rf, p, theta, seed, nop_policy(), allocator)
{}

template<typename A>
template<typename SS>
void theta_union_alloc<A>::update(SS&& sketch) {
  state_.update(std::forward<SS>(sketch));
}

template<typename A>
auto theta_union_alloc<A>::get_result(bool ordered) const -> CompactSketch {
  return state_.get_result(ordered);
}

template<typename A>
void theta_union_alloc<A>::reset() {
  state_.reset();
}

template<typename A>
theta_union_alloc<A>::builder::builder(const A& allocator): theta_base_builder<builder, A>(allocator) {}

template<typename A>
auto theta_union_alloc<A>::builder::build() const -> theta_union_alloc {
  return theta_union_alloc(this->starting_lg_size(), this->lg_k_, this->rf_, this->p_, this->starting_theta(), this->seed_, this->allocator_);
}

} /* namespace datasketches */

# endif
