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

template<typename A>
array_of_doubles_union_alloc<A>::array_of_doubles_union_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta, uint64_t seed, const Policy& policy, const A& allocator):
Base(lg_cur_size, lg_nom_size, rf, p, theta, seed, policy, allocator)
{}

template<typename A>
auto array_of_doubles_union_alloc<A>::get_result(bool ordered) const -> CompactSketch {
  return compact_array_of_doubles_sketch_alloc<A>(this->state_.get_policy().get_policy().get_num_values(), Base::get_result(ordered));
}

// builder

template<typename A>
array_of_doubles_union_alloc<A>::builder::builder(const Policy& policy, const A& allocator):
tuple_base_builder<builder, Policy, A>(policy, allocator) {}

template<typename A>
array_of_doubles_union_alloc<A> array_of_doubles_union_alloc<A>::builder::build() const {
  return array_of_doubles_union_alloc<A>(this->starting_lg_size(), this->lg_k_, this->rf_, this->p_, this->starting_theta(), this->seed_, this->policy_, this->allocator_);
}

} /* namespace datasketches */
