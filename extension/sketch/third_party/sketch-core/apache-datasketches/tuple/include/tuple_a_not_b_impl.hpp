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

template<typename S, typename A>
tuple_a_not_b<S, A>::tuple_a_not_b(uint64_t seed, const A& allocator):
state_(seed, allocator)
{}

template<typename S, typename A>
template<typename FwdSketch, typename Sketch>
auto tuple_a_not_b<S, A>::compute(FwdSketch&& a, const Sketch& b, bool ordered) const -> CompactSketch {
  return state_.compute(std::forward<FwdSketch>(a), b, ordered);
}

} /* namespace datasketches */
