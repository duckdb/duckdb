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

#include <catch.hpp>

#include <reverse_purge_hash_map.hpp>

namespace datasketches {

TEST_CASE("reverse purge hash map: empty", "[frequent_items_sketch]") {
  reverse_purge_hash_map<int> map(3, 3, std::allocator<int>());
  REQUIRE(map.get_num_active() == 0);
  REQUIRE(map.get_lg_cur_size() == 3);  // static_cast<uint8_t>(3)
}

TEST_CASE("reverse purge hash map: one item", "[frequent_items_sketch]") {
  reverse_purge_hash_map<int> map(3, 3, std::allocator<int>());
  map.adjust_or_insert(1, 1);
  REQUIRE(map.get_num_active() == 1);
  REQUIRE(map.get(1) == 1);
}

TEST_CASE("reverse purge hash map: iterator", "[frequent_items_sketch]") {
  reverse_purge_hash_map<int> map(3, 4, std::allocator<int>());
  for (int i = 0; i < 11; i++) map.adjust_or_insert(i, 1); // this should fit with no purge
  uint64_t sum = 0;
  for (auto it: map) sum += it.second;
  REQUIRE(sum == 11);
}

} /* namespace datasketches */
