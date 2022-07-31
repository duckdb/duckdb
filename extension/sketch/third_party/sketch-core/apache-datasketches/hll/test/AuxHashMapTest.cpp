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
#include <memory>
#include <stdexcept>

#include "AuxHashMap.hpp"

namespace datasketches {

TEST_CASE("aux hash map: check must replace", "[aux_hash_map]") {
  AuxHashMap<std::allocator<uint8_t>>* map = new AuxHashMap<std::allocator<uint8_t>>(3, 7, std::allocator<uint8_t>());
  map->mustAdd(100, 5);
  int val = map->mustFindValueFor(100);
  REQUIRE(val == 5);

  map->mustReplace(100, 10);
  val = map->mustFindValueFor(100);
  REQUIRE(val == 10);

  REQUIRE_THROWS_AS(map->mustReplace(101, 5), std::invalid_argument);

  delete map;
}

TEST_CASE("aux hash map: check grow space", "[aux_hash_map]") {
  auto map = std::unique_ptr<AuxHashMap<std::allocator<uint8_t>>, std::function<void(AuxHashMap<std::allocator<uint8_t>>*)>>(
      AuxHashMap<std::allocator<uint8_t>>::newAuxHashMap(3, 7, std::allocator<uint8_t>()),
      AuxHashMap<std::allocator<uint8_t>>::make_deleter()
      );
  REQUIRE(map->getLgAuxArrInts() == 3);
  for (uint8_t i = 1; i <= 7; ++i) {
    map->mustAdd(i, i);
  }
  REQUIRE(map->getLgAuxArrInts() == 4);
  auto itr = map->begin(true);
  int count1 = 0;
  int count2 = 0;
  while (itr != map->end()) {
    ++count2;
    int pair = *itr;
    if (pair != 0) { ++count1; }
    ++itr;
  }
  REQUIRE(count1 == 7);
  REQUIRE(count2 == 16);
}

TEST_CASE("aux hash map: check exception must find value for", "[aux_hash_map]") {
  AuxHashMap<std::allocator<uint8_t>> map(3, 7, std::allocator<uint8_t>());
  map.mustAdd(100, 5);
  REQUIRE_THROWS_AS(map.mustFindValueFor(101), std::invalid_argument);
}

TEST_CASE("aux hash map: check exception must add", "[aux_hash_map]") {
  AuxHashMap<std::allocator<uint8_t>>* map = AuxHashMap<std::allocator<uint8_t>>::newAuxHashMap(3, 7, std::allocator<uint8_t>());
  map->mustAdd(100, 5);
  REQUIRE_THROWS_AS(map->mustAdd(100, 6), std::invalid_argument);
  
  AuxHashMap<std::allocator<uint8_t>>::make_deleter()(map);
}

} /* namespace datasketches */
