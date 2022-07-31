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

#include "hll.hpp"
#include "CouponHashSet.hpp"
#include "HllUtil.hpp"

#include <catch.hpp>
#include <ostream>
#include <cmath>
#include <string>
#include <exception>
#include <stdexcept>

namespace datasketches {

TEST_CASE("coupon hash set: check corrupt bytearray", "[coupon_hash_set]") {
  uint8_t lgK = 8;
  hll_sketch sk1(lgK);
  for (int i = 0; i < 24; ++i) {
    sk1.update(i);
  }
  auto sketchBytes = sk1.serialize_updatable();
  uint8_t* bytes = sketchBytes.data();
  const size_t size = sketchBytes.size();

  bytes[hll_constants::PREAMBLE_INTS_BYTE] = 0;
  // fail in HllSketchImpl
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  // fail in CouponHashSet
  REQUIRE_THROWS_AS(CouponHashSet<std::allocator<uint8_t>>::newSet(bytes, size, std::allocator<uint8_t>()), std::invalid_argument);
  bytes[hll_constants::PREAMBLE_INTS_BYTE] = hll_constants::HASH_SET_PREINTS;

  bytes[hll_constants::SER_VER_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::SER_VER_BYTE] = hll_constants::SER_VER;

  bytes[hll_constants::FAMILY_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::FAMILY_BYTE] = hll_constants::FAMILY_ID;

  bytes[hll_constants::LG_K_BYTE] = 6;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::LG_K_BYTE] = lgK;

  uint8_t tmp = bytes[hll_constants::MODE_BYTE];
  bytes[hll_constants::MODE_BYTE] = 0x10; // HLL_6, LIST
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::MODE_BYTE] = tmp;

  tmp = bytes[hll_constants::LG_ARR_BYTE];
  bytes[hll_constants::LG_ARR_BYTE] = 0;
  hll_sketch::deserialize(bytes, size);
  // should work fine despite the corruption
  bytes[hll_constants::LG_ARR_BYTE] = tmp;

  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size - 1), std::out_of_range);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, 3), std::out_of_range);
}

TEST_CASE("coupon hash set: check corrupt stream", "[coupon_hash_set]") {
  uint8_t lgK = 9;
  hll_sketch sk1(lgK);
  for (int i = 0; i < 24; ++i) {
    sk1.update(i);
  }
  std::stringstream ss;
  sk1.serialize_compact(ss);

  ss.seekp(hll_constants::PREAMBLE_INTS_BYTE);
  ss.put(0);
  ss.seekg(0);
  // fail in HllSketchImpl
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  // fail in CouponHashSet
  REQUIRE_THROWS_AS(CouponHashSet<std::allocator<uint8_t>>::newSet(ss, std::allocator<uint8_t>()), std::invalid_argument);
  ss.seekp(hll_constants::PREAMBLE_INTS_BYTE);
  ss.put(hll_constants::HASH_SET_PREINTS);

  ss.seekp(hll_constants::SER_VER_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(hll_constants::SER_VER_BYTE);
  ss.put(hll_constants::SER_VER);

  ss.seekp(hll_constants::FAMILY_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(hll_constants::FAMILY_BYTE);
  ss.put(hll_constants::FAMILY_ID);

  ss.seekg(hll_constants::MODE_BYTE);
  auto tmp = ss.get();
  ss.seekp(hll_constants::MODE_BYTE);
  ss.put(0x22); // HLL_8, HLL
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(hll_constants::MODE_BYTE);
  ss.put((char)tmp);

  ss.seekg(hll_constants::LG_ARR_BYTE);
  tmp = ss.get();
  ss.seekp(hll_constants::LG_ARR_BYTE);
  ss.put(0);
  ss.seekg(0);
  hll_sketch::deserialize(ss);
  // should work fine despite the corruption
  ss.seekp(hll_constants::LG_ARR_BYTE);
  ss.put((char)tmp);
}


} // namespace datasketches
