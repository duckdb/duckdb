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

#include <exception>
#include <sstream>
#include <stdexcept>
#include <catch.hpp>

namespace datasketches {

static void testComposite(uint8_t lgK, const target_hll_type tgtHllType, const int n) {
  hll_union u(lgK);
  hll_sketch sk(lgK, tgtHllType);
  for (int i = 0; i < n; ++i) {
    u.update(i);
    sk.update(i);
  }
  u.update(sk); // merge
  hll_sketch res = u.get_result(target_hll_type::HLL_8);
  double est = res.get_composite_estimate();
  REQUIRE(sk.get_composite_estimate() == est);
}

TEST_CASE("hll array: check composite estimate", "[hll_array]") {
  testComposite(4, target_hll_type::HLL_8, 10000);
  testComposite(5, target_hll_type::HLL_8, 10000);
  testComposite(6, target_hll_type::HLL_8, 10000);
  testComposite(13, target_hll_type::HLL_8, 10000);
}

static void serializeDeserialize(uint8_t lgK, target_hll_type tgtHllType, const int n) {
  hll_sketch sk1(lgK, tgtHllType);

  for (int i = 0; i < n; ++i) {
    sk1.update(i);
  }
  //REQUIRE(sk1.getCurrentMode() == CurMode::HLL);

  double est1 = sk1.get_estimate();
  REQUIRE(est1 == Approx(n).margin(n * 0.03));

  // serialize as compact and updatable, deserialize, compare estimates are exact
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk1.serialize_compact(ss);
  hll_sketch sk2 = hll_sketch::deserialize(ss);
  REQUIRE(sk1.get_estimate() == sk2.get_estimate());

  ss.clear();
  sk1.serialize_updatable(ss);
  sk2 = hll_sketch::deserialize(ss);
  REQUIRE(sk1.get_estimate() == sk2.get_estimate());

  sk1.reset();
  REQUIRE(sk1.get_estimate() == 0.0);
}

TEST_CASE("hll array: check serialize deserialize", "[hll_array]") {
  uint8_t lgK = 4;
  int n = 8;
  serializeDeserialize(lgK, HLL_4, n);
  serializeDeserialize(lgK, HLL_6, n);
  serializeDeserialize(lgK, HLL_8, n);

  lgK = 15;
  n = (((1 << (lgK - 3))*3)/4) + 100;
  serializeDeserialize(lgK, HLL_4, n);
  serializeDeserialize(lgK, HLL_6, n);
  serializeDeserialize(lgK, HLL_8, n);

  lgK = 21;
  n = (((1 << (lgK - 3))*3)/4) + 1000;
  serializeDeserialize(lgK, HLL_4, n);
  serializeDeserialize(lgK, HLL_6, n);
  serializeDeserialize(lgK, HLL_8, n);
}

TEST_CASE("hll array: check is compact", "[hll_array]") {
  hll_sketch sk(4);
  for (int i = 0; i < 8; ++i) {
    sk.update(i);
  }
  REQUIRE_FALSE(sk.is_compact());
}

TEST_CASE("hll array: check corrupt bytearray", "[hll_array]") {
  uint8_t lgK = 8;
  hll_sketch sk1(lgK, HLL_8);
  for (int i = 0; i < 50; ++i) {
    sk1.update(i);
  }
  auto sketchBytes = sk1.serialize_compact();
  uint8_t* bytes = sketchBytes.data();
  const size_t size = sketchBytes.size();

  bytes[hll_constants::PREAMBLE_INTS_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  REQUIRE_THROWS_AS(HllArray<std::allocator<uint8_t>>::newHll(bytes, size, std::allocator<uint8_t>()), std::invalid_argument);
  bytes[hll_constants::PREAMBLE_INTS_BYTE] = hll_constants::HLL_PREINTS;

  bytes[hll_constants::SER_VER_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::SER_VER_BYTE] = hll_constants::SER_VER;

  bytes[hll_constants::FAMILY_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::FAMILY_BYTE] = hll_constants::FAMILY_ID;

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

TEST_CASE("hll array: check corrupt stream", "[hll_array]") {
  uint8_t lgK = 6;
  hll_sketch sk1(lgK);
  for (int i = 0; i < 50; ++i) {
    sk1.update(i);
  }
  std::stringstream ss;
  sk1.serialize_compact(ss);

  ss.seekp(hll_constants::PREAMBLE_INTS_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  REQUIRE_THROWS_AS(HllArray<std::allocator<uint8_t>>::newHll(ss, std::allocator<uint8_t>()), std::invalid_argument);
  ss.seekp(hll_constants::PREAMBLE_INTS_BYTE);
  ss.put(hll_constants::HLL_PREINTS);

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
  ss.put(0x11); // HLL_6, SET
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

} /* namespace datasketches */
