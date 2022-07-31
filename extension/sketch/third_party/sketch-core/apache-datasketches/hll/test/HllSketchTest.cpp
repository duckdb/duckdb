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

#include <stdexcept>

#include "hll.hpp"

#include <catch.hpp>
#include <test_allocator.hpp>

namespace datasketches {

using hll_sketch_test_alloc = hll_sketch_alloc<test_allocator<uint8_t>>;
using alloc = test_allocator<uint8_t>;

static void runCheckCopy(uint8_t lgConfigK, target_hll_type tgtHllType) {
  hll_sketch_test_alloc sk(lgConfigK, tgtHllType, false, 0);

  for (int i = 0; i < 7; ++i) {
    sk.update(i);
  }

  hll_sketch_test_alloc skCopy = sk;
  REQUIRE(sk.get_estimate() == skCopy.get_estimate());

  // no access to hllSketchImpl, so we'll ensure those differ by adding more
  // data to sk and ensuring the mode and estimates differ
  for (int i = 7; i < 24; ++i) {
    sk.update(i);
  }
  REQUIRE(16.0 < (sk.get_estimate() - skCopy.get_estimate()));

  skCopy = sk;
  REQUIRE(sk.get_estimate() == skCopy.get_estimate());

  int u = (sk.get_target_type() == HLL_4) ? 100000 : 25;
  for (int i = 24; i < u; ++i) {
    sk.update(i);
  }
  REQUIRE(sk.get_estimate() != skCopy.get_estimate()); // either 1 or 100k difference

  skCopy = sk;
  REQUIRE(sk.get_estimate() == skCopy.get_estimate());
}

TEST_CASE("hll sketch: check copies", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  runCheckCopy(14, HLL_4);
  runCheckCopy(8, HLL_6);
  runCheckCopy(8, HLL_8);
  REQUIRE(test_allocator_total_bytes == 0);
}

static void copyAs(target_hll_type srcType, target_hll_type dstType) {
  uint8_t lgK = 8;
  int n1 = 7;
  int n2 = 24;
  int n3 = 1000;
  int base = 0;

  hll_sketch_test_alloc src(lgK, srcType, false, 0);
  for (int i = 0; i < n1; ++i) {
    src.update(i + base);
  }
  hll_sketch_test_alloc dst(src, dstType);
  REQUIRE(src.get_estimate() == dst.get_estimate());

  for (int i = n1; i < n2; ++i) {
    src.update(i + base);
  }
  dst = hll_sketch_test_alloc(src, dstType);
  REQUIRE(src.get_estimate() == dst.get_estimate());

  for (int i = n2; i < n3; ++i) {
    src.update(i + base);
  }
  dst = hll_sketch_test_alloc(src, dstType);
  REQUIRE(src.get_estimate() == dst.get_estimate());
}

TEST_CASE("hll sketch: check copy as", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  copyAs(HLL_4, HLL_4);
  copyAs(HLL_4, HLL_6);
  copyAs(HLL_4, HLL_8);
  copyAs(HLL_6, HLL_4);
  copyAs(HLL_6, HLL_6);
  copyAs(HLL_6, HLL_8);
  copyAs(HLL_8, HLL_4);
  copyAs(HLL_8, HLL_6);
  copyAs(HLL_8, HLL_8);
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: check misc1", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  {
    uint8_t lgConfigK = 8;
    target_hll_type srcType = target_hll_type::HLL_8;
    hll_sketch_test_alloc sk(lgConfigK, srcType, false, 0);

    for (int i = 0; i < 7; ++i) { sk.update(i); } // LIST
    REQUIRE(sk.get_compact_serialization_bytes() == 36);
    REQUIRE(sk.get_updatable_serialization_bytes() == 40);

    for (int i = 7; i < 24; ++i) { sk.update(i); } // SET
    REQUIRE(sk.get_compact_serialization_bytes() == 108);
    REQUIRE(sk.get_updatable_serialization_bytes() == 140);

    sk.update(24); // HLL
    REQUIRE(sk.get_updatable_serialization_bytes() == 40 + 256);

    const auto hllBytes = hll_constants::HLL_BYTE_ARR_START + (1 << lgConfigK);
    REQUIRE(sk.get_compact_serialization_bytes() == hllBytes);
    REQUIRE(hll_sketch::get_max_updatable_serialization_bytes(lgConfigK, HLL_8) == hllBytes);
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: check num std dev", "[hll_sketch]") {
  REQUIRE_THROWS_AS(HllUtil<>::checkNumStdDev(0), std::invalid_argument);
}

void checkSerializationSizes(uint8_t lgConfigK, target_hll_type tgtHllType) {
  hll_sketch_test_alloc sk(lgConfigK, tgtHllType, false, 0);
  int i;

  // LIST
  for (i = 0; i < 7; ++i) { sk.update(i); }
  auto expected = hll_constants::LIST_INT_ARR_START + (i << 2);
  REQUIRE(sk.get_compact_serialization_bytes() == expected);
  expected = hll_constants::LIST_INT_ARR_START + (4 << hll_constants::LG_INIT_LIST_SIZE);
  REQUIRE(sk.get_updatable_serialization_bytes() == expected);

  // SET
  for (i = 7; i < 24; ++i) { sk.update(i); }
  expected = hll_constants::HASH_SET_INT_ARR_START + (i << 2);
  REQUIRE(sk.get_compact_serialization_bytes() == expected);
  expected = hll_constants::HASH_SET_INT_ARR_START + (4 << hll_constants::LG_INIT_SET_SIZE);
  REQUIRE(sk.get_updatable_serialization_bytes() == expected);
}

TEST_CASE("hll sketch: check ser sizes", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  checkSerializationSizes(8, HLL_8);
  checkSerializationSizes(8, HLL_6);
  checkSerializationSizes(8, HLL_4);
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: exercise to string", "[hll_sketch]") {
  hll_sketch sk(15, HLL_4);
  for (int i = 0; i < 25; ++i) { sk.update(i); }
  std::ostringstream oss(std::ios::binary);
  oss << sk.to_string(false, true, true, true);
  for (int i = 25; i < (1 << 20); ++i) { sk.update(i); }
  oss << sk.to_string(false, true, true, true);
  oss << sk.to_string(false, true, true, false);

  sk = hll_sketch(8, HLL_8);
  for (int i = 0; i < 25; ++i) { sk.update(i); }
  oss << sk.to_string(false, true, true, true);
}

// Creates and serializes then deserializes sketch.
// Returns true if deserialized sketch is compact.
static bool checkCompact(uint8_t lgK, const int n, const target_hll_type type, bool compact) {
  hll_sketch_test_alloc sk(lgK, type, false, 0);
  for (int i = 0; i < n; ++i) { sk.update(i); }
  
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  if (compact) { 
    sk.serialize_compact(ss);
    REQUIRE(ss.tellp() == sk.get_compact_serialization_bytes());
  } else {
    sk.serialize_updatable(ss);
    REQUIRE(ss.tellp() == sk.get_updatable_serialization_bytes());
  }
  
  hll_sketch_test_alloc sk2 = hll_sketch_test_alloc::deserialize(ss, alloc(0));
  REQUIRE(sk2.get_estimate() == Approx(n).margin(0.01));
  bool isCompact = sk2.is_compact();

  return isCompact;
}

TEST_CASE("hll sketch: check compact flag", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  {
    uint8_t lgK = 8;
    // unless/until we create non-updatable "direct" versions,
    // deserialized image should never be compact
    // LIST: follows serialization request
    REQUIRE(checkCompact(lgK, 7, HLL_8, false) == false);
    REQUIRE(checkCompact(lgK, 7, HLL_8, true) == false);

    // SET: follows serialization request
    REQUIRE(checkCompact(lgK, 24, HLL_8, false) == false);
    REQUIRE(checkCompact(lgK, 24, HLL_8, true) == false);

    // HLL8: always updatable
    REQUIRE(checkCompact(lgK, 25, HLL_8, false) == false);
    REQUIRE(checkCompact(lgK, 25, HLL_8, true) == false);

    // HLL6: always updatable
    REQUIRE(checkCompact(lgK, 25, HLL_6, false) == false);
    REQUIRE(checkCompact(lgK, 25, HLL_6, true) == false);

    // HLL4: follows serialization request
    REQUIRE(checkCompact(lgK, 25, HLL_4, false) == false);
    REQUIRE(checkCompact(lgK, 25, HLL_4, true) == false);
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: check k limits", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  {
    hll_sketch_test_alloc sketch1(hll_constants::MIN_LOG_K, target_hll_type::HLL_8, false, 0);
    hll_sketch_test_alloc sketch2(hll_constants::MAX_LOG_K, target_hll_type::HLL_4, false, 0);
    REQUIRE_THROWS_AS(hll_sketch_test_alloc(hll_constants::MIN_LOG_K - 1, target_hll_type::HLL_4, false, 0), std::invalid_argument);
    REQUIRE_THROWS_AS(hll_sketch_test_alloc(hll_constants::MAX_LOG_K + 1, target_hll_type::HLL_4, false, 0), std::invalid_argument);
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: check input types", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  {
    hll_sketch_test_alloc sk(8, target_hll_type::HLL_8, false, 0);

    // inserting the same value as a variety of input types
    sk.update((uint8_t) 102);
    sk.update((uint16_t) 102);
    sk.update((uint32_t) 102);
    sk.update((uint64_t) 102);
    sk.update((int8_t) 102);
    sk.update((int16_t) 102);
    sk.update((int32_t) 102);
    sk.update((int64_t) 102);
    REQUIRE(sk.get_estimate() == Approx(1.0).margin(0.01));

    // identical binary representations
    // no unsigned in Java, but need to sign-extend both as Java would do
    sk.update((uint8_t) 255);
    sk.update((int8_t) -1);

    sk.update((float) -2.0);
    sk.update((double) -2.0);

    std::string str = "input string";
    sk.update(str);
    sk.update(str.c_str(), str.length());
    REQUIRE(sk.get_estimate() == Approx(4.0).margin(0.01));

    sk = hll_sketch_test_alloc(8, target_hll_type::HLL_6, false, 0);
    sk.update((float) 0.0);
    sk.update((float) -0.0);
    sk.update((double) 0.0);
    sk.update((double) -0.0);
    REQUIRE(sk.get_estimate() == Approx(1.0).margin(0.01));

    sk = hll_sketch_test_alloc(8, target_hll_type::HLL_4, false, 0);
    sk.update(std::nanf("3"));
    sk.update(std::nan("9"));
    REQUIRE(sk.get_estimate() == Approx(1.0).margin(0.01));

    sk = hll_sketch_test_alloc(8, target_hll_type::HLL_4, false, 0);
    sk.update(nullptr, 0);
    sk.update("");
    REQUIRE(sk.is_empty());
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: deserialize list mode buffer overrun", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  {
    hll_sketch_test_alloc sketch(10, target_hll_type::HLL_4, false, 0);
    sketch.update(1);
    auto bytes = sketch.serialize_compact();
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), 7, 0), std::out_of_range);
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, 0), std::out_of_range);

    // ckeck for leaks on stream exceptions
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), 7));
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), bytes.size() - 1));
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: deserialize set mode buffer overrun", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  {
    hll_sketch_test_alloc sketch(10, target_hll_type::HLL_4, false, 0);
    for (int i = 0; i < 10; ++i) sketch.update(i);
    //std::cout << sketch.to_string();
    auto bytes = sketch.serialize_updatable();
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), 7, 0), std::out_of_range);
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, 0), std::out_of_range);

    // ckeck for leaks on stream exceptions
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), 7));
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), bytes.size() - 1));
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: deserialize HLL mode buffer overrun", "[hll_sketch]") {
  test_allocator_total_bytes = 0;
  {
    // this sketch should have aux table
    hll_sketch_test_alloc sketch(15, target_hll_type::HLL_4, false, 0);
    for (int i = 0; i < 14444; ++i) sketch.update(i);
    //std::cout << sketch.to_string();
    auto bytes = sketch.serialize_compact();
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), 7, 0), std::out_of_range);
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), 15, 0), std::out_of_range);
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), 16420, 0), std::out_of_range); // before aux table
    REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(bytes.data(), bytes.size() - 1, 0), std::out_of_range);

    // ckeck for leaks on stream exceptions
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), 7));
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), 15));
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), 16420)); // before aux table
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
    {
      std::stringstream ss;
      ss.exceptions(std::ios::failbit | std::ios::badbit);
      ss.str(std::string((char*)bytes.data(), bytes.size() - 1));
      REQUIRE_THROWS_AS(hll_sketch_test_alloc::deserialize(ss, alloc(0)), std::ios_base::failure);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: bytes serialize-deserialize-serialize list mode") {
  test_allocator_total_bytes = 0;
  {
    hll_sketch_test_alloc s1(10, target_hll_type::HLL_4, false, 0);
    s1.update(1);
    s1.update(2);
    s1.update(3);
    std::cout << s1.to_string();
    auto bytes1 = s1.serialize_compact();
    auto s2 = hll_sketch_test_alloc::deserialize(bytes1.data(), bytes1.size(), 0);
    auto bytes2 = s2.serialize_compact();
    REQUIRE(bytes1 == bytes2);
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: updatable bytes serialize-deserialize-serialize set mode") {
  test_allocator_total_bytes = 0;
  {
    hll_sketch_test_alloc s1(10, target_hll_type::HLL_4, false, 0);
    for (int i = 0; i < 10; ++i) s1.update(i);
    std::cout << s1.to_string();
    auto bytes1 = s1.serialize_updatable();
    auto s2 = hll_sketch_test_alloc::deserialize(bytes1.data(), bytes1.size(), 0);

    auto bytes2 = s2.serialize_updatable();
    REQUIRE(bytes1 == bytes2);
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

TEST_CASE("hll sketch: compact bytes serialize-deserialize-serialize set mode") {
  test_allocator_total_bytes = 0;
  {
    hll_sketch_test_alloc s1(10, target_hll_type::HLL_4, false, 0);
    for (int i = 0; i < 10; ++i) s1.update(i);
    std::cout << s1.to_string();
    auto bytes1 = s1.serialize_compact();
    auto s2 = hll_sketch_test_alloc::deserialize(bytes1.data(), bytes1.size(), 0);

    // cannot just compare bytes here
    // hash set does not preserve the order after reconstruction in compact mode
    // add more to push them to HLL mode
    for (int i = 10; i < 100; ++i) {
      s1.update(i);
      s2.update(i);
    }
    std::cout << s1.to_string();
    std::cout << s2.to_string();

    auto bytes2 = s1.serialize_compact();
    auto bytes3 = s2.serialize_compact();
    REQUIRE(bytes2 == bytes3);
  }
  REQUIRE(test_allocator_total_bytes == 0);
}

} /* namespace datasketches */
