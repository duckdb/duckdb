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


#include <cstring>
#include <sstream>
#include <fstream>
#include <stdexcept>

#include <catch.hpp>

#include "cpc_sketch.hpp"

namespace datasketches {

static const double RELATIVE_ERROR_FOR_LG_K_11 = 0.02;

TEST_CASE("cpc sketch: lg k limits", "[cpc_sketch]") {
  cpc_sketch s1(CPC_MIN_LG_K); // this should work
  cpc_sketch s2(CPC_MAX_LG_K); // this should work
  REQUIRE_THROWS_AS(cpc_sketch(CPC_MIN_LG_K - 1), std::invalid_argument);
  REQUIRE_THROWS_AS(cpc_sketch(CPC_MAX_LG_K + 1), std::invalid_argument);
}

TEST_CASE("cpc sketch: empty", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  REQUIRE(sketch.is_empty());
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
  REQUIRE(sketch.validate());
}

TEST_CASE("cpc sketch: one value", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  sketch.update(1);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_estimate() == Approx(1).margin(RELATIVE_ERROR_FOR_LG_K_11));
  REQUIRE(sketch.get_estimate() >= sketch.get_lower_bound(1));
  REQUIRE(sketch.get_estimate() <= sketch.get_upper_bound(1));
  REQUIRE(sketch.validate());
}

TEST_CASE("cpc sketch: many values", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(10000);
  for (int i = 0; i < n; i++) sketch.update(i);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_estimate() == Approx(n).margin(n * RELATIVE_ERROR_FOR_LG_K_11));
  REQUIRE(sketch.get_estimate() >= sketch.get_lower_bound(1));
  REQUIRE(sketch.get_estimate() <= sketch.get_upper_bound(1));
  REQUIRE(sketch.validate());
}

TEST_CASE("cpc sketch: overflow bug", "[cpc_sketch]") {
  cpc_sketch sketch(12);
  const int n = 100000000;
  uint64_t key = 15200000000; // problem happened with this sequence
  for (int i = 0; i < n; i++) sketch.update(key++);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.1));
  REQUIRE(sketch.get_estimate() >= sketch.get_lower_bound(1));
  REQUIRE(sketch.get_estimate() <= sketch.get_upper_bound(1));
  REQUIRE(sketch.validate());
}

TEST_CASE("cpc sketch: serialize deserialize empty", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  cpc_sketch deserialized = cpc_sketch::deserialize(s);
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  std::ofstream os("cpc-empty.bin");
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: serialize deserialize sparse", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(100);
  for (int i = 0; i < n; i++) sketch.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  cpc_sketch deserialized = cpc_sketch::deserialize(s);
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  std::ofstream os("cpc-sparse.bin");
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: serialize deserialize hybrid", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(200);
  for (int i = 0; i < n; i++) sketch.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  cpc_sketch deserialized = cpc_sketch::deserialize(s);
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  std::ofstream os("cpc-hybrid.bin");
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: serialize deserialize pinned", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(2000);
  for (int i = 0; i < n; i++) sketch.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  cpc_sketch deserialized = cpc_sketch::deserialize(s);
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  std::ofstream os("cpc-pinned.bin");
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: serialize deserialize sliding", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(20000);
  for (int i = 0; i < n; i++) sketch.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  cpc_sketch deserialized = cpc_sketch::deserialize(s);
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  std::ofstream os("cpc-sliding.bin");
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: serializing deserialize sliding large", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(3000000);
  for (int i = 0; i < n; i++) sketch.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  cpc_sketch deserialized = cpc_sketch::deserialize(s);
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  std::ofstream os("cpc-sliding-large.bin");
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: serialize deserialize empty, bytes", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  auto bytes = sketch.serialize();
  cpc_sketch deserialized = cpc_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);

  std::ofstream os("cpc-empty.bin");
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: serialize deserialize sparse, bytes", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(100);
  for (int i = 0; i < n; i++) sketch.update(i);
  auto bytes = sketch.serialize();
  cpc_sketch deserialized = cpc_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 15), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
}

TEST_CASE("cpc sketch: serialize deserialize hybrid, bytes", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(200);
  for (int i = 0; i < n; i++) sketch.update(i);
  auto bytes = sketch.serialize();
  cpc_sketch deserialized = cpc_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 15), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
}

TEST_CASE("cpc sketch: serialize deserialize pinned, bytes", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(2000);
  for (int i = 0; i < n; i++) sketch.update(i);
  auto bytes = sketch.serialize();
  cpc_sketch deserialized = cpc_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 15), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  std::cout << sketch.to_string();
}

TEST_CASE("cpc sketch: serialize deserialize sliding, bytes", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(20000);
  for (int i = 0; i < n; i++) sketch.update(i);
  auto bytes = sketch.serialize();
  cpc_sketch deserialized = cpc_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 15), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
}

TEST_CASE("cpc sketch: serialize deserialize sliding huge", "[cpc_sketch]") {
  cpc_sketch sketch(26);
  const int n = 10000000;
  for (int i = 0; i < n; i++) sketch.update(i);
  REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.001));
  auto bytes = sketch.serialize();
  cpc_sketch deserialized = cpc_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), 15), std::out_of_range);
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);

  // updating again with the same values should not change the sketch
  for (int i = 0; i < n; i++) deserialized.update(i);
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());
}

TEST_CASE("cpc sketch: copy", "[cpc_sketch]") {
  cpc_sketch s1(11);
  s1.update(1);
  cpc_sketch s2 = s1; // copy constructor
  REQUIRE_FALSE(s2.is_empty());
  REQUIRE(s2.get_estimate() == Approx(1).margin(RELATIVE_ERROR_FOR_LG_K_11));
  s2.update(2);
  s1 = s2; // operator=
  REQUIRE(s1.get_estimate() == Approx(2).margin(RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc sketch: serialize deserialize empty, custom seed", "[cpc_sketch]") {
  cpc_sketch sketch(11, 123);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  cpc_sketch deserialized = cpc_sketch::deserialize(s, 123);
  REQUIRE(deserialized.is_empty() == sketch.is_empty());
  REQUIRE(deserialized.get_estimate() == sketch.get_estimate());
  REQUIRE(deserialized.validate());

  // incompatible seed
  s.seekg(0); // rewind the stream to read the same sketch again
  REQUIRE_THROWS_AS(cpc_sketch::deserialize(s), std::invalid_argument);
}

TEST_CASE("cpc sketch: kapp range", "[cpc_sketch]") {
  cpc_sketch s(11);
  REQUIRE(s.get_lower_bound(1) == 0.0);
  REQUIRE(s.get_upper_bound(1) == 0.0);
  REQUIRE(s.get_lower_bound(2) == 0.0);
  REQUIRE(s.get_upper_bound(2) == 0.0);
  REQUIRE(s.get_lower_bound(3) == 0.0);
  REQUIRE(s.get_upper_bound(3) == 0.0);
  REQUIRE_THROWS_AS(s.get_lower_bound(4), std::invalid_argument);
  REQUIRE_THROWS_AS(s.get_upper_bound(4), std::invalid_argument);
}

TEST_CASE("cpc sketch: validate fail", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(2000);
  for (int i = 0; i < n; i++) sketch.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  s.seekp(700); // the stream should be 856 bytes long. corrupt it somewhere before the end
  s << "corrupt data";
  cpc_sketch deserialized = cpc_sketch::deserialize(s);
  REQUIRE_FALSE(deserialized.validate());
}

TEST_CASE("cpc sketch: serialize both ways", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const int n(2000);
  for (int i = 0; i < n; i++) sketch.update(i);
  const int header_size_bytes = 4;
  auto bytes = sketch.serialize(header_size_bytes);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch.serialize(s);
  REQUIRE(static_cast<size_t>(s.tellp()) == bytes.size() - header_size_bytes);

  char* pp = new char[s.tellp()];
  s.read(pp, s.tellp());
  REQUIRE(std::memcmp(pp, bytes.data() + header_size_bytes, bytes.size() - header_size_bytes) == 0);
  delete [] pp;
}

TEST_CASE("cpc sketch: update int equivalence", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  sketch.update((uint64_t) -1);
  sketch.update((int64_t) -1);
  sketch.update((uint32_t) -1);
  sketch.update((int32_t) -1);
  sketch.update((uint16_t) -1);
  sketch.update((int16_t) -1);
  sketch.update((uint8_t) -1);
  sketch.update((int8_t) -1);
  REQUIRE(sketch.get_estimate() == Approx(1).margin(RELATIVE_ERROR_FOR_LG_K_11));
  std::ofstream os("cpc-negative-one.bin"); // to compare with Java
  sketch.serialize(os);
}

TEST_CASE("cpc sketch: update float equivalence", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  sketch.update((float) 1);
  sketch.update((double) 1);
  REQUIRE(sketch.get_estimate() == Approx(1).margin(RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc sketch: update string equivalence", "[cpc_sketch]") {
  cpc_sketch sketch(11);
  const std::string a("a");
  sketch.update(a);
  sketch.update(a.c_str(), a.length());
  REQUIRE(sketch.get_estimate() == Approx(1).margin(RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc sketch: max serialized size", "[cpc_sketch]") {
  REQUIRE(cpc_sketch::get_max_serialized_size_bytes(4) == 24 + 40);
  REQUIRE(cpc_sketch::get_max_serialized_size_bytes(26) == static_cast<size_t>((0.6 * (1 << 26)) + 40));
}

} /* namespace datasketches */
