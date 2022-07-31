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
#include <sstream>
#include <fstream>
#include <stdexcept>

#include "frequent_items_sketch.hpp"

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

TEST_CASE("frequent items: invalid k", "[frequent_items_sketch]") {
  REQUIRE_THROWS_AS(frequent_items_sketch<int>(2), std::invalid_argument);
}

TEST_CASE("frequent items: empty", "[frequent_items_sketch]") {
  frequent_items_sketch<int> sketch(3);
  REQUIRE(sketch.is_empty());
  REQUIRE(sketch.get_num_active_items() == 0);
  REQUIRE(sketch.get_total_weight() == 0);
}

TEST_CASE("frequent items: one item", "[frequent_items_sketch]") {
  frequent_items_sketch<std::string> sketch(3);
  sketch.update("a");
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_num_active_items() == 1);
  REQUIRE(sketch.get_total_weight() == 1);
  REQUIRE(sketch.get_estimate("a") == 1);
  REQUIRE(sketch.get_lower_bound("a") == 1);
  REQUIRE(sketch.get_upper_bound("a") == 1);
}

TEST_CASE("frequent items: several items, no resize, no purge", "[frequent_items_sketch]") {
  frequent_items_sketch<std::string> sketch(3);
  sketch.update("a");
  sketch.update("b");
  sketch.update("c");
  sketch.update("d");
  sketch.update("b");
  sketch.update("c");
  sketch.update("b");
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 7);
  REQUIRE(sketch.get_num_active_items() == 4);
  REQUIRE(sketch.get_estimate("a") == 1);
  REQUIRE(sketch.get_estimate("b") == 3);
  REQUIRE(sketch.get_estimate("c") == 2);
  REQUIRE(sketch.get_estimate("d") == 1);
}

TEST_CASE("frequent items: several items, with resize, no purge", "[frequent_items_sketch]") {
  frequent_items_sketch<std::string> sketch(4);
  sketch.update("a");
  sketch.update("b");
  sketch.update("c");
  sketch.update("d");
  sketch.update("b");
  sketch.update("c");
  sketch.update("b");
  sketch.update("e");
  sketch.update("f");
  sketch.update("g");
  sketch.update("h");
  sketch.update("i");
  sketch.update("j");
  sketch.update("k");
  sketch.update("l");
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 15);
  REQUIRE(sketch.get_num_active_items() == 12);
  REQUIRE(sketch.get_estimate("a") == 1);
  REQUIRE(sketch.get_estimate("b") == 3);
  REQUIRE(sketch.get_estimate("c") == 2);
  REQUIRE(sketch.get_estimate("d") == 1);
}

TEST_CASE("frequent items: estimation mode", "[frequent_items_sketch]") {
  frequent_items_sketch<int> sketch(3);
  sketch.update(1, 10);
  sketch.update(2);
  sketch.update(3);
  sketch.update(4);
  sketch.update(5);
  sketch.update(6);
  sketch.update(7, 15);
  sketch.update(8);
  sketch.update(9);
  sketch.update(10);
  sketch.update(11);
  sketch.update(12);
  REQUIRE(sketch.get_maximum_error() > 0); // estimation mode

  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 35);
  
  auto items = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES);
  REQUIRE(items.size() == 2); // only 2 items (1 and 7) should have counts more than 1
  REQUIRE(items[0].get_item() == 7);
  REQUIRE(items[0].get_estimate() == 15);
  REQUIRE(items[1].get_item() == 1);
  REQUIRE(items[1].get_estimate() == 10);

  items = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_NEGATIVES);
  REQUIRE(2 <= items.size()); // at least 2 items
  REQUIRE(12 >= items.size()); // but not more than 12 items
}

TEST_CASE("frequent items: merge exact mode", "[frequent_items_sketch]") {
  frequent_items_sketch<int> sketch1(3);
  sketch1.update(1);
  sketch1.update(2);
  sketch1.update(3);
  sketch1.update(4);

  frequent_items_sketch<int> sketch2(3);
  sketch1.update(2);
  sketch1.update(3);
  sketch1.update(2);

  sketch1.merge(sketch2);
  REQUIRE_FALSE(sketch1.is_empty());
  REQUIRE(sketch1.get_total_weight() == 7);
  REQUIRE(sketch1.get_num_active_items() == 4);
  REQUIRE(sketch1.get_estimate(1) == 1);
  REQUIRE(sketch1.get_estimate(2) == 3);
  REQUIRE(sketch1.get_estimate(3) == 2);
  REQUIRE(sketch1.get_estimate(4) == 1);
}

TEST_CASE("frequent items: merge estimation mode", "[frequent_items_sketch]") {
  frequent_items_sketch<int> sketch1(4);
  sketch1.update(1, 9); // to make sure it survives the purge
  sketch1.update(2);
  sketch1.update(3);
  sketch1.update(4);
  sketch1.update(5);
  sketch1.update(6);
  sketch1.update(7);
  sketch1.update(8);
  sketch1.update(9);
  sketch1.update(10);
  sketch1.update(11);
  sketch1.update(12);
  sketch1.update(13);
  sketch1.update(14);
  REQUIRE(sketch1.get_maximum_error() > 0); // estimation mode

  frequent_items_sketch<int> sketch2(4);
  sketch2.update(8);
  sketch2.update(9);
  sketch2.update(10);
  sketch2.update(11);
  sketch2.update(12);
  sketch2.update(13);
  sketch2.update(14);
  sketch2.update(15);
  sketch2.update(16);
  sketch2.update(17);
  sketch2.update(18);
  sketch2.update(19);
  sketch2.update(20);
  sketch2.update(21, 11); // to make sure it survives the purge
  REQUIRE(sketch2.get_maximum_error() > 0); // estimation mode

  sketch1.merge(sketch2);
  REQUIRE_FALSE(sketch1.is_empty());
  REQUIRE(sketch1.get_total_weight() == 46);
  REQUIRE(2 <= sketch1.get_num_active_items());

  auto items = sketch1.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES, 2);
  REQUIRE(items.size() == 2); // only 2 items (1 and 21) should be above threshold
  REQUIRE(items[0].get_item() == 21);
  REQUIRE(11 <= items[0].get_estimate()); // always overestimated
  REQUIRE(items[1].get_item() == 1);
  REQUIRE(9 <= items[1].get_estimate()); // always overestimated
}

TEST_CASE("frequent items: deserialize from java long", "[frequent_items_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "longs_sketch_from_java.sk", std::ios::binary);
  auto sketch = frequent_items_sketch<long long>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 4);
  REQUIRE(sketch.get_num_active_items() == 4);
  REQUIRE(sketch.get_estimate(1) == 1);
  REQUIRE(sketch.get_estimate(2) == 1);
  REQUIRE(sketch.get_estimate(3) == 1);
  REQUIRE(sketch.get_estimate(4) == 1);
}

TEST_CASE("frequent items: deserialize from java string", "[frequent_items_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "items_sketch_string_from_java.sk", std::ios::binary);
  auto sketch = frequent_items_sketch<std::string>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 4);
  REQUIRE(sketch.get_num_active_items() == 4);
  REQUIRE(sketch.get_estimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa") == 1);
  REQUIRE(sketch.get_estimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb") == 1);
  REQUIRE(sketch.get_estimate("ccccccccccccccccccccccccccccc") == 1);
  REQUIRE(sketch.get_estimate("ddddddddddddddddddddddddddddd") == 1);
}

TEST_CASE("frequent items: deserialize from java string, utf-8", "[frequent_items_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "items_sketch_string_utf8_from_java.sk", std::ios::binary);
  auto sketch = frequent_items_sketch<std::string>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 10);
  REQUIRE(sketch.get_num_active_items() == 4);
  REQUIRE(sketch.get_estimate("абвгд") == 1);
  REQUIRE(sketch.get_estimate("еёжзи") == 2);
  REQUIRE(sketch.get_estimate("йклмн") == 3);
  REQUIRE(sketch.get_estimate("опрст") == 4);
}

TEST_CASE("frequent items: deserialize long64 stream", "[frequent_items_sketch]") {
  frequent_items_sketch<long long> sketch1(3);
  sketch1.update(1, 1);
  sketch1.update(2, 2);
  sketch1.update(3, 3);
  sketch1.update(4, 4);
  sketch1.update(5, 5);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch1.serialize(s);
  auto sketch2 = frequent_items_sketch<long long>::deserialize(s);
  REQUIRE_FALSE(sketch2.is_empty());
  REQUIRE(sketch2.get_total_weight() == 15);
  REQUIRE(sketch2.get_num_active_items() == 5);
  REQUIRE(sketch2.get_estimate(1) == 1);
  REQUIRE(sketch2.get_estimate(2) == 2);
  REQUIRE(sketch2.get_estimate(3) == 3);
  REQUIRE(sketch2.get_estimate(4) == 4);
  REQUIRE(sketch2.get_estimate(5) == 5);
}

TEST_CASE("frequent items: serialize deserialiation long64 bytes", "[frequent_items_sketch]") {
  frequent_items_sketch<long long> sketch1(3);
  sketch1.update(1, 1);
  sketch1.update(2, 2);
  sketch1.update(3, 3);
  sketch1.update(4, 4);
  sketch1.update(5, 5);

  auto bytes = sketch1.serialize();
  auto sketch2 = frequent_items_sketch<long long>::deserialize(bytes.data(), bytes.size());
  REQUIRE_FALSE(sketch2.is_empty());
  REQUIRE(sketch2.get_total_weight() == 15);
  REQUIRE(sketch2.get_num_active_items() == 5);
  REQUIRE(sketch2.get_estimate(1) == 1);
  REQUIRE(sketch2.get_estimate(2) == 2);
  REQUIRE(sketch2.get_estimate(3) == 3);
  REQUIRE(sketch2.get_estimate(4) == 4);
  REQUIRE(sketch2.get_estimate(5) == 5);
}

TEST_CASE("frequent items: serialize deserialize string stream", "[frequent_items_sketch]") {
  frequent_items_sketch<std::string> sketch1(3);
  sketch1.update("aaaaaaaaaaaaaaaa", 1);
  sketch1.update("bbbbbbbbbbbbbbbb", 2);
  sketch1.update("cccccccccccccccc", 3);
  sketch1.update("dddddddddddddddd", 4);
  sketch1.update("eeeeeeeeeeeeeeee", 5);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch1.serialize(s);
  auto sketch2 = frequent_items_sketch<std::string>::deserialize(s);
  REQUIRE_FALSE(sketch2.is_empty());
  REQUIRE(sketch2.get_total_weight() == 15);
  REQUIRE(sketch2.get_num_active_items() == 5);
  REQUIRE(sketch2.get_estimate("aaaaaaaaaaaaaaaa") == 1);
  REQUIRE(sketch2.get_estimate("bbbbbbbbbbbbbbbb") == 2);
  REQUIRE(sketch2.get_estimate("cccccccccccccccc") == 3);
  REQUIRE(sketch2.get_estimate("dddddddddddddddd") == 4);
  REQUIRE(sketch2.get_estimate("eeeeeeeeeeeeeeee") == 5);
}

TEST_CASE("frequent items: serialize deserialize string bytes", "[frequent_items_sketch]") {
  frequent_items_sketch<std::string> sketch1(3);
  sketch1.update("aaaaaaaaaaaaaaaa", 1);
  sketch1.update("bbbbbbbbbbbbbbbb", 2);
  sketch1.update("cccccccccccccccc", 3);
  sketch1.update("dddddddddddddddd", 4);
  sketch1.update("eeeeeeeeeeeeeeee", 5);

  auto bytes = sketch1.serialize();
  auto sketch2 = frequent_items_sketch<std::string>::deserialize(bytes.data(), bytes.size());
  REQUIRE_FALSE(sketch2.is_empty());
  REQUIRE(sketch2.get_total_weight() == 15);
  REQUIRE(sketch2.get_num_active_items() == 5);
  REQUIRE(sketch2.get_estimate("aaaaaaaaaaaaaaaa") == 1);
  REQUIRE(sketch2.get_estimate("bbbbbbbbbbbbbbbb") == 2);
  REQUIRE(sketch2.get_estimate("cccccccccccccccc") == 3);
  REQUIRE(sketch2.get_estimate("dddddddddddddddd") == 4);
  REQUIRE(sketch2.get_estimate("eeeeeeeeeeeeeeee") == 5);
}

TEST_CASE("frequent items: serialize deserialize string, utf-8 stream", "[frequent_items_sketch]") {
  frequent_items_sketch<std::string> sketch1(3);
  sketch1.update("абвгд", 1);
  sketch1.update("еёжзи", 2);
  sketch1.update("йклмн", 3);
  sketch1.update("опрст", 4);
  sketch1.update("уфхцч", 5);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sketch1.serialize(s);
  auto sketch2 = frequent_items_sketch<std::string>::deserialize(s);
  REQUIRE_FALSE(sketch2.is_empty());
  REQUIRE(sketch2.get_total_weight() == 15);
  REQUIRE(sketch2.get_num_active_items() == 5);
  REQUIRE(sketch2.get_estimate("абвгд") == 1);
  REQUIRE(sketch2.get_estimate("еёжзи") == 2);
  REQUIRE(sketch2.get_estimate("йклмн") == 3);
  REQUIRE(sketch2.get_estimate("опрст") == 4);
  REQUIRE(sketch2.get_estimate("уфхцч") == 5);
}

TEST_CASE("frequent items: int64 deserialize single item buffer overrun", "[frequent_items_sketch]") {
  frequent_items_sketch<int64_t> sketch(3);
  sketch.update(1);
  auto bytes = sketch.serialize();
  REQUIRE_THROWS_AS(frequent_items_sketch<int64_t>::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("frequent items: string deserialize single item buffer overrun", "[frequent_items_sketch]") {
  frequent_items_sketch<std::string> sketch(3);
  sketch.update("a");
  auto bytes = sketch.serialize();
  REQUIRE_THROWS_AS(frequent_items_sketch<std::string>::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

} /* namespace datasketches */
