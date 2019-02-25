// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/util/key_value_metadata.h"

namespace arrow {

TEST(KeyValueMetadataTest, SimpleConstruction) {
  KeyValueMetadata metadata;
  ASSERT_EQ(0, metadata.size());
}

TEST(KeyValueMetadataTest, StringVectorConstruction) {
  std::vector<std::string> keys = {"foo", "bar"};
  std::vector<std::string> values = {"bizz", "buzz"};

  KeyValueMetadata metadata(keys, values);
  ASSERT_EQ("foo", metadata.key(0));
  ASSERT_EQ("bar", metadata.key(1));
  ASSERT_EQ("bizz", metadata.value(0));
  ASSERT_EQ("buzz", metadata.value(1));
  ASSERT_EQ(2, metadata.size());
}

TEST(KeyValueMetadataTest, StringMapConstruction) {
  std::unordered_map<std::string, std::string> pairs = {{"foo", "bizz"}, {"bar", "buzz"}};
  std::unordered_map<std::string, std::string> result_map;
  result_map.reserve(pairs.size());

  KeyValueMetadata metadata(pairs);
  metadata.ToUnorderedMap(&result_map);
  ASSERT_EQ(pairs, result_map);
  ASSERT_EQ(2, metadata.size());
}

TEST(KeyValueMetadataTest, StringAppend) {
  std::vector<std::string> keys = {"foo", "bar"};
  std::vector<std::string> values = {"bizz", "buzz"};

  KeyValueMetadata metadata(keys, values);
  ASSERT_EQ("foo", metadata.key(0));
  ASSERT_EQ("bar", metadata.key(1));
  ASSERT_EQ("bizz", metadata.value(0));
  ASSERT_EQ("buzz", metadata.value(1));
  ASSERT_EQ(2, metadata.size());

  metadata.Append("purple", "orange");
  metadata.Append("blue", "red");

  ASSERT_EQ("purple", metadata.key(2));
  ASSERT_EQ("blue", metadata.key(3));

  ASSERT_EQ("orange", metadata.value(2));
  ASSERT_EQ("red", metadata.value(3));
}

TEST(KeyValueMetadataTest, Copy) {
  std::vector<std::string> keys = {"foo", "bar"};
  std::vector<std::string> values = {"bizz", "buzz"};

  KeyValueMetadata metadata(keys, values);
  auto metadata2 = metadata.Copy();
  ASSERT_TRUE(metadata.Equals(*metadata2));
}

TEST(KeyValueMetadataTest, Equals) {
  std::vector<std::string> keys = {"foo", "bar"};
  std::vector<std::string> values = {"bizz", "buzz"};

  KeyValueMetadata metadata(keys, values);
  KeyValueMetadata metadata2(keys, values);
  KeyValueMetadata metadata3(keys, {"buzz", "bizz"});

  ASSERT_TRUE(metadata.Equals(metadata2));
  ASSERT_FALSE(metadata.Equals(metadata3));
}

TEST(KeyValueMetadataTest, ToString) {
  std::vector<std::string> keys = {"foo", "bar"};
  std::vector<std::string> values = {"bizz", "buzz"};

  KeyValueMetadata metadata(keys, values);

  std::string result = metadata.ToString();
  std::string expected = R"(
-- metadata --
foo: bizz
bar: buzz)";

  ASSERT_EQ(expected, result);
}

}  // namespace arrow
