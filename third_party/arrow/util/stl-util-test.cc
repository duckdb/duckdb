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

#include <vector>

#include <gtest/gtest.h>

#include "arrow/util/stl.h"

namespace arrow {
namespace internal {

TEST(StlUtilTest, VectorAddRemoveTest) {
  std::vector<int> values;
  std::vector<int> result = AddVectorElement(values, 0, 100);
  EXPECT_EQ(values.size(), 0);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], 100);

  // Add 200 at index 0 and 300 at the end.
  std::vector<int> result2 = AddVectorElement(result, 0, 200);
  result2 = AddVectorElement(result2, result2.size(), 300);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result2.size(), 3);
  EXPECT_EQ(result2[0], 200);
  EXPECT_EQ(result2[1], 100);
  EXPECT_EQ(result2[2], 300);

  // Remove 100, 300, 200
  std::vector<int> result3 = DeleteVectorElement(result2, 1);
  EXPECT_EQ(result2.size(), 3);
  EXPECT_EQ(result3.size(), 2);
  EXPECT_EQ(result3[0], 200);
  EXPECT_EQ(result3[1], 300);

  result3 = DeleteVectorElement(result3, 1);
  EXPECT_EQ(result3.size(), 1);
  EXPECT_EQ(result3[0], 200);

  result3 = DeleteVectorElement(result3, 0);
  EXPECT_TRUE(result3.empty());
}

}  // namespace internal
}  // namespace arrow
