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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/util.h"
#include "arrow/util/lazy.h"

namespace arrow {

class TestLazyIter : public ::testing::Test {
 public:
  int64_t kSize = 1000;
  void SetUp() {
    randint(kSize, 0, 1000000, &source_);
    target_.resize(kSize);
  }

 protected:
  std::vector<int> source_;
  std::vector<int> target_;
};

TEST_F(TestLazyIter, TestIncrementCopy) {
  auto add_one = [this](int64_t index) { return source_[index] + 1; };
  auto lazy_range = internal::MakeLazyRange(add_one, kSize);
  std::copy(lazy_range.begin(), lazy_range.end(), target_.begin());

  for (int64_t index = 0; index < kSize; ++index) {
    ASSERT_EQ(source_[index] + 1, target_[index]);
  }
}

TEST_F(TestLazyIter, TestPostIncrementCopy) {
  auto add_one = [this](int64_t index) { return source_[index] + 1; };
  auto lazy_range = internal::MakeLazyRange(add_one, kSize);
  auto iter = lazy_range.begin();
  auto end = lazy_range.end();
  auto target_iter = target_.begin();

  while (iter != end) {
    *(target_iter++) = *(iter++);
  }

  for (size_t index = 0, limit = source_.size(); index != limit; ++index) {
    ASSERT_EQ(source_[index] + 1, target_[index]);
  }
}
}  // namespace arrow
