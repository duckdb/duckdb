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

#include <sstream>

#include <gtest/gtest.h>

#include "arrow/status.h"

namespace arrow {

TEST(StatusTest, TestCodeAndMessage) {
  Status ok = Status::OK();
  ASSERT_EQ(StatusCode::OK, ok.code());
  Status file_error = Status::IOError("file error");
  ASSERT_EQ(StatusCode::IOError, file_error.code());
  ASSERT_EQ("file error", file_error.message());
}

TEST(StatusTest, TestToString) {
  Status file_error = Status::IOError("file error");
  ASSERT_EQ("IOError: file error", file_error.ToString());

  std::stringstream ss;
  ss << file_error;
  ASSERT_EQ(file_error.ToString(), ss.str());
}

TEST(StatusTest, AndStatus) {
  Status a = Status::OK();
  Status b = Status::OK();
  Status c = Status::Invalid("invalid value");
  Status d = Status::IOError("file error");

  Status res;
  res = a & b;
  ASSERT_TRUE(res.ok());
  res = a & c;
  ASSERT_TRUE(res.IsInvalid());
  res = d & c;
  ASSERT_TRUE(res.IsIOError());

  res = Status::OK();
  res &= c;
  ASSERT_TRUE(res.IsInvalid());
  res &= d;
  ASSERT_TRUE(res.IsInvalid());

  // With rvalues
  res = Status::OK() & Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::OK();
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());

  res = Status::OK();
  res &= Status::OK();
  ASSERT_TRUE(res.ok());
  res &= Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res &= Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());
}

}  // namespace arrow
