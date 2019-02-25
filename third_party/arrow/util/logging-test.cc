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

#include <chrono>
#include <cstdint>
#include <iostream>

#include <gtest/gtest.h>

#include "arrow/util/logging.h"

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging_test.cc.

namespace arrow {
namespace util {

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

// This is not really test.
// This file just print some information using the logging macro.

void PrintLog() {
  ARROW_LOG(DEBUG) << "This is the"
                   << " DEBUG"
                   << " message";
  ARROW_LOG(INFO) << "This is the"
                  << " INFO message";
  ARROW_LOG(WARNING) << "This is the"
                     << " WARNING message";
  ARROW_LOG(ERROR) << "This is the"
                   << " ERROR message";
  ARROW_CHECK(true) << "This is a ARROW_CHECK"
                    << " message but it won't show up";
  // The following 2 lines should not run since it will cause program failure.
  // ARROW_LOG(FATAL) << "This is the FATAL message";
  // ARROW_CHECK(false) << "This is a ARROW_CHECK message but it won't show up";
}

TEST(PrintLogTest, LogTestWithoutInit) {
  // Without ArrowLog::StartArrowLog, this should also work.
  PrintLog();
}

TEST(PrintLogTest, LogTestWithInit) {
  // Test empty app name.
  ArrowLog::StartArrowLog("", ArrowLogLevel::ARROW_DEBUG);
  PrintLog();
  ArrowLog::ShutDownArrowLog();
}

// This test will output large amount of logs to stderr, should be disabled in travis.
TEST(LogPerfTest, PerfTest) {
  ArrowLog::StartArrowLog("/fake/path/to/appdire/LogPerfTest", ArrowLogLevel::ARROW_ERROR,
                          "/tmp/");
  int rounds = 10000;

  int64_t start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    ARROW_LOG(DEBUG) << "This is the "
                     << "ARROW_DEBUG message";
  }
  int64_t elapsed = current_time_ms() - start_time;
  std::cout << "Testing DEBUG log for " << rounds << " rounds takes " << elapsed << " ms."
            << std::endl;

  start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    ARROW_LOG(ERROR) << "This is the "
                     << "RARROW_ERROR message";
  }
  elapsed = current_time_ms() - start_time;
  std::cout << "Testing ARROW_ERROR log for " << rounds << " rounds takes " << elapsed
            << " ms." << std::endl;

  start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    ARROW_CHECK(i >= 0) << "This is a ARROW_CHECK "
                        << "message but it won't show up";
  }
  elapsed = current_time_ms() - start_time;
  std::cout << "Testing ARROW_CHECK(true) for " << rounds << " rounds takes " << elapsed
            << " ms." << std::endl;
  ArrowLog::ShutDownArrowLog();
}

}  // namespace util
}  // namespace arrow

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
