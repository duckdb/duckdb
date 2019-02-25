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

#include "benchmark/benchmark.h"

#include <cstdint>
#include <vector>

#include "arrow/util/int-util.h"

namespace arrow {
namespace internal {

std::vector<uint64_t> GetUIntSequence(int n_values, uint64_t addend = 0) {
  std::vector<uint64_t> values(n_values);
  for (int i = 0; i < n_values; ++i) {
    values[i] = static_cast<uint64_t>(i) + addend;
  }
  return values;
}

std::vector<int64_t> GetIntSequence(int n_values, uint64_t addend = 0) {
  std::vector<int64_t> values(n_values);
  for (int i = 0; i < n_values; ++i) {
    values[i] = static_cast<int64_t>(i) + addend;
  }
  return values;
}

std::vector<uint8_t> GetValidBytes(int n_values) {
  std::vector<uint8_t> valid_bytes(n_values);
  for (int i = 0; i < n_values; ++i) {
    valid_bytes[i] = (i % 3 == 0) ? 1 : 0;
  }
  return valid_bytes;
}

static void BM_DetectUIntWidthNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto values = GetUIntSequence(0x12345);

  while (state.KeepRunning()) {
    auto result = DetectUIntWidth(values.data(), static_cast<int64_t>(values.size()));
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(uint64_t));
}

static void BM_DetectUIntWidthNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto values = GetUIntSequence(0x12345);
  const auto valid_bytes = GetValidBytes(0x12345);

  while (state.KeepRunning()) {
    auto result = DetectUIntWidth(values.data(), valid_bytes.data(),
                                  static_cast<int64_t>(values.size()));
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(uint64_t));
}

static void BM_DetectIntWidthNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto values = GetIntSequence(0x12345, -0x1234);

  while (state.KeepRunning()) {
    auto result = DetectIntWidth(values.data(), static_cast<int64_t>(values.size()));
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(uint64_t));
}

static void BM_DetectIntWidthNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto values = GetIntSequence(0x12345, -0x1234);
  const auto valid_bytes = GetValidBytes(0x12345);

  while (state.KeepRunning()) {
    auto result = DetectIntWidth(values.data(), valid_bytes.data(),
                                 static_cast<int64_t>(values.size()));
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(uint64_t));
}

BENCHMARK(BM_DetectUIntWidthNoNulls)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DetectUIntWidthNulls)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DetectIntWidthNoNulls)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_DetectIntWidthNulls)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

}  // namespace internal
}  // namespace arrow
