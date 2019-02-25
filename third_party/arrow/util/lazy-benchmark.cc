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
#include <iterator>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/testing/util.h"
#include "arrow/util/lazy.h"

namespace arrow {

static constexpr int64_t kSize = 100000000;

template <typename T = int32_t>
std::vector<T> generate_junk(int64_t size) {
  std::vector<T> v(size);
  randint(size, 0, 100000, &v);
  return v;
}

// Baseline
void BM_for_loop(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);

  for (auto _ : state) {
    for (int64_t index = 0; index < kSize; ++index) target[index] = source[index] + 1;
  }
}

BENCHMARK(BM_for_loop)->Repetitions(3)->Unit(benchmark::kMillisecond);

// For comparison: pure copy without any changes
void BM_std_copy(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);

  for (auto _ : state) {
    std::copy(source.begin(), source.end(), target.begin());
  }
}

BENCHMARK(BM_std_copy)->Repetitions(3)->Unit(benchmark::kMillisecond);

// For comparison: pure copy with type convesion.
void BM_std_copy_converting(benchmark::State& state) {
  auto source = generate_junk<int32_t>(kSize);
  // bigger type to avoid warnings
  std::vector<int64_t> target(kSize);

  for (auto _ : state) {
    std::copy(source.begin(), source.end(), target.begin());
  }
}

BENCHMARK(BM_std_copy_converting)->Repetitions(3)->Unit(benchmark::kMillisecond);

// std::copy with a lazy range as a source
void BM_lazy_copy(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);
  auto lazy_range = internal::MakeLazyRange(
      [&source](int64_t index) { return source[index]; }, source.size());

  for (auto _ : state) {
    std::copy(lazy_range.begin(), lazy_range.end(), target.begin());
  }
}

BENCHMARK(BM_lazy_copy)->Repetitions(3)->Unit(benchmark::kMillisecond);

// std::copy with a lazy range which does static cast.
// Should be the same performance as std::copy with differtly typed iterators
void BM_lazy_copy_converting(benchmark::State& state) {
  auto source = generate_junk<int64_t>(kSize);
  std::vector<int32_t> target(kSize);
  auto lazy_range = internal::MakeLazyRange(
      [&source](int64_t index) { return static_cast<int32_t>(source[index]); },
      source.size());

  for (auto _ : state) {
    std::copy(lazy_range.begin(), lazy_range.end(), target.begin());
  }
}

BENCHMARK(BM_lazy_copy_converting)->Repetitions(3)->Unit(benchmark::kMillisecond);

// For loop with a post-increment of a lazy operator
void BM_lazy_postinc(benchmark::State& state) {
  auto source = generate_junk(kSize);
  std::vector<int> target(kSize);
  auto lazy_range = internal::MakeLazyRange(
      [&source](int64_t index) { return source[index]; }, source.size());

  for (auto _ : state) {
    auto lazy_iter = lazy_range.begin();
    auto lazy_end = lazy_range.end();
    auto target_iter = target.begin();

    while (lazy_iter != lazy_end) *(target_iter++) = *(lazy_iter++);
  }
}

BENCHMARK(BM_lazy_postinc)->Repetitions(3)->Unit(benchmark::kMillisecond);

}  // namespace arrow
