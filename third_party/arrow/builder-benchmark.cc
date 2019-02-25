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
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit-util.h"

namespace arrow {

constexpr int64_t kFinalSize = 256;

static void BM_BuildPrimitiveArrayNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<int64_t> data(256 * 1024, 100);
  while (state.KeepRunning()) {
    Int64Builder builder;
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data(), data.size(), nullptr));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t) *
                          kFinalSize);
}

static void BM_BuildVectorNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<int64_t> data(256 * 1024, 100);
  while (state.KeepRunning()) {
    std::vector<int64_t> builder;
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      builder.insert(builder.end(), data.cbegin(), data.cend());
    }
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t) *
                          kFinalSize);
}

static void BM_BuildAdaptiveIntNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  int64_t size = static_cast<int64_t>(std::numeric_limits<int16_t>::max()) * 256;
  int64_t chunk_size = size / 8;
  std::vector<int64_t> data(size);
  for (int64_t i = 0; i < size; i++) {
    data[i] = i;
  }
  while (state.KeepRunning()) {
    AdaptiveIntBuilder builder;
    for (int64_t i = 0; i < size; i += chunk_size) {
      // Build up an array of 128 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data() + i, chunk_size, nullptr));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t));
}

static void BM_BuildAdaptiveIntNoNullsScalarAppend(
    benchmark::State& state) {  // NOLINT non-const reference
  int64_t size = static_cast<int64_t>(std::numeric_limits<int16_t>::max()) * 256;
  std::vector<int64_t> data(size);
  for (int64_t i = 0; i < size; i++) {
    data[i] = i;
  }
  while (state.KeepRunning()) {
    AdaptiveIntBuilder builder;
    for (int64_t i = 0; i < size; i++) {
      ABORT_NOT_OK(builder.Append(data[i]));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t));
}

static void BM_BuildAdaptiveUIntNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  int64_t size = static_cast<int64_t>(std::numeric_limits<uint16_t>::max()) * 256;
  int64_t chunk_size = size / 8;
  std::vector<uint64_t> data(size);
  for (uint64_t i = 0; i < static_cast<uint64_t>(size); i++) {
    data[i] = i;
  }
  while (state.KeepRunning()) {
    AdaptiveUIntBuilder builder;
    for (int64_t i = 0; i < size; i += chunk_size) {
      // Build up an array of 128 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data() + i, chunk_size, nullptr));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t));
}

static void BM_BuildAdaptiveUIntNoNullsScalarAppend(
    benchmark::State& state) {  // NOLINT non-const reference
  int64_t size = static_cast<int64_t>(std::numeric_limits<int16_t>::max()) * 256;
  std::vector<uint64_t> data(size);
  for (uint64_t i = 0; i < static_cast<uint64_t>(size); i++) {
    data[i] = i;
  }
  while (state.KeepRunning()) {
    AdaptiveUIntBuilder builder;
    for (int64_t i = 0; i < size; i++) {
      ABORT_NOT_OK(builder.Append(data[i]));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t));
}

static void BM_BuildBooleanArrayNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<uint8_t> data(2 * 1024 * 1024);
  constexpr uint8_t bit_pattern = 0xcc;  // 0b11001100
  uint64_t index = 0;
  std::generate(data.begin(), data.end(),
                [&]() -> uint8_t { return (bit_pattern >> ((index++) % 8)) & 1; });

  while (state.KeepRunning()) {
    BooleanBuilder builder;
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data(), data.size()));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * kFinalSize);
}

static void BM_BuildBinaryArray(benchmark::State& state) {  // NOLINT non-const reference
  // About 160MB
  const int64_t iterations = 1 << 24;
  std::string value = "1234567890";

  for (auto _ : state) {
    BinaryBuilder builder;
    for (int64_t i = 0; i < iterations; i++) {
      ABORT_NOT_OK(builder.Append(value));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * iterations * value.size());
}

static void BM_BuildChunkedBinaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  // About 160MB
  const int64_t iterations = 1 << 24;
  std::string value = "1234567890";

  for (auto _ : state) {
    // 1MB chunks
    const int32_t chunksize = 1 << 20;
    internal::ChunkedBinaryBuilder builder(chunksize);
    for (int64_t i = 0; i < iterations; i++) {
      ABORT_NOT_OK(builder.Append(reinterpret_cast<const uint8_t*>(value.data()),
                                  static_cast<int32_t>(value.size())));
    }
    ArrayVector out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * iterations * value.size());
}

static void BM_BuildFixedSizeBinaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  const int64_t iterations = 1 << 20;
  const int width = 10;

  auto type = fixed_size_binary(width);
  const char value[width + 1] = "1234567890";

  while (state.KeepRunning()) {
    FixedSizeBinaryBuilder builder(type);
    for (int64_t i = 0; i < iterations; i++) {
      ABORT_NOT_OK(builder.Append(value));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * iterations * width);
}

// ----------------------------------------------------------------------
// DictionaryBuilder benchmarks

// Testing with different distributions of integer values helps stress
// the hash table's robustness.

// Make a vector out of `n_distinct` sequential int values
template <class Integer>
static std::vector<Integer> MakeSequentialIntDictFodder(int32_t n_values,
                                                        int32_t n_distinct) {
  std::default_random_engine gen(42);
  std::vector<Integer> values(n_values);
  {
    std::uniform_int_distribution<Integer> values_dist(0, n_distinct - 1);
    std::generate(values.begin(), values.end(), [&]() { return values_dist(gen); });
  }
  return values;
}

// Make a vector out of `n_distinct` int values with potentially colliding hash
// entries as only their highest bits differ.
template <class Integer>
static std::vector<Integer> MakeSimilarIntDictFodder(int32_t n_values,
                                                     int32_t n_distinct) {
  std::default_random_engine gen(42);
  std::vector<Integer> values(n_values);
  {
    std::uniform_int_distribution<Integer> values_dist(0, n_distinct - 1);
    auto max_int = std::numeric_limits<Integer>::max();
    auto multiplier = static_cast<Integer>(BitUtil::NextPower2(max_int / n_distinct / 2));
    std::generate(values.begin(), values.end(),
                  [&]() { return multiplier * values_dist(gen); });
  }
  return values;
}

// Make a vector out of `n_distinct` random int values
template <class Integer>
static std::vector<Integer> MakeRandomIntDictFodder(int32_t n_values,
                                                    int32_t n_distinct) {
  std::default_random_engine gen(42);
  std::vector<Integer> values_dict(n_distinct);
  std::vector<Integer> values(n_values);

  {
    std::uniform_int_distribution<Integer> values_dist(
        0, std::numeric_limits<Integer>::max());
    std::generate(values_dict.begin(), values_dict.end(),
                  [&]() { return static_cast<Integer>(values_dist(gen)); });
  }
  {
    std::uniform_int_distribution<int32_t> indices_dist(0, n_distinct - 1);
    std::generate(values.begin(), values.end(),
                  [&]() { return values_dict[indices_dist(gen)]; });
  }
  return values;
}

// Make a vector out of `n_distinct` string values
static std::vector<std::string> MakeStringDictFodder(int32_t n_values,
                                                     int32_t n_distinct) {
  std::default_random_engine gen(42);
  std::vector<std::string> values_dict(n_distinct);
  std::vector<std::string> values(n_values);

  {
    auto it = values_dict.begin();
    // Add empty string
    *it++ = "";
    // Add a few similar strings
    *it++ = "abc";
    *it++ = "abcdef";
    *it++ = "abcfgh";
    // Add random strings
    std::uniform_int_distribution<int32_t> length_dist(2, 20);
    std::independent_bits_engine<std::default_random_engine, 8, uint16_t> bytes_gen(42);

    std::generate(it, values_dict.end(), [&] {
      auto length = length_dist(gen);
      std::string s(length, 'X');
      for (int32_t i = 0; i < length; ++i) {
        s[i] = static_cast<char>(bytes_gen());
      }
      return s;
    });
  }
  {
    std::uniform_int_distribution<int32_t> indices_dist(0, n_distinct - 1);
    std::generate(values.begin(), values.end(),
                  [&] { return values_dict[indices_dist(gen)]; });
  }
  return values;
}

template <class DictionaryBuilderType, class Scalar>
static void BenchmarkScalarDictionaryArray(
    benchmark::State& state,  // NOLINT non-const reference
    const std::vector<Scalar>& fodder) {
  while (state.KeepRunning()) {
    DictionaryBuilder<Int64Type> builder(default_memory_pool());
    for (const auto value : fodder) {
      ABORT_NOT_OK(builder.Append(value));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * fodder.size() * sizeof(Scalar));
}

static void BM_BuildInt64DictionaryArrayRandom(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeRandomIntDictFodder<int64_t>(10000, 100);
  BenchmarkScalarDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BM_BuildInt64DictionaryArraySequential(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeSequentialIntDictFodder<int64_t>(10000, 100);
  BenchmarkScalarDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BM_BuildInt64DictionaryArraySimilar(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeSimilarIntDictFodder<int64_t>(10000, 100);
  BenchmarkScalarDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BM_BuildStringDictionaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeStringDictFodder(10000, 100);
  auto type = binary();
  auto fodder_size =
      std::accumulate(fodder.begin(), fodder.end(), static_cast<size_t>(0),
                      [&](size_t acc, const std::string& s) { return acc + s.size(); });

  while (state.KeepRunning()) {
    BinaryDictionaryBuilder builder(default_memory_pool());
    for (const auto& value : fodder) {
      ABORT_NOT_OK(builder.Append(value));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * fodder_size);
}

// ----------------------------------------------------------------------
// Benchmark declarations

static constexpr int32_t kRepetitions = 2;

BENCHMARK(BM_BuildPrimitiveArrayNoNulls)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildVectorNoNulls)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildBooleanArrayNoNulls)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildAdaptiveIntNoNulls)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildAdaptiveIntNoNullsScalarAppend)
    ->Repetitions(3)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildAdaptiveUIntNoNulls)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildAdaptiveUIntNoNullsScalarAppend)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildBinaryArray)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildChunkedBinaryArray)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildFixedSizeBinaryArray)->MinTime(3.0)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildInt64DictionaryArrayRandom)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildInt64DictionaryArraySequential)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildInt64DictionaryArraySimilar)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildStringDictionaryArray)
    ->Repetitions(kRepetitions)
    ->Unit(benchmark::kMicrosecond);

}  // namespace arrow
