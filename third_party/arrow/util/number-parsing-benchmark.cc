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

#include <limits>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/parsing.h"

namespace arrow {
namespace internal {

template <typename c_int>
static std::vector<std::string> MakeIntStrings(int32_t num_items) {
  using c_int_limits = std::numeric_limits<c_int>;
  std::vector<std::string> base_strings = {"0",
                                           "5",
                                           c_int_limits::is_signed ? "-12" : "12",
                                           "34",
                                           "99",
                                           c_int_limits::is_signed ? "-111" : "111",
                                           std::to_string(c_int_limits::min()),
                                           std::to_string(c_int_limits::max())};
  std::vector<std::string> strings;
  for (int32_t i = 0; i < num_items; ++i) {
    strings.push_back(base_strings[i % base_strings.size()]);
  }
  return strings;
}

static std::vector<std::string> MakeFloatStrings(int32_t num_items) {
  std::vector<std::string> base_strings = {"0.0",         "5",        "-12.3",
                                           "98765430000", "3456.789", "0.0012345",
                                           "2.34567e8",   "-5.67e-8"};
  std::vector<std::string> strings;
  for (int32_t i = 0; i < num_items; ++i) {
    strings.push_back(base_strings[i % base_strings.size()]);
  }
  return strings;
}

static std::vector<std::string> MakeTimestampStrings(int32_t num_items) {
  std::vector<std::string> base_strings = {"2018-11-13 17:11:10", "2018-11-13 11:22:33",
                                           "2016-02-29 11:22:33"};

  std::vector<std::string> strings;
  for (int32_t i = 0; i < num_items; ++i) {
    strings.push_back(base_strings[i % base_strings.size()]);
  }
  return strings;
}

template <typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
static void BM_IntegerParsing(benchmark::State& state) {  // NOLINT non-const reference
  auto strings = MakeIntStrings<C_TYPE>(1000);
  StringConverter<ARROW_TYPE> converter;

  while (state.KeepRunning()) {
    C_TYPE total = 0;
    for (const auto& s : strings) {
      C_TYPE value;
      if (!converter(s.data(), s.length(), &value)) {
        std::cerr << "Conversion failed for '" << s << "'";
        std::abort();
      }
      total = static_cast<C_TYPE>(total + value);
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * strings.size());
}

template <typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
static void BM_FloatParsing(benchmark::State& state) {  // NOLINT non-const reference
  auto strings = MakeFloatStrings(1000);
  StringConverter<ARROW_TYPE> converter;

  while (state.KeepRunning()) {
    C_TYPE total = 0;
    for (const auto& s : strings) {
      C_TYPE value;
      if (!converter(s.data(), s.length(), &value)) {
        std::cerr << "Conversion failed for '" << s << "'";
        std::abort();
      }
      total += value;
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * strings.size());
}

template <TimeUnit::type UNIT>
static void BM_TimestampParsing(benchmark::State& state) {  // NOLINT non-const reference
  using c_type = TimestampType::c_type;

  auto strings = MakeTimestampStrings(1000);
  auto type = timestamp(UNIT);
  StringConverter<TimestampType> converter(type);

  while (state.KeepRunning()) {
    c_type total = 0;
    for (const auto& s : strings) {
      c_type value;
      if (!converter(s.data(), s.length(), &value)) {
        std::cerr << "Conversion failed for '" << s << "'";
        std::abort();
      }
      total += value;
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * strings.size());
}

BENCHMARK_TEMPLATE(BM_IntegerParsing, Int8Type);
BENCHMARK_TEMPLATE(BM_IntegerParsing, Int16Type);
BENCHMARK_TEMPLATE(BM_IntegerParsing, Int32Type);
BENCHMARK_TEMPLATE(BM_IntegerParsing, Int64Type);
BENCHMARK_TEMPLATE(BM_IntegerParsing, UInt8Type);
BENCHMARK_TEMPLATE(BM_IntegerParsing, UInt16Type);
BENCHMARK_TEMPLATE(BM_IntegerParsing, UInt32Type);
BENCHMARK_TEMPLATE(BM_IntegerParsing, UInt64Type);

BENCHMARK_TEMPLATE(BM_FloatParsing, FloatType);
BENCHMARK_TEMPLATE(BM_FloatParsing, DoubleType);

BENCHMARK_TEMPLATE(BM_TimestampParsing, TimeUnit::SECOND);
BENCHMARK_TEMPLATE(BM_TimestampParsing, TimeUnit::MILLI);
BENCHMARK_TEMPLATE(BM_TimestampParsing, TimeUnit::MICRO);
BENCHMARK_TEMPLATE(BM_TimestampParsing, TimeUnit::NANO);

}  // namespace internal
}  // namespace arrow
