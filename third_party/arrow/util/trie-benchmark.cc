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
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/trie.h"

namespace arrow {
namespace internal {

static inline bool InlinedNullLookup(util::string_view s) {
  // An inlined version of trie lookup for a specific set of strings
  // (see AllNulls())
  auto size = s.length();
  auto data = s.data();
  if (size == 0) {
    return false;
  }
  if (size == 1) {
    return false;
  }

  auto chars = reinterpret_cast<const char*>(data);
  auto first = chars[0];
  auto second = chars[1];
  switch (first) {
    case 'N': {
      // "NA", "N/A", "NaN", "NULL"
      if (size == 2) {
        return second == 'A';
      }
      auto third = chars[2];
      if (size == 3) {
        return (second == '/' && third == 'A') || (second == 'a' && third == 'N');
      }
      if (size == 4) {
        return (second == 'U' && third == 'L' && chars[3] == 'L');
      }
      return false;
    }
    case 'n': {
      // "n/a", "nan", "null"
      if (size == 2) {
        return false;
      }
      auto third = chars[2];
      if (size == 3) {
        return (second == '/' && third == 'a') || (second == 'a' && third == 'n');
      }
      if (size == 4) {
        return (second == 'u' && third == 'l' && chars[3] == 'l');
      }
      return false;
    }
    case '1': {
      // '1.#IND', '1.#QNAN'
      if (size == 6) {
        // '#' is the most unlikely char here, check it first
        return (chars[2] == '#' && chars[1] == '.' && chars[3] == 'I' &&
                chars[4] == 'N' && chars[5] == 'D');
      }
      if (size == 7) {
        return (chars[2] == '#' && chars[1] == '.' && chars[3] == 'Q' &&
                chars[4] == 'N' && chars[5] == 'A' && chars[6] == 'N');
      }
      return false;
    }
    case '-': {
      switch (second) {
        case 'N':
          // "-NaN"
          return (size == 4 && chars[2] == 'a' && chars[3] == 'N');
        case 'n':
          // "-nan"
          return (size == 4 && chars[2] == 'a' && chars[3] == 'n');
        case '1':
          // "-1.#IND", "-1.#QNAN"
          if (size == 7) {
            return (chars[3] == '#' && chars[2] == '.' && chars[4] == 'I' &&
                    chars[5] == 'N' && chars[6] == 'D');
          }
          if (size == 8) {
            return (chars[3] == '#' && chars[2] == '.' && chars[4] == 'Q' &&
                    chars[5] == 'N' && chars[6] == 'A' && chars[7] == 'N');
          }
          return false;
        default:
          return false;
      }
    }
    case '#': {
      // "#N/A", "#N/A N/A", "#NA"
      if (size < 3 || chars[1] != 'N') {
        return false;
      }
      auto third = chars[2];
      if (size == 3) {
        return third == 'A';
      }
      if (size == 4) {
        return third == '/' && chars[3] == 'A';
      }
      if (size == 8) {
        return std::memcmp(data + 2, "/A N/A", 5) == 0;
      }
      return false;
    }
    default:
      return false;
  }
}

std::vector<std::string> AllNulls() {
  return {"#N/A",    "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND",
          "1.#QNAN", "N/A",      "NA",  "NULL",    "NaN",      "n/a",  "nan",  "null"};
}

Trie MakeNullsTrie() {
  auto nulls = AllNulls();

  TrieBuilder builder;
  for (const auto& str : AllNulls()) {
    ABORT_NOT_OK(builder.Append(str));
  }
  return builder.Finish();
}

std::vector<std::string> Expand(const std::vector<std::string>& base, size_t n) {
  std::vector<std::string> result;
  result.reserve(n);

  while (true) {
    for (const auto& v : base) {
      result.push_back(v);
      if (result.size() == n) {
        return result;
      }
    }
  }
}

static void BenchmarkTrieLookups(benchmark::State& state,  // NOLINT non-const reference
                                 const std::vector<std::string>& strings) {
  Trie trie = MakeNullsTrie();
  int32_t total = 0;

  auto lookups = Expand(strings, 100);

  for (auto _ : state) {
    for (const auto& s : lookups) {
      total += trie.Find(s);
    }
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * lookups.size());
}

static void BenchmarkInlinedTrieLookups(
    benchmark::State& state,  // NOLINT non-const reference
    const std::vector<std::string>& strings) {
  int32_t total = 0;

  auto lookups = Expand(strings, 100);

  for (auto _ : state) {
    for (const auto& s : lookups) {
      total += InlinedNullLookup(s);
    }
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * lookups.size());
}

static void BM_TrieLookupFound(benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkTrieLookups(state, {"N/A", "null", "-1.#IND", "N/A"});
}

static void BM_TrieLookupNotFound(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkTrieLookups(state, {"None", "1.0", "", "abc"});
}

static void BM_InlinedTrieLookupFound(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkInlinedTrieLookups(state, {"N/A", "null", "-1.#IND", "N/A"});
}

static void BM_InlinedTrieLookupNotFound(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkInlinedTrieLookups(state, {"None", "1.0", "", "abc"});
}

static const int kRepetitions = 2;

BENCHMARK(BM_TrieLookupFound)->Repetitions(kRepetitions);
BENCHMARK(BM_TrieLookupNotFound)->Repetitions(kRepetitions);
BENCHMARK(BM_InlinedTrieLookupFound)->Repetitions(kRepetitions);
BENCHMARK(BM_InlinedTrieLookupNotFound)->Repetitions(kRepetitions);

}  // namespace internal
}  // namespace arrow
