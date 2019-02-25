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

#include <string>
#include <vector>

#include "arrow/util/decimal.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace Decimal {

static void BM_FromString(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<std::string> values = {"0", "1.23", "12.345e6", "-12.345e-6"};

  while (state.KeepRunning()) {
    for (const auto& value : values) {
      Decimal128 dec;
      int32_t scale, precision;
      ARROW_UNUSED(Decimal128::FromString(value, &dec, &scale, &precision));
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

BENCHMARK(BM_FromString)->Repetitions(3)->Unit(benchmark::kMicrosecond);

}  // namespace Decimal
}  // namespace arrow
