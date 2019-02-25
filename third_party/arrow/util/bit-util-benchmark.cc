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

#include <vector>

#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit-util.h"

namespace arrow {

using internal::CopyBitmap;

namespace BitUtil {

// A naive bitmap reader implementation, meant as a baseline against
// internal::BitmapReader

class NaiveBitmapReader {
 public:
  NaiveBitmapReader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0) {}

  bool IsSet() const { return BitUtil::GetBit(bitmap_, position_); }

  bool IsNotSet() const { return !IsSet(); }

  void Next() { ++position_; }

 private:
  const uint8_t* bitmap_;
  uint64_t position_;
};

// A naive bitmap writer implementation, meant as a baseline against
// internal::BitmapWriter

class NaiveBitmapWriter {
 public:
  NaiveBitmapWriter(uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0) {}

  void Set() {
    const int64_t byte_offset = position_ / 8;
    const int64_t bit_offset = position_ % 8;
    auto bit_set_mask = (1U << bit_offset);
    bitmap_[byte_offset] = static_cast<uint8_t>(bitmap_[byte_offset] | bit_set_mask);
  }

  void Clear() {
    const int64_t byte_offset = position_ / 8;
    const int64_t bit_offset = position_ % 8;
    auto bit_clear_mask = 0xFFU ^ (1U << bit_offset);
    bitmap_[byte_offset] = static_cast<uint8_t>(bitmap_[byte_offset] & bit_clear_mask);
  }

  void Next() { ++position_; }

  void Finish() {}

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
};

static std::shared_ptr<Buffer> CreateRandomBuffer(int64_t nbytes) {
  std::shared_ptr<Buffer> buffer;
  ABORT_NOT_OK(AllocateBuffer(nbytes, &buffer));
  memset(buffer->mutable_data(), 0, nbytes);
  random_bytes(nbytes, 0, buffer->mutable_data());
  return buffer;
}

template <typename BitmapReaderType>
static void BenchmarkBitmapReader(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  const uint8_t* bitmap = buffer->data();

  for (auto _ : state) {
    {
      BitmapReaderType reader(bitmap, 0, num_bits);
      int64_t total = 0;
      for (int64_t i = 0; i < num_bits; i++) {
        total += reader.IsSet();
        reader.Next();
      }
      benchmark::DoNotOptimize(total);
    }
    {
      BitmapReaderType reader(bitmap, 0, num_bits);
      int64_t total = 0;
      for (int64_t i = 0; i < num_bits; i++) {
        total += !reader.IsNotSet();
        reader.Next();
      }
      benchmark::DoNotOptimize(total);
    }
  }
  state.SetBytesProcessed(2 * int64_t(state.iterations()) * nbytes);
}

template <typename BitmapWriterType>
static void BenchmarkBitmapWriter(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  uint8_t* bitmap = buffer->mutable_data();
  const bool pattern[] = {false, false, false, true, true, true};

  while (state.KeepRunning()) {
    int64_t pattern_index = 0;
    BitmapWriterType writer(bitmap, 0, num_bits);
    for (int64_t i = 0; i < num_bits; i++) {
      if (pattern[pattern_index++]) {
        writer.Set();
      } else {
        writer.Clear();
      }
      if (pattern_index == sizeof(pattern) / sizeof(bool)) {
        pattern_index = 0;
      }
      writer.Next();
    }
    writer.Finish();
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * nbytes);
}

template <typename GenerateBitsFunctorType>
static void BenchmarkGenerateBits(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  uint8_t* bitmap = buffer->mutable_data();
  // pattern should be the same as in BenchmarkBitmapWriter
  const bool pattern[] = {false, false, false, true, true, true};

  while (state.KeepRunning()) {
    int64_t pattern_index = 0;
    const auto generate = [&]() -> bool {
      bool b = pattern[pattern_index++];
      if (pattern_index == sizeof(pattern) / sizeof(bool)) {
        pattern_index = 0;
      }
      return b;
    };
    GenerateBitsFunctorType()(bitmap, 0, num_bits, generate);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(2 * int64_t(state.iterations()) * nbytes);
}

static void BM_NaiveBitmapReader(benchmark::State& state) {
  BenchmarkBitmapReader<NaiveBitmapReader>(state, state.range(0));
}

static void BM_BitmapReader(benchmark::State& state) {
  BenchmarkBitmapReader<internal::BitmapReader>(state, state.range(0));
}

static void BM_NaiveBitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<NaiveBitmapWriter>(state, state.range(0));
}

static void BM_BitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<internal::BitmapWriter>(state, state.range(0));
}

static void BM_FirstTimeBitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<internal::FirstTimeBitmapWriter>(state, state.range(0));
}

struct GenerateBitsFunctor {
  template <class Generator>
  void operator()(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
    return internal::GenerateBits(bitmap, start_offset, length, g);
  }
};

struct GenerateBitsUnrolledFunctor {
  template <class Generator>
  void operator()(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
    return internal::GenerateBitsUnrolled(bitmap, start_offset, length, g);
  }
};

static void BM_GenerateBits(benchmark::State& state) {
  BenchmarkGenerateBits<GenerateBitsFunctor>(state, state.range(0));
}

static void BM_GenerateBitsUnrolled(benchmark::State& state) {
  BenchmarkGenerateBits<GenerateBitsUnrolledFunctor>(state, state.range(0));
}

static void BM_CopyBitmap(benchmark::State& state) {  // NOLINT non-const reference
  const int kBufferSize = static_cast<int>(state.range(0));
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(kBufferSize);

  const int num_bits = kBufferSize * 8;
  const uint8_t* src = buffer->data();

  std::shared_ptr<Buffer> copy;
  while (state.KeepRunning()) {
    ABORT_NOT_OK(CopyBitmap(default_memory_pool(), src, state.range(1), num_bits, &copy));
  }
  state.SetBytesProcessed(state.iterations() * kBufferSize * sizeof(int8_t));
}

BENCHMARK(BM_CopyBitmap)
    ->Args({100000, 0})
    ->Args({1000000, 0})
    ->Args({100000, 4})
    ->Args({1000000, 4})
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_NaiveBitmapReader)
    ->Args({1000000})
    ->MinTime(5.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BitmapReader)->Args({1000000})->MinTime(5.0)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_NaiveBitmapWriter)
    ->Args({100000})
    ->Repetitions(2)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BitmapWriter)
    ->Args({100000})
    ->Repetitions(2)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_FirstTimeBitmapWriter)
    ->Args({100000})
    ->Repetitions(2)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_GenerateBits)
    ->Args({100000})
    ->Repetitions(2)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_GenerateBitsUnrolled)
    ->Args({100000})
    ->Repetitions(2)
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

}  // namespace BitUtil
}  // namespace arrow
