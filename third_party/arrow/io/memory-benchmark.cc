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

#ifdef _MSC_VER
#include <intrin.h>
#else
#include <immintrin.h>
#endif

#include <iostream>

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/cpu-info.h"

#include "benchmark/benchmark.h"

namespace arrow {

using internal::CpuInfo;
static CpuInfo* cpu_info = CpuInfo::GetInstance();

static const int kNumCores = cpu_info->num_cores();
static const int64_t kL1Size = cpu_info->CacheSize(CpuInfo::L1_CACHE);
static const int64_t kL2Size = cpu_info->CacheSize(CpuInfo::L2_CACHE);
static const int64_t kL3Size = cpu_info->CacheSize(CpuInfo::L3_CACHE);

constexpr size_t kMemoryPerCore = 32 * 1024 * 1024;
using BufferPtr = std::shared_ptr<Buffer>;

#ifdef ARROW_AVX512

using VectorType = __m512i;
#define VectorSet _mm512_set1_epi32
#define VectorLoad _mm512_stream_load_si512
#define VectorLoadAsm(SRC, DST) \
  asm volatile("vmovaps %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamLoad _mm512_stream_load_si512
#define VectorStreamLoadAsm(SRC, DST) \
  asm volatile("vmovntdqa %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamWrite _mm512_stream_si512

#else

#ifdef ARROW_AVX2

using VectorType = __m256i;
#define VectorSet _mm256_set1_epi32
#define VectorLoad _mm256_stream_load_si256
#define VectorLoadAsm(SRC, DST) \
  asm volatile("vmovaps %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamLoad _mm256_stream_load_si256
#define VectorStreamLoadAsm(SRC, DST) \
  asm volatile("vmovntdqa %[src], %[dst]" : [dst] "=v"(DST) : [src] "m"(SRC) :)
#define VectorStreamWrite _mm256_stream_si256

#else

using VectorType = __m128i;
#define VectorSet _mm_set1_epi32
#define VectorLoad _mm_stream_load_si128
#define VectorLoadAsm(SRC, DST) \
  asm volatile("movaps %[src], %[dst]" : [dst] "=x"(DST) : [src] "m"(SRC) :)
#define VectorStreamLoad _mm_stream_load_si128
#define VectorStreamLoadAsm(SRC, DST) \
  asm volatile("movntdqa %[src], %[dst]" : [dst] "=x"(DST) : [src] "m"(SRC) :)
#define VectorStreamWrite _mm_stream_si128

#endif
#endif

static void Read(void* src, void* dst, size_t size) {
  const auto simd = static_cast<VectorType*>(src);
  VectorType a, b, c, d;
  (void)dst;

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorLoadAsm(simd[i], a);
    VectorLoadAsm(simd[i + 1], b);
    VectorLoadAsm(simd[i + 2], c);
    VectorLoadAsm(simd[i + 3], d);
  }

  memset(&a, 0, sizeof(a));
  memset(&b, 0, sizeof(b));
  memset(&c, 0, sizeof(c));
  memset(&d, 0, sizeof(d));

  benchmark::DoNotOptimize(a + b + c + d);
}

// See http://codearcana.com/posts/2013/05/18/achieving-maximum-memory-bandwidth.html
// for the usage of stream loads/writes. Or section 6.1, page 47 of
// https://akkadia.org/drepper/cpumemory.pdf .
static void StreamRead(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(src);
  VectorType a, b, c, d;
  (void)dst;

  memset(&a, 0, sizeof(a));
  memset(&b, 0, sizeof(b));
  memset(&c, 0, sizeof(c));
  memset(&d, 0, sizeof(d));

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorStreamLoadAsm(simd[i], a);
    VectorStreamLoadAsm(simd[i + 1], b);
    VectorStreamLoadAsm(simd[i + 2], c);
    VectorStreamLoadAsm(simd[i + 3], d);
  }

  benchmark::DoNotOptimize(a + b + c + d);
}

static void StreamWrite(void* src, void* dst, size_t size) {
  auto simd = static_cast<VectorType*>(dst);
  const VectorType ones = VectorSet(1);
  (void)src;

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorStreamWrite(&simd[i], ones);
    VectorStreamWrite(&simd[i + 1], ones);
    VectorStreamWrite(&simd[i + 2], ones);
    VectorStreamWrite(&simd[i + 3], ones);
  }
}

static void StreamReadWrite(void* src, void* dst, size_t size) {
  auto src_simd = static_cast<VectorType*>(src);
  auto dst_simd = static_cast<VectorType*>(dst);

  for (size_t i = 0; i < size / sizeof(VectorType); i += 4) {
    VectorStreamWrite(&dst_simd[i], VectorStreamLoad(&src_simd[i]));
    VectorStreamWrite(&dst_simd[i + 1], VectorStreamLoad(&src_simd[i + 1]));
    VectorStreamWrite(&dst_simd[i + 2], VectorStreamLoad(&src_simd[i + 2]));
    VectorStreamWrite(&dst_simd[i + 3], VectorStreamLoad(&src_simd[i + 3]));
  }
}

static void PlatformMemcpy(void* src, void* dst, size_t size) { memcpy(src, dst, size); }

using ApplyFn = decltype(Read);

template <ApplyFn Apply>
static void MemoryBandwidth(benchmark::State& state) {  // NOLINT non-const reference
  const size_t buffer_size = state.range(0);
  BufferPtr src, dst;

  ABORT_NOT_OK(AllocateBuffer(buffer_size, &dst));
  ABORT_NOT_OK(AllocateBuffer(buffer_size, &src));
  random_bytes(buffer_size, 0, src->mutable_data());

  while (state.KeepRunning()) {
    Apply(src->mutable_data(), dst->mutable_data(), buffer_size);
  }

  state.SetBytesProcessed(state.iterations() * buffer_size);
}

static void SetCacheBandwidthArgs(benchmark::internal::Benchmark* bench) {
  auto cache_sizes = {kL1Size, kL2Size, kL3Size};
  for (auto size : cache_sizes) {
    bench->Arg(size / 2);
    bench->Arg(size);
    bench->Arg(size * 2);
  }

  bench->ArgName("size");
}

BENCHMARK_TEMPLATE(MemoryBandwidth, Read)->Apply(SetCacheBandwidthArgs);

static void SetMemoryBandwidthArgs(benchmark::internal::Benchmark* bench) {
  // `UseRealTime` is required due to threads, otherwise the cumulative CPU time
  // is used which will skew the results by the number of threads.
  bench->Arg(kMemoryPerCore)->ThreadRange(1, kNumCores)->UseRealTime();
}

BENCHMARK_TEMPLATE(MemoryBandwidth, StreamRead)->Apply(SetMemoryBandwidthArgs);
BENCHMARK_TEMPLATE(MemoryBandwidth, StreamWrite)->Apply(SetMemoryBandwidthArgs);
BENCHMARK_TEMPLATE(MemoryBandwidth, StreamReadWrite)->Apply(SetMemoryBandwidthArgs);
BENCHMARK_TEMPLATE(MemoryBandwidth, PlatformMemcpy)->Apply(SetMemoryBandwidthArgs);

static void ParallelMemoryCopy(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t n_threads = state.range(0);
  const int64_t buffer_size = kMemoryPerCore;

  std::shared_ptr<Buffer> src, dst;
  ABORT_NOT_OK(AllocateBuffer(buffer_size, &src));
  ABORT_NOT_OK(AllocateBuffer(buffer_size, &dst));

  random_bytes(buffer_size, 0, src->mutable_data());

  while (state.KeepRunning()) {
    io::FixedSizeBufferWriter writer(dst);
    writer.set_memcopy_threads(static_cast<int>(n_threads));
    ABORT_NOT_OK(writer.Write(src->data(), src->size()));
  }

  state.SetBytesProcessed(int64_t(state.iterations()) * buffer_size);
}

BENCHMARK(ParallelMemoryCopy)
    ->RangeMultiplier(2)
    ->Range(1, kNumCores)
    ->ArgName("threads")
    ->UseRealTime();

}  // namespace arrow
