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
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/compression.h"

namespace arrow {
namespace util {

std::vector<uint8_t> MakeCompressibleData(int data_size) {
  // XXX This isn't a real-world corpus so doesn't really represent the
  // comparative qualities of the algorithms

  // First make highly compressible data
  std::string base_data =
      "Apache Arrow is a cross-language development platform for in-memory data";
  int nrepeats = static_cast<int>(1 + data_size / base_data.size());

  std::vector<uint8_t> data(base_data.size() * nrepeats);
  for (int i = 0; i < nrepeats; ++i) {
    std::memcpy(data.data() + i * base_data.size(), base_data.data(), base_data.size());
  }
  data.resize(data_size);

  // Then randomly mutate some bytes so as to make things harder
  std::mt19937 engine(42);
  std::exponential_distribution<> offsets(0.05);
  std::uniform_int_distribution<> values(0, 255);

  int64_t pos = 0;
  while (pos < data_size) {
    data[pos] = static_cast<uint8_t>(values(engine));
    pos += static_cast<int64_t>(offsets(engine));
  }

  return data;
}

int64_t StreamingCompress(Codec* codec, const std::vector<uint8_t>& data,
                          std::vector<uint8_t>* compressed_data = nullptr) {
  if (compressed_data != nullptr) {
    compressed_data->clear();
    compressed_data->shrink_to_fit();
  }
  std::shared_ptr<Compressor> compressor;
  ABORT_NOT_OK(codec->MakeCompressor(&compressor));

  const uint8_t* input = data.data();
  int64_t input_len = data.size();
  int64_t compressed_size = 0;

  std::vector<uint8_t> output_buffer(1 << 20);  // 1 MB

  while (input_len > 0) {
    int64_t bytes_read = 0, bytes_written = 0;
    ABORT_NOT_OK(compressor->Compress(input_len, input, output_buffer.size(),
                                      output_buffer.data(), &bytes_read, &bytes_written));
    input += bytes_read;
    input_len -= bytes_read;
    compressed_size += bytes_written;
    if (compressed_data != nullptr && bytes_written > 0) {
      compressed_data->resize(compressed_data->size() + bytes_written);
      memcpy(compressed_data->data() + compressed_data->size() - bytes_written,
             output_buffer.data(), bytes_written);
    }
    if (bytes_read == 0) {
      // Need to enlarge output buffer
      output_buffer.resize(output_buffer.size() * 2);
    }
  }
  while (true) {
    bool should_retry;
    int64_t bytes_written;
    ABORT_NOT_OK(compressor->End(output_buffer.size(), output_buffer.data(),
                                 &bytes_written, &should_retry));
    compressed_size += bytes_written;
    if (compressed_data != nullptr && bytes_written > 0) {
      compressed_data->resize(compressed_data->size() + bytes_written);
      memcpy(compressed_data->data() + compressed_data->size() - bytes_written,
             output_buffer.data(), bytes_written);
    }
    if (should_retry) {
      // Need to enlarge output buffer
      output_buffer.resize(output_buffer.size() * 2);
    } else {
      break;
    }
  }
  return compressed_size;
}

static void BM_StreamingCompression(
    Compression::type compression, const std::vector<uint8_t>& data,
    benchmark::State& state) {  // NOLINT non-const reference
  std::unique_ptr<Codec> codec;
  ABORT_NOT_OK(Codec::Create(compression, &codec));

  while (state.KeepRunning()) {
    int64_t compressed_size = StreamingCompress(codec.get(), data);
    state.counters["ratio"] =
        static_cast<double>(data.size()) / static_cast<double>(compressed_size);
  }
  state.SetBytesProcessed(state.iterations() * data.size());
}

template <Compression::type COMPRESSION>
static void BM_StreamingCompression(
    benchmark::State& state) {                        // NOLINT non-const reference
  auto data = MakeCompressibleData(8 * 1024 * 1024);  // 8 MB

  BM_StreamingCompression(COMPRESSION, data, state);
}

static void BM_StreamingDecompression(
    Compression::type compression, const std::vector<uint8_t>& data,
    benchmark::State& state) {  // NOLINT non-const reference
  std::unique_ptr<Codec> codec;
  ABORT_NOT_OK(Codec::Create(compression, &codec));

  std::vector<uint8_t> compressed_data;
  ARROW_UNUSED(StreamingCompress(codec.get(), data, &compressed_data));
  state.counters["ratio"] =
      static_cast<double>(data.size()) / static_cast<double>(compressed_data.size());

  while (state.KeepRunning()) {
    std::shared_ptr<Decompressor> decompressor;
    ABORT_NOT_OK(codec->MakeDecompressor(&decompressor));

    const uint8_t* input = compressed_data.data();
    int64_t input_len = compressed_data.size();
    int64_t decompressed_size = 0;

    std::vector<uint8_t> output_buffer(1 << 20);  // 1 MB
    while (!decompressor->IsFinished()) {
      int64_t bytes_read, bytes_written;
      bool need_more_output;
      ABORT_NOT_OK(decompressor->Decompress(input_len, input, output_buffer.size(),
                                            output_buffer.data(), &bytes_read,
                                            &bytes_written, &need_more_output));
      input += bytes_read;
      input_len -= bytes_read;
      decompressed_size += bytes_written;
      if (need_more_output) {
        // Enlarge output buffer
        output_buffer.resize(output_buffer.size() * 2);
      }
    }
    ARROW_CHECK(decompressed_size == static_cast<int64_t>(data.size()));
  }
  state.SetBytesProcessed(state.iterations() * data.size());
}

template <Compression::type COMPRESSION>
static void BM_StreamingDecompression(
    benchmark::State& state) {                        // NOLINT non-const reference
  auto data = MakeCompressibleData(8 * 1024 * 1024);  // 8 MB

  BM_StreamingDecompression(COMPRESSION, data, state);
}

BENCHMARK_TEMPLATE(BM_StreamingCompression, Compression::GZIP)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);
BENCHMARK_TEMPLATE(BM_StreamingCompression, Compression::BROTLI)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);
BENCHMARK_TEMPLATE(BM_StreamingCompression, Compression::ZSTD)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);
BENCHMARK_TEMPLATE(BM_StreamingCompression, Compression::LZ4)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);

BENCHMARK_TEMPLATE(BM_StreamingDecompression, Compression::GZIP)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);
BENCHMARK_TEMPLATE(BM_StreamingDecompression, Compression::BROTLI)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);
BENCHMARK_TEMPLATE(BM_StreamingDecompression, Compression::ZSTD)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);
BENCHMARK_TEMPLATE(BM_StreamingDecompression, Compression::LZ4)
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(1);

}  // namespace util
}  // namespace arrow
