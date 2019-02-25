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

#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/compression.h"

using std::string;
using std::vector;

namespace arrow {
namespace util {

vector<uint8_t> MakeRandomData(int data_size) {
  vector<uint8_t> data(data_size);
  random_bytes(data_size, 1234, data.data());
  return data;
}

vector<uint8_t> MakeCompressibleData(int data_size) {
  std::string base_data =
      "Apache Arrow is a cross-language development platform for in-memory data";
  int nrepeats = static_cast<int>(1 + data_size / base_data.size());

  vector<uint8_t> data(base_data.size() * nrepeats);
  for (int i = 0; i < nrepeats; ++i) {
    std::memcpy(data.data() + i * base_data.size(), base_data.data(), base_data.size());
  }
  data.resize(data_size);
  return data;
}

// Check roundtrip of one-shot compression and decompression functions.

void CheckCodecRoundtrip(Compression::type ctype, const vector<uint8_t>& data) {
  // create multiple compressors to try to break them
  std::unique_ptr<Codec> c1, c2;

  ASSERT_OK(Codec::Create(ctype, &c1));
  ASSERT_OK(Codec::Create(ctype, &c2));

  int max_compressed_len =
      static_cast<int>(c1->MaxCompressedLen(data.size(), data.data()));
  std::vector<uint8_t> compressed(max_compressed_len);
  std::vector<uint8_t> decompressed(data.size());

  // compress with c1
  int64_t actual_size;
  ASSERT_OK(c1->Compress(data.size(), data.data(), max_compressed_len, compressed.data(),
                         &actual_size));
  compressed.resize(actual_size);

  // decompress with c2
  ASSERT_OK(c2->Decompress(compressed.size(), compressed.data(), decompressed.size(),
                           decompressed.data()));

  ASSERT_EQ(data, decompressed);

  // decompress with size with c2
  int64_t actual_decompressed_size;
  ASSERT_OK(c2->Decompress(compressed.size(), compressed.data(), decompressed.size(),
                           decompressed.data(), &actual_decompressed_size));

  ASSERT_EQ(data, decompressed);
  ASSERT_EQ(data.size(), actual_decompressed_size);

  // compress with c2
  int64_t actual_size2;
  ASSERT_OK(c2->Compress(data.size(), data.data(), max_compressed_len, compressed.data(),
                         &actual_size2));
  ASSERT_EQ(actual_size2, actual_size);

  // decompress with c1
  ASSERT_OK(c1->Decompress(compressed.size(), compressed.data(), decompressed.size(),
                           decompressed.data()));

  ASSERT_EQ(data, decompressed);

  // decompress with size with c1
  int64_t actual_decompressed_size2;
  ASSERT_OK(c1->Decompress(compressed.size(), compressed.data(), decompressed.size(),
                           decompressed.data(), &actual_decompressed_size2));

  ASSERT_EQ(data, decompressed);
  ASSERT_EQ(data.size(), actual_decompressed_size2);
}

// Check the streaming compressor against one-shot decompression

void CheckStreamingCompressor(Codec* codec, const vector<uint8_t>& data) {
  std::shared_ptr<Compressor> compressor;
  ASSERT_OK(codec->MakeCompressor(&compressor));

  std::vector<uint8_t> compressed;
  int64_t compressed_size = 0;
  const uint8_t* input = data.data();
  int64_t remaining = data.size();

  compressed.resize(10);
  bool do_flush = false;

  while (remaining > 0) {
    // Feed a small amount each time
    int64_t input_len = std::min(remaining, static_cast<int64_t>(1111));
    int64_t output_len = compressed.size() - compressed_size;
    uint8_t* output = compressed.data() + compressed_size;
    int64_t bytes_read, bytes_written;
    ASSERT_OK(compressor->Compress(input_len, input, output_len, output, &bytes_read,
                                   &bytes_written));
    ASSERT_LE(bytes_read, input_len);
    ASSERT_LE(bytes_written, output_len);
    compressed_size += bytes_written;
    input += bytes_read;
    remaining -= bytes_read;
    if (bytes_read == 0) {
      compressed.resize(compressed.capacity() * 2);
    }
    // Once every two iterations, do a flush
    if (do_flush) {
      bool should_retry = false;
      do {
        output_len = compressed.size() - compressed_size;
        output = compressed.data() + compressed_size;
        ASSERT_OK(compressor->Flush(output_len, output, &bytes_written, &should_retry));
        ASSERT_LE(bytes_written, output_len);
        compressed_size += bytes_written;
        if (should_retry) {
          compressed.resize(compressed.capacity() * 2);
        }
      } while (should_retry);
    }
    do_flush = !do_flush;
  }

  // End the compressed stream
  bool should_retry = false;
  do {
    int64_t output_len = compressed.size() - compressed_size;
    uint8_t* output = compressed.data() + compressed_size;
    int64_t bytes_written;
    ASSERT_OK(compressor->End(output_len, output, &bytes_written, &should_retry));
    ASSERT_LE(bytes_written, output_len);
    compressed_size += bytes_written;
    if (should_retry) {
      compressed.resize(compressed.capacity() * 2);
    }
  } while (should_retry);

  // Check decompressing the compressed data
  std::vector<uint8_t> decompressed(data.size());
  ASSERT_OK(codec->Decompress(compressed_size, compressed.data(), decompressed.size(),
                              decompressed.data()));

  ASSERT_EQ(data, decompressed);
}

// Check the streaming decompressor against one-shot compression

void CheckStreamingDecompressor(Codec* codec, const vector<uint8_t>& data) {
  // Create compressed data
  int64_t max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
  std::vector<uint8_t> compressed(max_compressed_len);
  int64_t compressed_size;
  ASSERT_OK(codec->Compress(data.size(), data.data(), max_compressed_len,
                            compressed.data(), &compressed_size));
  compressed.resize(compressed_size);

  // Run streaming decompression
  std::shared_ptr<Decompressor> decompressor;
  ASSERT_OK(codec->MakeDecompressor(&decompressor));

  std::vector<uint8_t> decompressed;
  int64_t decompressed_size = 0;
  const uint8_t* input = compressed.data();
  int64_t remaining = compressed.size();

  decompressed.resize(10);
  while (!decompressor->IsFinished()) {
    // Feed a small amount each time
    int64_t input_len = std::min(remaining, static_cast<int64_t>(23));
    int64_t output_len = decompressed.size() - decompressed_size;
    uint8_t* output = decompressed.data() + decompressed_size;
    int64_t bytes_read, bytes_written;
    bool need_more_output;
    ASSERT_OK(decompressor->Decompress(input_len, input, output_len, output, &bytes_read,
                                       &bytes_written, &need_more_output));
    ASSERT_LE(bytes_read, input_len);
    ASSERT_LE(bytes_written, output_len);
    ASSERT_TRUE(need_more_output || bytes_written > 0 || bytes_read > 0)
        << "Decompression not progressing anymore";
    if (need_more_output) {
      decompressed.resize(decompressed.capacity() * 2);
    }
    decompressed_size += bytes_written;
    input += bytes_read;
    remaining -= bytes_read;
  }
  ASSERT_TRUE(decompressor->IsFinished());
  ASSERT_EQ(remaining, 0);

  // Check the decompressed data
  decompressed.resize(decompressed_size);
  ASSERT_EQ(data.size(), decompressed_size);
  ASSERT_EQ(data, decompressed);
}

// Check the streaming compressor and decompressor together

void CheckStreamingRoundtrip(Codec* codec, const vector<uint8_t>& data) {
  std::shared_ptr<Compressor> compressor;
  std::shared_ptr<Decompressor> decompressor;
  ASSERT_OK(codec->MakeCompressor(&compressor));
  ASSERT_OK(codec->MakeDecompressor(&decompressor));

  std::default_random_engine engine(42);
  std::uniform_int_distribution<int> buf_size_distribution(10, 40);

  auto make_buf_size = [&]() -> int64_t { return buf_size_distribution(engine); };

  // Compress...

  std::vector<uint8_t> compressed(1);
  int64_t compressed_size = 0;
  {
    const uint8_t* input = data.data();
    int64_t remaining = data.size();

    while (remaining > 0) {
      // Feed a varying amount each time
      int64_t input_len = std::min(remaining, make_buf_size());
      int64_t output_len = compressed.size() - compressed_size;
      uint8_t* output = compressed.data() + compressed_size;
      int64_t bytes_read, bytes_written;
      ASSERT_OK(compressor->Compress(input_len, input, output_len, output, &bytes_read,
                                     &bytes_written));
      ASSERT_LE(bytes_read, input_len);
      ASSERT_LE(bytes_written, output_len);
      compressed_size += bytes_written;
      input += bytes_read;
      remaining -= bytes_read;
      if (bytes_read == 0) {
        compressed.resize(compressed.capacity() * 2);
      }
    }
    // End the compressed stream
    bool should_retry = false;
    do {
      int64_t output_len = compressed.size() - compressed_size;
      uint8_t* output = compressed.data() + compressed_size;
      int64_t bytes_written;
      ASSERT_OK(compressor->End(output_len, output, &bytes_written, &should_retry));
      ASSERT_LE(bytes_written, output_len);
      compressed_size += bytes_written;
      if (should_retry) {
        compressed.resize(compressed.capacity() * 2);
      }
    } while (should_retry);

    compressed.resize(compressed_size);
  }

  // Then decompress...

  std::vector<uint8_t> decompressed(2);
  int64_t decompressed_size = 0;
  {
    const uint8_t* input = compressed.data();
    int64_t remaining = compressed.size();

    while (!decompressor->IsFinished()) {
      // Feed a varying amount each time
      int64_t input_len = std::min(remaining, make_buf_size());
      int64_t output_len = decompressed.size() - decompressed_size;
      uint8_t* output = decompressed.data() + decompressed_size;
      int64_t bytes_read, bytes_written;
      bool need_more_output;
      ASSERT_OK(decompressor->Decompress(input_len, input, output_len, output,
                                         &bytes_read, &bytes_written, &need_more_output));
      ASSERT_LE(bytes_read, input_len);
      ASSERT_LE(bytes_written, output_len);
      ASSERT_TRUE(need_more_output || bytes_written > 0 || bytes_read > 0)
          << "Decompression not progressing anymore";
      if (need_more_output) {
        decompressed.resize(decompressed.capacity() * 2);
      }
      decompressed_size += bytes_written;
      input += bytes_read;
      remaining -= bytes_read;
    }
    ASSERT_EQ(remaining, 0);
    decompressed.resize(decompressed_size);
  }

  ASSERT_EQ(data.size(), decompressed.size());
  ASSERT_EQ(data, decompressed);
}

class CodecTest : public ::testing::TestWithParam<Compression::type> {
 protected:
  Compression::type GetCompression() { return GetParam(); }

  std::unique_ptr<Codec> MakeCodec() {
    std::unique_ptr<Codec> codec;
    ABORT_NOT_OK(Codec::Create(GetCompression(), &codec));
    return codec;
  }
};

TEST_P(CodecTest, CodecRoundtrip) {
  if (GetCompression() == Compression::BZ2) {
    // SKIP: BZ2 doesn't support one-shot compression
    return;
  }

  int sizes[] = {0, 10000, 100000};
  for (int data_size : sizes) {
    vector<uint8_t> data = MakeRandomData(data_size);
    CheckCodecRoundtrip(GetCompression(), data);

    data = MakeCompressibleData(data_size);
    CheckCodecRoundtrip(GetCompression(), data);
  }
}

TEST_P(CodecTest, OutputBufferIsSmall) {
  auto type = GetCompression();
  if (type != Compression::SNAPPY) {
    return;
  }

  std::unique_ptr<Codec> codec;
  ASSERT_OK(Codec::Create(type, &codec));

  vector<uint8_t> data = MakeRandomData(10);
  auto max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
  std::vector<uint8_t> compressed(max_compressed_len);
  std::vector<uint8_t> decompressed(data.size() - 1);

  int64_t actual_size;
  ASSERT_OK(codec->Compress(data.size(), data.data(), max_compressed_len,
                            compressed.data(), &actual_size));
  compressed.resize(actual_size);

  int64_t actual_decompressed_size;
  std::stringstream ss;
  ss << "Invalid: Output buffer size (" << decompressed.size() << ") must be "
     << data.size() << " or larger.";
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid, ss.str(),
      codec->Decompress(compressed.size(), compressed.data(), decompressed.size(),
                        decompressed.data(), &actual_decompressed_size));
}

TEST_P(CodecTest, StreamingCompressor) {
  if (GetCompression() == Compression::SNAPPY) {
    // SKIP: snappy doesn't support streaming compression
    return;
  }
  if (GetCompression() == Compression::BZ2) {
    // SKIP: BZ2 doesn't support one-shot decompression
    return;
  }
  if (GetCompression() == Compression::LZ4) {
    // SKIP: LZ4 streaming compression uses the LZ4 framing format,
    // which must be tested against a streaming decompressor
    return;
  }

  int sizes[] = {0, 10, 100000};
  for (int data_size : sizes) {
    auto codec = MakeCodec();

    vector<uint8_t> data = MakeRandomData(data_size);
    CheckStreamingCompressor(codec.get(), data);

    data = MakeCompressibleData(data_size);
    CheckStreamingCompressor(codec.get(), data);
  }
}

TEST_P(CodecTest, StreamingDecompressor) {
  if (GetCompression() == Compression::SNAPPY) {
    // SKIP: snappy doesn't support streaming decompression
    return;
  }
  if (GetCompression() == Compression::BZ2) {
    // SKIP: BZ2 doesn't support one-shot compression
    return;
  }
  if (GetCompression() == Compression::LZ4) {
    // SKIP: LZ4 streaming decompression uses the LZ4 framing format,
    // which must be tested against a streaming compressor
    return;
  }

  int sizes[] = {0, 10, 100000};
  for (int data_size : sizes) {
    auto codec = MakeCodec();

    vector<uint8_t> data = MakeRandomData(data_size);
    CheckStreamingDecompressor(codec.get(), data);

    data = MakeCompressibleData(data_size);
    CheckStreamingDecompressor(codec.get(), data);
  }
}

TEST_P(CodecTest, StreamingRoundtrip) {
  if (GetCompression() == Compression::SNAPPY) {
    // SKIP: snappy doesn't support streaming decompression
    return;
  }

  int sizes[] = {0, 10, 100000};
  for (int data_size : sizes) {
    auto codec = MakeCodec();

    vector<uint8_t> data = MakeRandomData(data_size);
    CheckStreamingRoundtrip(codec.get(), data);

    data = MakeCompressibleData(data_size);
    CheckStreamingRoundtrip(codec.get(), data);
  }
}

INSTANTIATE_TEST_CASE_P(TestGZip, CodecTest, ::testing::Values(Compression::GZIP));

INSTANTIATE_TEST_CASE_P(TestSnappy, CodecTest, ::testing::Values(Compression::SNAPPY));

INSTANTIATE_TEST_CASE_P(TestLZ4, CodecTest, ::testing::Values(Compression::LZ4));

INSTANTIATE_TEST_CASE_P(TestBrotli, CodecTest, ::testing::Values(Compression::BROTLI));

// bz2 requires a binary installation, there is no ExternalProject
#if ARROW_WITH_BZ2
INSTANTIATE_TEST_CASE_P(TestBZ2, CodecTest, ::testing::Values(Compression::BZ2));
#endif

// The ExternalProject for zstd does not build on CMake < 3.7, so we do not
// require it here
#ifdef ARROW_WITH_ZSTD
INSTANTIATE_TEST_CASE_P(TestZSTD, CodecTest, ::testing::Values(Compression::ZSTD));
#endif

}  // namespace util
}  // namespace arrow
