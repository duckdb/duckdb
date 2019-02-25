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

#include "arrow/util/compression_brotli.h"

#include <cstddef>
#include <cstdint>
#include <sstream>

#include <brotli/decode.h>
#include <brotli/encode.h>
#include <brotli/types.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {

// Brotli compression quality is max (11) by default, which is slow.
// We use 8 as a default as it is the best trade-off for Parquet workload.
constexpr int kBrotliDefaultCompressionLevel = 8;

// ----------------------------------------------------------------------
// Brotli decompressor implementation

class BrotliDecompressor : public Decompressor {
 public:
  BrotliDecompressor() {}

  ~BrotliDecompressor() override {
    if (state_ != nullptr) {
      BrotliDecoderDestroyInstance(state_);
    }
  }

  Status Init() {
    state_ = BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
    if (state_ == nullptr) {
      return BrotliError("Brotli init failed");
    }
    return Status::OK();
  }

  Status Decompress(int64_t input_len, const uint8_t* input, int64_t output_len,
                    uint8_t* output, int64_t* bytes_read, int64_t* bytes_written,
                    bool* need_more_output) override {
    auto avail_in = static_cast<size_t>(input_len);
    auto avail_out = static_cast<size_t>(output_len);
    BrotliDecoderResult ret;

    ret = BrotliDecoderDecompressStream(state_, &avail_in, &input, &avail_out, &output,
                                        nullptr /* total_out */);
    if (ret == BROTLI_DECODER_RESULT_ERROR) {
      return BrotliError(BrotliDecoderGetErrorCode(state_), "Brotli decompress failed: ");
    }
    *need_more_output = (ret == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT);
    *bytes_read = static_cast<int64_t>(input_len - avail_in);
    *bytes_written = static_cast<int64_t>(output_len - avail_out);
    return Status::OK();
  }

  bool IsFinished() override { return BrotliDecoderIsFinished(state_); }

 protected:
  Status BrotliError(const char* msg) { return Status::IOError(msg); }

  Status BrotliError(BrotliDecoderErrorCode code, const char* prefix_msg) {
    return Status::IOError(prefix_msg, BrotliDecoderErrorString(code));
  }

  BrotliDecoderState* state_ = nullptr;
};

// ----------------------------------------------------------------------
// Brotli compressor implementation

class BrotliCompressor : public Compressor {
 public:
  BrotliCompressor() {}

  ~BrotliCompressor() override {
    if (state_ != nullptr) {
      BrotliEncoderDestroyInstance(state_);
    }
  }

  Status Init() {
    state_ = BrotliEncoderCreateInstance(nullptr, nullptr, nullptr);
    if (state_ == nullptr) {
      return BrotliError("Brotli init failed");
    }
    if (!BrotliEncoderSetParameter(state_, BROTLI_PARAM_QUALITY,
                                   kBrotliDefaultCompressionLevel)) {
      return BrotliError("Brotli set compression level failed");
    }
    return Status::OK();
  }

  Status Compress(int64_t input_len, const uint8_t* input, int64_t output_len,
                  uint8_t* output, int64_t* bytes_read, int64_t* bytes_written) override;

  Status Flush(int64_t output_len, uint8_t* output, int64_t* bytes_written,
               bool* should_retry) override;

  Status End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
             bool* should_retry) override;

 protected:
  Status BrotliError(const char* msg) { return Status::IOError(msg); }

  BrotliEncoderState* state_ = nullptr;
};

Status BrotliCompressor::Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output,
                                  int64_t* bytes_read, int64_t* bytes_written) {
  auto avail_in = static_cast<size_t>(input_len);
  auto avail_out = static_cast<size_t>(output_len);
  BROTLI_BOOL ret;

  ret = BrotliEncoderCompressStream(state_, BROTLI_OPERATION_PROCESS, &avail_in, &input,
                                    &avail_out, &output, nullptr /* total_out */);
  if (!ret) {
    return BrotliError("Brotli compress failed");
  }
  *bytes_read = static_cast<int64_t>(input_len - avail_in);
  *bytes_written = static_cast<int64_t>(output_len - avail_out);
  return Status::OK();
}

Status BrotliCompressor::Flush(int64_t output_len, uint8_t* output,
                               int64_t* bytes_written, bool* should_retry) {
  size_t avail_in = 0;
  const uint8_t* next_in = nullptr;
  auto avail_out = static_cast<size_t>(output_len);
  BROTLI_BOOL ret;

  ret = BrotliEncoderCompressStream(state_, BROTLI_OPERATION_FLUSH, &avail_in, &next_in,
                                    &avail_out, &output, nullptr /* total_out */);
  if (!ret) {
    return BrotliError("Brotli flush failed");
  }
  *bytes_written = static_cast<int64_t>(output_len - avail_out);
  *should_retry = !!BrotliEncoderHasMoreOutput(state_);
  return Status::OK();
}

Status BrotliCompressor::End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                             bool* should_retry) {
  size_t avail_in = 0;
  const uint8_t* next_in = nullptr;
  auto avail_out = static_cast<size_t>(output_len);
  BROTLI_BOOL ret;

  ret = BrotliEncoderCompressStream(state_, BROTLI_OPERATION_FINISH, &avail_in, &next_in,
                                    &avail_out, &output, nullptr /* total_out */);
  if (!ret) {
    return BrotliError("Brotli end failed");
  }
  *bytes_written = static_cast<int64_t>(output_len - avail_out);
  *should_retry = !!BrotliEncoderHasMoreOutput(state_);
  DCHECK_EQ(*should_retry, !BrotliEncoderIsFinished(state_));
  return Status::OK();
}

// ----------------------------------------------------------------------
// Brotli codec implementation

Status BrotliCodec::MakeCompressor(std::shared_ptr<Compressor>* out) {
  auto ptr = std::make_shared<BrotliCompressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status BrotliCodec::MakeDecompressor(std::shared_ptr<Decompressor>* out) {
  auto ptr = std::make_shared<BrotliDecompressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status BrotliCodec::Decompress(int64_t input_len, const uint8_t* input,
                               int64_t output_buffer_len, uint8_t* output_buffer) {
  return Decompress(input_len, input, output_buffer_len, output_buffer, nullptr);
}

Status BrotliCodec::Decompress(int64_t input_len, const uint8_t* input,
                               int64_t output_buffer_len, uint8_t* output_buffer,
                               int64_t* output_len) {
  std::size_t output_size = output_buffer_len;
  if (BrotliDecoderDecompress(input_len, input, &output_size, output_buffer) !=
      BROTLI_DECODER_RESULT_SUCCESS) {
    return Status::IOError("Corrupt brotli compressed data.");
  }
  if (output_len) {
    *output_len = output_size;
  }
  return Status::OK();
}

int64_t BrotliCodec::MaxCompressedLen(int64_t input_len,
                                      const uint8_t* ARROW_ARG_UNUSED(input)) {
  return BrotliEncoderMaxCompressedSize(input_len);
}

Status BrotliCodec::Compress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer,
                             int64_t* output_len) {
  std::size_t output_size = output_buffer_len;
  if (BrotliEncoderCompress(kBrotliDefaultCompressionLevel, BROTLI_DEFAULT_WINDOW,
                            BROTLI_DEFAULT_MODE, input_len, input, &output_size,
                            output_buffer) == BROTLI_FALSE) {
    return Status::IOError("Brotli compression failure.");
  }
  *output_len = output_size;
  return Status::OK();
}

}  // namespace util
}  // namespace arrow
