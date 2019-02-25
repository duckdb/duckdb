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

#include "arrow/util/compression_lz4.h"

#include <cstdint>
#include <cstring>
#include <sstream>

#include <lz4.h>
#include <lz4frame.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {

static Status LZ4Error(LZ4F_errorCode_t ret, const char* prefix_msg) {
  return Status::IOError(prefix_msg, LZ4F_getErrorName(ret));
}

// ----------------------------------------------------------------------
// Lz4 decompressor implementation

class LZ4Decompressor : public Decompressor {
 public:
  LZ4Decompressor() {}

  ~LZ4Decompressor() override {
    if (ctx_ != nullptr) {
      ARROW_UNUSED(LZ4F_freeDecompressionContext(ctx_));
    }
  }

  Status Init() {
    LZ4F_errorCode_t ret;
    finished_ = false;

    ret = LZ4F_createDecompressionContext(&ctx_, LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 init failed: ");
    } else {
      return Status::OK();
    }
  }

  Status Decompress(int64_t input_len, const uint8_t* input, int64_t output_len,
                    uint8_t* output, int64_t* bytes_read, int64_t* bytes_written,
                    bool* need_more_output) override {
    auto src = input;
    auto dst = output;
    auto srcSize = static_cast<size_t>(input_len);
    auto dstCapacity = static_cast<size_t>(output_len);
    size_t ret;

    ret = LZ4F_decompress(ctx_, dst, &dstCapacity, src, &srcSize, nullptr /* options */);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 decompress failed: ");
    }
    *bytes_read = static_cast<int64_t>(srcSize);
    *bytes_written = static_cast<int64_t>(dstCapacity);
    *need_more_output = (*bytes_read == 0 && *bytes_written == 0);
    finished_ = (ret == 0);
    return Status::OK();
  }

  bool IsFinished() override { return finished_; }

 protected:
  LZ4F_dctx* ctx_ = nullptr;
  bool finished_;
};

// ----------------------------------------------------------------------
// Lz4 compressor implementation

class LZ4Compressor : public Compressor {
 public:
  LZ4Compressor() {}

  ~LZ4Compressor() override {
    if (ctx_ != nullptr) {
      ARROW_UNUSED(LZ4F_freeCompressionContext(ctx_));
    }
  }

  Status Init() {
    LZ4F_errorCode_t ret;
    memset(&prefs_, 0, sizeof(prefs_));
    first_time_ = true;

    ret = LZ4F_createCompressionContext(&ctx_, LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
      return LZ4Error(ret, "LZ4 init failed: ");
    } else {
      return Status::OK();
    }
  }

  Status Compress(int64_t input_len, const uint8_t* input, int64_t output_len,
                  uint8_t* output, int64_t* bytes_read, int64_t* bytes_written) override;

  Status Flush(int64_t output_len, uint8_t* output, int64_t* bytes_written,
               bool* should_retry) override;

  Status End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
             bool* should_retry) override;

 protected:
  LZ4F_cctx* ctx_ = nullptr;
  LZ4F_preferences_t prefs_;
  bool first_time_;
};

#define BEGIN_COMPRESS(dst, dstCapacity)                       \
  if (first_time_) {                                           \
    if (dstCapacity < LZ4F_HEADER_SIZE_MAX) {                  \
      /* Output too small to write LZ4F header */              \
      return Status::OK();                                     \
    }                                                          \
    ret = LZ4F_compressBegin(ctx_, dst, dstCapacity, &prefs_); \
    if (LZ4F_isError(ret)) {                                   \
      return LZ4Error(ret, "LZ4 compress begin failed: ");     \
    }                                                          \
    first_time_ = false;                                       \
    dst += ret;                                                \
    dstCapacity -= ret;                                        \
    *bytes_written += static_cast<int64_t>(ret);               \
  }

Status LZ4Compressor::Compress(int64_t input_len, const uint8_t* input,
                               int64_t output_len, uint8_t* output, int64_t* bytes_read,
                               int64_t* bytes_written) {
  auto src = input;
  auto dst = output;
  auto srcSize = static_cast<size_t>(input_len);
  auto dstCapacity = static_cast<size_t>(output_len);
  size_t ret;

  *bytes_read = 0;
  *bytes_written = 0;

  BEGIN_COMPRESS(dst, dstCapacity);

  if (dstCapacity < LZ4F_compressBound(srcSize, &prefs_)) {
    // Output too small to compress into
    return Status::OK();
  }
  ret = LZ4F_compressUpdate(ctx_, dst, dstCapacity, src, srcSize, nullptr /* options */);
  if (LZ4F_isError(ret)) {
    return LZ4Error(ret, "LZ4 compress update failed: ");
  }
  *bytes_read = input_len;
  *bytes_written += static_cast<int64_t>(ret);
  DCHECK_LE(*bytes_written, output_len);
  return Status::OK();
}

Status LZ4Compressor::Flush(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                            bool* should_retry) {
  auto dst = output;
  auto dstCapacity = static_cast<size_t>(output_len);
  size_t ret;

  *bytes_written = 0;
  *should_retry = true;

  BEGIN_COMPRESS(dst, dstCapacity);

  if (dstCapacity < LZ4F_compressBound(0, &prefs_)) {
    // Output too small to flush into
    return Status::OK();
  }

  ret = LZ4F_flush(ctx_, dst, dstCapacity, nullptr /* options */);
  if (LZ4F_isError(ret)) {
    return LZ4Error(ret, "LZ4 flush failed: ");
  }
  *bytes_written += static_cast<int64_t>(ret);
  *should_retry = false;
  DCHECK_LE(*bytes_written, output_len);
  return Status::OK();
}

Status LZ4Compressor::End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                          bool* should_retry) {
  auto dst = output;
  auto dstCapacity = static_cast<size_t>(output_len);
  size_t ret;

  *bytes_written = 0;
  *should_retry = true;

  BEGIN_COMPRESS(dst, dstCapacity);

  if (dstCapacity < LZ4F_compressBound(0, &prefs_)) {
    // Output too small to end frame into
    return Status::OK();
  }

  ret = LZ4F_compressEnd(ctx_, dst, dstCapacity, nullptr /* options */);
  if (LZ4F_isError(ret)) {
    return LZ4Error(ret, "LZ4 end failed: ");
  }
  *bytes_written += static_cast<int64_t>(ret);
  *should_retry = false;
  DCHECK_LE(*bytes_written, output_len);
  return Status::OK();
}

#undef BEGIN_COMPRESS

// ----------------------------------------------------------------------
// Lz4 codec implementation

Status Lz4Codec::MakeCompressor(std::shared_ptr<Compressor>* out) {
  auto ptr = std::make_shared<LZ4Compressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status Lz4Codec::MakeDecompressor(std::shared_ptr<Decompressor>* out) {
  auto ptr = std::make_shared<LZ4Decompressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status Lz4Codec::Decompress(int64_t input_len, const uint8_t* input,
                            int64_t output_buffer_len, uint8_t* output_buffer) {
  return Decompress(input_len, input, output_buffer_len, output_buffer, nullptr);
}

Status Lz4Codec::Decompress(int64_t input_len, const uint8_t* input,
                            int64_t output_buffer_len, uint8_t* output_buffer,
                            int64_t* output_len) {
  int64_t decompressed_size = LZ4_decompress_safe(
      reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
      static_cast<int>(input_len), static_cast<int>(output_buffer_len));
  if (decompressed_size < 0) {
    return Status::IOError("Corrupt Lz4 compressed data.");
  }
  if (output_len) {
    *output_len = decompressed_size;
  }
  return Status::OK();
}

int64_t Lz4Codec::MaxCompressedLen(int64_t input_len,
                                   const uint8_t* ARROW_ARG_UNUSED(input)) {
  return LZ4_compressBound(static_cast<int>(input_len));
}

Status Lz4Codec::Compress(int64_t input_len, const uint8_t* input,
                          int64_t output_buffer_len, uint8_t* output_buffer,
                          int64_t* output_len) {
  *output_len = LZ4_compress_default(
      reinterpret_cast<const char*>(input), reinterpret_cast<char*>(output_buffer),
      static_cast<int>(input_len), static_cast<int>(output_buffer_len));
  if (*output_len == 0) {
    return Status::IOError("Lz4 compression failure.");
  }
  return Status::OK();
}

}  // namespace util
}  // namespace arrow
