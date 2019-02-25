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

#include "arrow/util/compression_zstd.h"

#include <cstddef>
#include <cstdint>
#include <sstream>

#include <zstd.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {

// XXX level = 1 probably doesn't compress very much
constexpr int kZSTDDefaultCompressionLevel = 1;

static Status ZSTDError(size_t ret, const char* prefix_msg) {
  return Status::IOError(prefix_msg, ZSTD_getErrorName(ret));
}

// ----------------------------------------------------------------------
// ZSTD decompressor implementation

class ZSTDDecompressor : public Decompressor {
 public:
  ZSTDDecompressor() : stream_(ZSTD_createDStream()) {}

  ~ZSTDDecompressor() override { ZSTD_freeDStream(stream_); }

  Status Init() {
    finished_ = false;
    size_t ret = ZSTD_initDStream(stream_);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD init failed: ");
    } else {
      return Status::OK();
    }
  }

  Status Decompress(int64_t input_len, const uint8_t* input, int64_t output_len,
                    uint8_t* output, int64_t* bytes_read, int64_t* bytes_written,
                    bool* need_more_output) override {
    ZSTD_inBuffer in_buf;
    ZSTD_outBuffer out_buf;

    in_buf.src = input;
    in_buf.size = static_cast<size_t>(input_len);
    in_buf.pos = 0;
    out_buf.dst = output;
    out_buf.size = static_cast<size_t>(output_len);
    out_buf.pos = 0;

    size_t ret;
    ret = ZSTD_decompressStream(stream_, &out_buf, &in_buf);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD decompress failed: ");
    }
    *bytes_read = static_cast<int64_t>(in_buf.pos);
    *bytes_written = static_cast<int64_t>(out_buf.pos);
    *need_more_output = *bytes_read == 0 && *bytes_written == 0;
    finished_ = (ret == 0);
    return Status::OK();
  }

  bool IsFinished() override { return finished_; }

 protected:
  ZSTD_DStream* stream_;
  bool finished_;
};

// ----------------------------------------------------------------------
// ZSTD compressor implementation

class ZSTDCompressor : public Compressor {
 public:
  ZSTDCompressor() : stream_(ZSTD_createCStream()) {}

  ~ZSTDCompressor() override { ZSTD_freeCStream(stream_); }

  Status Init() {
    size_t ret = ZSTD_initCStream(stream_, kZSTDDefaultCompressionLevel);
    if (ZSTD_isError(ret)) {
      return ZSTDError(ret, "ZSTD init failed: ");
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
  ZSTD_CStream* stream_;
};

Status ZSTDCompressor::Compress(int64_t input_len, const uint8_t* input,
                                int64_t output_len, uint8_t* output, int64_t* bytes_read,
                                int64_t* bytes_written) {
  ZSTD_inBuffer in_buf;
  ZSTD_outBuffer out_buf;

  in_buf.src = input;
  in_buf.size = static_cast<size_t>(input_len);
  in_buf.pos = 0;
  out_buf.dst = output;
  out_buf.size = static_cast<size_t>(output_len);
  out_buf.pos = 0;

  size_t ret;
  ret = ZSTD_compressStream(stream_, &out_buf, &in_buf);
  if (ZSTD_isError(ret)) {
    return ZSTDError(ret, "ZSTD compress failed: ");
  }
  *bytes_read = static_cast<int64_t>(in_buf.pos);
  *bytes_written = static_cast<int64_t>(out_buf.pos);
  return Status::OK();
}

Status ZSTDCompressor::Flush(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                             bool* should_retry) {
  ZSTD_outBuffer out_buf;

  out_buf.dst = output;
  out_buf.size = static_cast<size_t>(output_len);
  out_buf.pos = 0;

  size_t ret;
  ret = ZSTD_flushStream(stream_, &out_buf);
  if (ZSTD_isError(ret)) {
    return ZSTDError(ret, "ZSTD flush failed: ");
  }
  *bytes_written = static_cast<int64_t>(out_buf.pos);
  *should_retry = ret > 0;
  return Status::OK();
}

Status ZSTDCompressor::End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                           bool* should_retry) {
  ZSTD_outBuffer out_buf;

  out_buf.dst = output;
  out_buf.size = static_cast<size_t>(output_len);
  out_buf.pos = 0;

  size_t ret;
  ret = ZSTD_endStream(stream_, &out_buf);
  if (ZSTD_isError(ret)) {
    return ZSTDError(ret, "ZSTD end failed: ");
  }
  *bytes_written = static_cast<int64_t>(out_buf.pos);
  *should_retry = ret > 0;
  return Status::OK();
}

// ----------------------------------------------------------------------
// ZSTD codec implementation

Status ZSTDCodec::MakeCompressor(std::shared_ptr<Compressor>* out) {
  auto ptr = std::make_shared<ZSTDCompressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status ZSTDCodec::MakeDecompressor(std::shared_ptr<Decompressor>* out) {
  auto ptr = std::make_shared<ZSTDDecompressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status ZSTDCodec::Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) {
  return Decompress(input_len, input, output_buffer_len, output_buffer, nullptr);
}

Status ZSTDCodec::Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer,
                             int64_t* output_len) {
  if (output_buffer == nullptr) {
    // We may pass a NULL 0-byte output buffer but some zstd versions demand
    // a valid pointer: https://github.com/facebook/zstd/issues/1385
    static uint8_t empty_buffer[1];
    DCHECK_EQ(output_buffer_len, 0);
    output_buffer = empty_buffer;
  }

  size_t ret = ZSTD_decompress(output_buffer, static_cast<size_t>(output_buffer_len),
                               input, static_cast<size_t>(input_len));
  if (ZSTD_isError(ret)) {
    return ZSTDError(ret, "ZSTD decompression failed: ");
  }
  if (static_cast<int64_t>(ret) != output_buffer_len) {
    return Status::IOError("Corrupt ZSTD compressed data.");
  }
  if (output_len) {
    *output_len = static_cast<int64_t>(ret);
  }
  return Status::OK();
}

int64_t ZSTDCodec::MaxCompressedLen(int64_t input_len,
                                    const uint8_t* ARROW_ARG_UNUSED(input)) {
  return ZSTD_compressBound(input_len);
}

Status ZSTDCodec::Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer,
                           int64_t* output_len) {
  size_t ret =
      ZSTD_compress(output_buffer, static_cast<size_t>(output_buffer_len), input,
                    static_cast<size_t>(input_len), kZSTDDefaultCompressionLevel);
  if (ZSTD_isError(ret)) {
    return ZSTDError(ret, "ZSTD compression failed: ");
  }
  *output_len = static_cast<int64_t>(ret);
  return Status::OK();
}

}  // namespace util
}  // namespace arrow
