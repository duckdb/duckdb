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

#include "arrow/util/compression_bz2.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>

// Avoid defining max() macro
#include "arrow/util/windows_compatibility.h"

#include <bzlib.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {

constexpr int kBZ2DefaultCompressionLevel = 9;

// Max number of bytes the bz2 APIs accept at a time
static constexpr auto kSizeLimit =
    static_cast<int64_t>(std::numeric_limits<unsigned int>::max());

Status BZ2Error(const char* prefix_msg, int bz_result) {
  DCHECK(bz_result != BZ_OK && bz_result != BZ_RUN_OK && bz_result != BZ_FLUSH_OK &&
         bz_result != BZ_FINISH_OK && bz_result != BZ_STREAM_END);
  StatusCode code;
  std::stringstream ss;
  ss << prefix_msg;
  switch (bz_result) {
    case BZ_CONFIG_ERROR:
      code = StatusCode::UnknownError;
      ss << "bz2 library improperly configured (internal error)";
      break;
    case BZ_SEQUENCE_ERROR:
      code = StatusCode::UnknownError;
      ss << "wrong sequence of calls to bz2 library (internal error)";
      break;
    case BZ_PARAM_ERROR:
      code = StatusCode::UnknownError;
      ss << "wrong parameter to bz2 library (internal error)";
      break;
    case BZ_MEM_ERROR:
      code = StatusCode::OutOfMemory;
      ss << "could not allocate memory for bz2 library";
      break;
    case BZ_DATA_ERROR:
      code = StatusCode::IOError;
      ss << "invalid bz2 data";
      break;
    case BZ_DATA_ERROR_MAGIC:
      code = StatusCode::IOError;
      ss << "data is not bz2-compressed (no magic header)";
      break;
    default:
      code = StatusCode::UnknownError;
      ss << "unknown bz2 error " << bz_result;
      break;
  }
  return Status(code, ss.str());
}

// ----------------------------------------------------------------------
// bz2 decompressor implementation

class BZ2Decompressor : public Decompressor {
 public:
  BZ2Decompressor() : initialized_(false) {}

  ~BZ2Decompressor() override {
    if (initialized_) {
      ARROW_UNUSED(BZ2_bzDecompressEnd(&stream_));
    }
  }

  Status Init() {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));
    int ret;
    ret = BZ2_bzDecompressInit(&stream_, 0, 0);
    if (ret != BZ_OK) {
      return BZ2Error("bz2 decompressor init failed: ", ret);
    }
    initialized_ = true;
    finished_ = false;
    return Status::OK();
  }

  Status Decompress(int64_t input_len, const uint8_t* input, int64_t output_len,
                    uint8_t* output, int64_t* bytes_read, int64_t* bytes_written,
                    bool* need_more_output) override {
    stream_.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    stream_.avail_in = static_cast<unsigned int>(std::min(input_len, kSizeLimit));
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzDecompress(&stream_);
    if (ret == BZ_OK || ret == BZ_STREAM_END) {
      *bytes_read = input_len - stream_.avail_in;
      *bytes_written = output_len - stream_.avail_out;
      finished_ = (ret == BZ_STREAM_END);
      *need_more_output = (!finished_ && *bytes_read == 0 && *bytes_written == 0);
      return Status::OK();
    } else {
      return BZ2Error("bz2 decompress failed: ", ret);
    }
  }

  bool IsFinished() override { return finished_; }

 protected:
  bz_stream stream_;
  bool initialized_;
  bool finished_;
};

// ----------------------------------------------------------------------
// bz2 compressor implementation

class BZ2Compressor : public Compressor {
 public:
  BZ2Compressor() : initialized_(false) {}

  ~BZ2Compressor() override {
    if (initialized_) {
      ARROW_UNUSED(BZ2_bzCompressEnd(&stream_));
    }
  }

  Status Init() {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));
    int ret;
    ret = BZ2_bzCompressInit(&stream_, kBZ2DefaultCompressionLevel, 0, 0);
    if (ret != BZ_OK) {
      return BZ2Error("bz2 compressor init failed: ", ret);
    }
    initialized_ = true;
    return Status::OK();
  }

  Status Compress(int64_t input_len, const uint8_t* input, int64_t output_len,
                  uint8_t* output, int64_t* bytes_read, int64_t* bytes_written) override {
    stream_.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    stream_.avail_in = static_cast<unsigned int>(std::min(input_len, kSizeLimit));
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzCompress(&stream_, BZ_RUN);
    if (ret == BZ_RUN_OK) {
      *bytes_read = input_len - stream_.avail_in;
      *bytes_written = output_len - stream_.avail_out;
      return Status::OK();
    } else {
      return BZ2Error("bz2 compress failed: ", ret);
    }
  }

  Status Flush(int64_t output_len, uint8_t* output, int64_t* bytes_written,
               bool* should_retry) override {
    stream_.next_in = nullptr;
    stream_.avail_in = 0;
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzCompress(&stream_, BZ_FLUSH);
    if (ret == BZ_RUN_OK || ret == BZ_FLUSH_OK) {
      *bytes_written = output_len - stream_.avail_out;
      *should_retry = (ret == BZ_FLUSH_OK);
      return Status::OK();
    } else {
      return BZ2Error("bz2 compress failed: ", ret);
    }
  }

  Status End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
             bool* should_retry) override {
    stream_.next_in = nullptr;
    stream_.avail_in = 0;
    stream_.next_out = reinterpret_cast<char*>(output);
    stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
    int ret;

    ret = BZ2_bzCompress(&stream_, BZ_FINISH);
    if (ret == BZ_STREAM_END || ret == BZ_FINISH_OK) {
      *bytes_written = output_len - stream_.avail_out;
      *should_retry = (ret == BZ_FINISH_OK);
      return Status::OK();
    } else {
      return BZ2Error("bz2 compress failed: ", ret);
    }
  }

 protected:
  bz_stream stream_;
  bool initialized_;
};

// ----------------------------------------------------------------------
// bz2 codec implementation

Status BZ2Codec::MakeCompressor(std::shared_ptr<Compressor>* out) {
  auto ptr = std::make_shared<BZ2Compressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status BZ2Codec::MakeDecompressor(std::shared_ptr<Decompressor>* out) {
  auto ptr = std::make_shared<BZ2Decompressor>();
  RETURN_NOT_OK(ptr->Init());
  *out = ptr;
  return Status::OK();
}

Status BZ2Codec::Decompress(int64_t input_len, const uint8_t* input,
                            int64_t output_buffer_len, uint8_t* output_buffer) {
  return Status::NotImplemented("One-shot bz2 decompression not supported");
}

Status BZ2Codec::Decompress(int64_t input_len, const uint8_t* input,
                            int64_t output_buffer_len, uint8_t* output_buffer,
                            int64_t* output_len) {
  return Status::NotImplemented("One-shot bz2 decompression not supported");
}

int64_t BZ2Codec::MaxCompressedLen(int64_t input_len,
                                   const uint8_t* ARROW_ARG_UNUSED(input)) {
  // Cannot determine upper bound for bz2-compressed data
  return 0;
}

Status BZ2Codec::Compress(int64_t input_len, const uint8_t* input,
                          int64_t output_buffer_len, uint8_t* output_buffer,
                          int64_t* output_len) {
  return Status::NotImplemented("One-shot bz2 compression not supported");
}

}  // namespace util
}  // namespace arrow
