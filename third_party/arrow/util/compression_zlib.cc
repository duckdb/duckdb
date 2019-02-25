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

#include "arrow/util/compression_zlib.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include <zconf.h>
#include <zlib.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {

constexpr int kGZipDefaultCompressionLevel = 9;

// ----------------------------------------------------------------------
// gzip implementation

// These are magic numbers from zlib.h.  Not clear why they are not defined
// there.

// Maximum window size
static constexpr int WINDOW_BITS = 15;

// Output Gzip.
static constexpr int GZIP_CODEC = 16;

// Determine if this is libz or gzip from header.
static constexpr int DETECT_CODEC = 32;

static int CompressionWindowBitsForFormat(GZipCodec::Format format) {
  int window_bits = WINDOW_BITS;
  switch (format) {
    case GZipCodec::DEFLATE:
      window_bits = -window_bits;
      break;
    case GZipCodec::GZIP:
      window_bits += GZIP_CODEC;
      break;
    case GZipCodec::ZLIB:
      break;
  }
  return window_bits;
}

static int DecompressionWindowBitsForFormat(GZipCodec::Format format) {
  if (format == GZipCodec::DEFLATE) {
    return -WINDOW_BITS;
  } else {
    /* If not deflate, autodetect format from header */
    return WINDOW_BITS | DETECT_CODEC;
  }
}

static Status ZlibErrorPrefix(const char* prefix_msg, const char* msg) {
  return Status::IOError(prefix_msg, (msg) ? msg : "(unknown error)");
}

// ----------------------------------------------------------------------
// gzip decompressor implementation

class GZipDecompressor : public Decompressor {
 public:
  GZipDecompressor() : initialized_(false), finished_(false) {}

  ~GZipDecompressor() override {
    if (initialized_) {
      inflateEnd(&stream_);
    }
  }

  Status Init(GZipCodec::Format format) {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));
    finished_ = false;

    int ret;
    int window_bits = DecompressionWindowBitsForFormat(format);
    if ((ret = inflateInit2(&stream_, window_bits)) != Z_OK) {
      return ZlibError("zlib inflateInit failed: ");
    } else {
      initialized_ = true;
      return Status::OK();
    }
  }

  Status Decompress(int64_t input_len, const uint8_t* input, int64_t output_len,
                    uint8_t* output, int64_t* bytes_read, int64_t* bytes_written,
                    bool* need_more_output) override {
    static constexpr auto input_limit =
        static_cast<int64_t>(std::numeric_limits<uInt>::max());
    stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
    stream_.avail_in = static_cast<uInt>(std::min(input_len, input_limit));
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));
    int ret;

    ret = inflate(&stream_, Z_SYNC_FLUSH);
    if (ret == Z_DATA_ERROR || ret == Z_STREAM_ERROR || ret == Z_MEM_ERROR) {
      return ZlibError("zlib inflate failed: ");
    }
    if (ret == Z_NEED_DICT) {
      return ZlibError("zlib inflate failed (need preset dictionary): ");
    }
    if (ret == Z_BUF_ERROR) {
      // No progress was possible
      *bytes_read = 0;
      *bytes_written = 0;
      *need_more_output = true;
    } else {
      DCHECK(ret == Z_OK || ret == Z_STREAM_END);
      // Some progress has been made
      *bytes_read = input_len - stream_.avail_in;
      *bytes_written = output_len - stream_.avail_out;
      *need_more_output = false;
    }
    finished_ = (ret == Z_STREAM_END);
    return Status::OK();
  }

  bool IsFinished() override { return finished_; }

 protected:
  Status ZlibError(const char* prefix_msg) {
    return ZlibErrorPrefix(prefix_msg, stream_.msg);
  }

  z_stream stream_;
  bool initialized_;
  bool finished_;
};

// ----------------------------------------------------------------------
// gzip compressor implementation

class GZipCompressor : public Compressor {
 public:
  GZipCompressor() : initialized_(false) {}

  ~GZipCompressor() override {
    if (initialized_) {
      deflateEnd(&stream_);
    }
  }

  Status Init(GZipCodec::Format format) {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));

    int ret;
    // Initialize to run specified format
    int window_bits = CompressionWindowBitsForFormat(format);
    if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window_bits,
                            kGZipDefaultCompressionLevel, Z_DEFAULT_STRATEGY)) != Z_OK) {
      return ZlibError("zlib deflateInit failed: ");
    } else {
      initialized_ = true;
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
  Status ZlibError(const char* prefix_msg) {
    return ZlibErrorPrefix(prefix_msg, stream_.msg);
  }

  z_stream stream_;
  bool initialized_;
};

Status GZipCompressor::Compress(int64_t input_len, const uint8_t* input,
                                int64_t output_len, uint8_t* output, int64_t* bytes_read,
                                int64_t* bytes_written) {
  DCHECK(initialized_) << "Called on non-initialized stream";

  static constexpr auto input_limit =
      static_cast<int64_t>(std::numeric_limits<uInt>::max());

  stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  stream_.avail_in = static_cast<uInt>(std::min(input_len, input_limit));
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));

  int64_t ret = 0;
  ret = deflate(&stream_, Z_NO_FLUSH);
  if (ret == Z_STREAM_ERROR) {
    return ZlibError("zlib compress failed: ");
  }
  if (ret == Z_OK) {
    // Some progress has been made
    *bytes_read = input_len - stream_.avail_in;
    *bytes_written = output_len - stream_.avail_out;
  } else {
    // No progress was possible
    DCHECK_EQ(ret, Z_BUF_ERROR);
    *bytes_read = 0;
    *bytes_written = 0;
  }
  return Status::OK();
}

Status GZipCompressor::Flush(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                             bool* should_retry) {
  DCHECK(initialized_) << "Called on non-initialized stream";

  static constexpr auto input_limit =
      static_cast<int64_t>(std::numeric_limits<uInt>::max());

  stream_.avail_in = 0;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));

  int64_t ret = 0;
  ret = deflate(&stream_, Z_SYNC_FLUSH);
  if (ret == Z_STREAM_ERROR) {
    return ZlibError("zlib flush failed: ");
  }
  if (ret == Z_OK) {
    *bytes_written = output_len - stream_.avail_out;
  } else {
    DCHECK_EQ(ret, Z_BUF_ERROR);
    *bytes_written = 0;
  }
  // "If deflate returns with avail_out == 0, this function must be called
  //  again with the same value of the flush parameter and more output space
  //  (updated avail_out), until the flush is complete (deflate returns
  //  with non-zero avail_out)."
  *should_retry = (*bytes_written == 0);
  return Status::OK();
}

Status GZipCompressor::End(int64_t output_len, uint8_t* output, int64_t* bytes_written,
                           bool* should_retry) {
  DCHECK(initialized_) << "Called on non-initialized stream";

  static constexpr auto input_limit =
      static_cast<int64_t>(std::numeric_limits<uInt>::max());

  stream_.avail_in = 0;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));

  int64_t ret = 0;
  ret = deflate(&stream_, Z_FINISH);
  if (ret == Z_STREAM_ERROR) {
    return ZlibError("zlib flush failed: ");
  }
  *bytes_written = output_len - stream_.avail_out;
  if (ret == Z_STREAM_END) {
    // Flush complete, we can now end the stream
    *should_retry = false;
    initialized_ = false;
    ret = deflateEnd(&stream_);
    if (ret == Z_OK) {
      return Status::OK();
    } else {
      return ZlibError("zlib end failed: ");
    }
  } else {
    // Not everything could be flushed,
    *should_retry = true;
    return Status::OK();
  }
}

// ----------------------------------------------------------------------
// gzip codec implementation

class GZipCodec::GZipCodecImpl {
 public:
  explicit GZipCodecImpl(GZipCodec::Format format)
      : format_(format),
        compressor_initialized_(false),
        decompressor_initialized_(false) {}

  ~GZipCodecImpl() {
    EndCompressor();
    EndDecompressor();
  }

  Status MakeCompressor(std::shared_ptr<Compressor>* out) {
    auto ptr = std::make_shared<GZipCompressor>();
    RETURN_NOT_OK(ptr->Init(format_));
    *out = ptr;
    return Status::OK();
  }

  Status MakeDecompressor(std::shared_ptr<Decompressor>* out) {
    auto ptr = std::make_shared<GZipDecompressor>();
    RETURN_NOT_OK(ptr->Init(format_));
    *out = ptr;
    return Status::OK();
  }

  Status InitCompressor() {
    EndDecompressor();
    memset(&stream_, 0, sizeof(stream_));

    int ret;
    // Initialize to run specified format
    int window_bits = CompressionWindowBitsForFormat(format_);
    if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window_bits,
                            kGZipDefaultCompressionLevel, Z_DEFAULT_STRATEGY)) != Z_OK) {
      return ZlibErrorPrefix("zlib deflateInit failed: ", stream_.msg);
    }
    compressor_initialized_ = true;
    return Status::OK();
  }

  void EndCompressor() {
    if (compressor_initialized_) {
      (void)deflateEnd(&stream_);
    }
    compressor_initialized_ = false;
  }

  Status InitDecompressor() {
    EndCompressor();
    memset(&stream_, 0, sizeof(stream_));
    int ret;

    // Initialize to run either deflate or zlib/gzip format
    int window_bits = DecompressionWindowBitsForFormat(format_);
    if ((ret = inflateInit2(&stream_, window_bits)) != Z_OK) {
      return ZlibErrorPrefix("zlib inflateInit failed: ", stream_.msg);
    }
    decompressor_initialized_ = true;
    return Status::OK();
  }

  void EndDecompressor() {
    if (decompressor_initialized_) {
      (void)inflateEnd(&stream_);
    }
    decompressor_initialized_ = false;
  }

  Status Decompress(int64_t input_length, const uint8_t* input,
                    int64_t output_buffer_length, uint8_t* output,
                    int64_t* output_length) {
    if (!decompressor_initialized_) {
      RETURN_NOT_OK(InitDecompressor());
    }
    if (output_buffer_length == 0) {
      // The zlib library does not allow *output to be NULL, even when
      // output_buffer_length is 0 (inflate() will return Z_STREAM_ERROR). We don't
      // consider this an error, so bail early if no output is expected. Note that we
      // don't signal an error if the input actually contains compressed data.
      if (output_length) {
        *output_length = 0;
      }
      return Status::OK();
    }

    // Reset the stream for this block
    if (inflateReset(&stream_) != Z_OK) {
      return ZlibErrorPrefix("zlib inflateReset failed: ", stream_.msg);
    }

    int ret = 0;
    // gzip can run in streaming mode or non-streaming mode.  We only
    // support the non-streaming use case where we present it the entire
    // compressed input and a buffer big enough to contain the entire
    // compressed output.  In the case where we don't know the output,
    // we just make a bigger buffer and try the non-streaming mode
    // from the beginning again.
    while (ret != Z_STREAM_END) {
      stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
      stream_.avail_in = static_cast<uInt>(input_length);
      stream_.next_out = reinterpret_cast<Bytef*>(output);
      stream_.avail_out = static_cast<uInt>(output_buffer_length);

      // We know the output size.  In this case, we can use Z_FINISH
      // which is more efficient.
      ret = inflate(&stream_, Z_FINISH);
      if (ret == Z_STREAM_END || ret != Z_OK) break;

      // Failure, buffer was too small
      return Status::IOError("Too small a buffer passed to GZipCodec. InputLength=",
                             input_length, " OutputLength=", output_buffer_length);
    }

    // Failure for some other reason
    if (ret != Z_STREAM_END) {
      return ZlibErrorPrefix("GZipCodec failed: ", stream_.msg);
    }

    if (output_length) {
      *output_length = stream_.total_out;
    }

    return Status::OK();
  }

  int64_t MaxCompressedLen(int64_t input_length, const uint8_t* ARROW_ARG_UNUSED(input)) {
    // Must be in compression mode
    if (!compressor_initialized_) {
      Status s = InitCompressor();
      DCHECK(s.ok());
    }
    int64_t max_len = deflateBound(&stream_, static_cast<uLong>(input_length));
    // ARROW-3514: return a more pessimistic estimate to account for bugs
    // in old zlib versions.
    return max_len + 12;
  }

  Status Compress(int64_t input_length, const uint8_t* input, int64_t output_buffer_len,
                  uint8_t* output, int64_t* output_len) {
    if (!compressor_initialized_) {
      RETURN_NOT_OK(InitCompressor());
    }
    stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
    stream_.avail_in = static_cast<uInt>(input_length);
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = static_cast<uInt>(output_buffer_len);

    int64_t ret = 0;
    if ((ret = deflate(&stream_, Z_FINISH)) != Z_STREAM_END) {
      if (ret == Z_OK) {
        // Will return Z_OK (and stream.msg NOT set) if stream.avail_out is too
        // small
        return Status::IOError("zlib deflate failed, output buffer too small");
      }

      return ZlibErrorPrefix("zlib deflate failed: ", stream_.msg);
    }

    if (deflateReset(&stream_) != Z_OK) {
      return ZlibErrorPrefix("zlib deflateReset failed: ", stream_.msg);
    }

    // Actual output length
    *output_len = output_buffer_len - stream_.avail_out;
    return Status::OK();
  }

 private:
  // zlib is stateful and the z_stream state variable must be initialized
  // before
  z_stream stream_;

  // Realistically, this will always be GZIP, but we leave the option open to
  // configure
  GZipCodec::Format format_;

  // These variables are mutually exclusive. When the codec is in "compressor"
  // state, compressor_initialized_ is true while decompressor_initialized_ is
  // false. When it's decompressing, the opposite is true.
  //
  // Indeed, this is slightly hacky, but the alternative is having separate
  // Compressor and Decompressor classes. If this ever becomes an issue, we can
  // perform the refactoring then
  bool compressor_initialized_;
  bool decompressor_initialized_;
};

GZipCodec::GZipCodec(Format format) { impl_.reset(new GZipCodecImpl(format)); }

GZipCodec::~GZipCodec() {}

Status GZipCodec::Decompress(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output) {
  return impl_->Decompress(input_length, input, output_buffer_len, output, nullptr);
}

Status GZipCodec::Decompress(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output,
                             int64_t* output_len) {
  return impl_->Decompress(input_length, input, output_buffer_len, output, output_len);
}

int64_t GZipCodec::MaxCompressedLen(int64_t input_length, const uint8_t* input) {
  return impl_->MaxCompressedLen(input_length, input);
}

Status GZipCodec::Compress(int64_t input_length, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output,
                           int64_t* output_len) {
  return impl_->Compress(input_length, input, output_buffer_len, output, output_len);
}

Status GZipCodec::MakeCompressor(std::shared_ptr<Compressor>* out) {
  return impl_->MakeCompressor(out);
}

Status GZipCodec::MakeDecompressor(std::shared_ptr<Decompressor>* out) {
  return impl_->MakeDecompressor(out);
}

const char* GZipCodec::name() const { return "gzip"; }

}  // namespace util
}  // namespace arrow
