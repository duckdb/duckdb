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

#include "arrow/io/compressed.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"

namespace arrow {

using util::Codec;
using util::Compressor;
using util::Decompressor;

namespace io {

// ----------------------------------------------------------------------
// CompressedOutputStream implementation

class CompressedOutputStream::Impl {
 public:
  Impl(MemoryPool* pool, Codec* codec, const std::shared_ptr<OutputStream>& raw)
      : pool_(pool), raw_(raw), codec_(codec), is_open_(true), compressed_pos_(0) {}

  ~Impl() { DCHECK(Close().ok()); }

  Status Init() {
    RETURN_NOT_OK(codec_->MakeCompressor(&compressor_));
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, kChunkSize, &compressed_));
    compressed_pos_ = 0;
    return Status::OK();
  }

  Status Tell(int64_t* position) const {
    return Status::NotImplemented("Cannot tell() a compressed stream");
  }

  std::shared_ptr<OutputStream> raw() const { return raw_; }

  Status FlushCompressed() {
    if (compressed_pos_ > 0) {
      RETURN_NOT_OK(raw_->Write(compressed_->data(), compressed_pos_));
      compressed_pos_ = 0;
    }
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);

    auto input = reinterpret_cast<const uint8_t*>(data);
    while (nbytes > 0) {
      int64_t bytes_read, bytes_written;
      int64_t input_len = nbytes;
      int64_t output_len = compressed_->size() - compressed_pos_;
      uint8_t* output = compressed_->mutable_data() + compressed_pos_;
      RETURN_NOT_OK(compressor_->Compress(input_len, input, output_len, output,
                                          &bytes_read, &bytes_written));
      compressed_pos_ += bytes_written;

      if (bytes_read == 0) {
        // Not enough output, try to flush it and retry
        if (compressed_pos_ > 0) {
          RETURN_NOT_OK(FlushCompressed());
          output_len = compressed_->size() - compressed_pos_;
          output = compressed_->mutable_data() + compressed_pos_;
          RETURN_NOT_OK(compressor_->Compress(input_len, input, output_len, output,
                                              &bytes_read, &bytes_written));
          compressed_pos_ += bytes_written;
        }
      }
      input += bytes_read;
      nbytes -= bytes_read;
      if (compressed_pos_ == compressed_->size()) {
        // Output buffer full, flush it
        RETURN_NOT_OK(FlushCompressed());
      }
      if (bytes_read == 0) {
        // Need to enlarge output buffer
        RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
      }
    }
    return Status::OK();
  }

  Status Flush() {
    std::lock_guard<std::mutex> guard(lock_);

    while (true) {
      // Flush compressor
      int64_t bytes_written;
      bool should_retry;
      int64_t output_len = compressed_->size() - compressed_pos_;
      uint8_t* output = compressed_->mutable_data() + compressed_pos_;
      RETURN_NOT_OK(
          compressor_->Flush(output_len, output, &bytes_written, &should_retry));
      compressed_pos_ += bytes_written;

      // Flush compressed output
      RETURN_NOT_OK(FlushCompressed());

      if (should_retry) {
        // Need to enlarge output buffer
        RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
      } else {
        break;
      }
    }
    return Status::OK();
  }

  Status FinalizeCompression() {
    while (true) {
      // Try to end compressor
      int64_t bytes_written;
      bool should_retry;
      int64_t output_len = compressed_->size() - compressed_pos_;
      uint8_t* output = compressed_->mutable_data() + compressed_pos_;
      RETURN_NOT_OK(compressor_->End(output_len, output, &bytes_written, &should_retry));
      compressed_pos_ += bytes_written;

      // Flush compressed output
      RETURN_NOT_OK(FlushCompressed());

      if (should_retry) {
        // Need to enlarge output buffer
        RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
      } else {
        // Done
        break;
      }
    }
    return Status::OK();
  }

  Status Close() {
    std::lock_guard<std::mutex> guard(lock_);

    if (is_open_) {
      is_open_ = false;
      RETURN_NOT_OK(FinalizeCompression());
      return raw_->Close();
    } else {
      return Status::OK();
    }
  }

  bool closed() {
    std::lock_guard<std::mutex> guard(lock_);
    return !is_open_;
  }

 private:
  // Write 64 KB compressed data at a time
  static const int64_t kChunkSize = 64 * 1024;

  MemoryPool* pool_;
  std::shared_ptr<OutputStream> raw_;
  Codec* codec_;
  bool is_open_;
  std::shared_ptr<Compressor> compressor_;
  std::shared_ptr<ResizableBuffer> compressed_;
  int64_t compressed_pos_;

  mutable std::mutex lock_;
};

Status CompressedOutputStream::Make(util::Codec* codec,
                                    const std::shared_ptr<OutputStream>& raw,
                                    std::shared_ptr<CompressedOutputStream>* out) {
  return Make(default_memory_pool(), codec, raw, out);
}

Status CompressedOutputStream::Make(MemoryPool* pool, util::Codec* codec,
                                    const std::shared_ptr<OutputStream>& raw,
                                    std::shared_ptr<CompressedOutputStream>* out) {
  std::shared_ptr<CompressedOutputStream> res(new CompressedOutputStream);
  res->impl_ = std::unique_ptr<Impl>(new Impl(pool, codec, std::move(raw)));
  RETURN_NOT_OK(res->impl_->Init());
  *out = res;
  return Status::OK();
}

CompressedOutputStream::~CompressedOutputStream() {}

Status CompressedOutputStream::Close() { return impl_->Close(); }

bool CompressedOutputStream::closed() const { return impl_->closed(); }

Status CompressedOutputStream::Tell(int64_t* position) const {
  return impl_->Tell(position);
}

Status CompressedOutputStream::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status CompressedOutputStream::Flush() { return impl_->Flush(); }

std::shared_ptr<OutputStream> CompressedOutputStream::raw() const { return impl_->raw(); }

// ----------------------------------------------------------------------
// CompressedInputStream implementation

class CompressedInputStream::Impl {
 public:
  Impl(MemoryPool* pool, Codec* codec, const std::shared_ptr<InputStream>& raw)
      : pool_(pool), raw_(raw), codec_(codec), is_open_(true) {}

  Status Init() {
    RETURN_NOT_OK(codec_->MakeDecompressor(&decompressor_));
    return Status::OK();
  }

  ~Impl() { DCHECK(Close().ok()); }

  Status Close() {
    std::lock_guard<std::mutex> guard(lock_);
    if (is_open_) {
      is_open_ = false;
      return raw_->Close();
    } else {
      return Status::OK();
    }
  }

  bool closed() {
    std::lock_guard<std::mutex> guard(lock_);
    return !is_open_;
  }

  Status Tell(int64_t* position) const {
    return Status::NotImplemented("Cannot tell() a compressed stream");
  }

  // Read compressed data if necessary
  Status EnsureCompressedData() {
    int64_t compressed_avail = compressed_ ? compressed_->size() - compressed_pos_ : 0;
    if (compressed_avail == 0) {
      // No compressed data available, read a full chunk
      RETURN_NOT_OK(raw_->Read(kChunkSize, &compressed_));
      compressed_pos_ = 0;
    }
    return Status::OK();
  }

  Status DecompressData() {
    int64_t decompress_size = kDecompressSize;

    while (true) {
      RETURN_NOT_OK(AllocateResizableBuffer(pool_, decompress_size, &decompressed_));
      decompressed_pos_ = 0;

      bool need_more_output;
      int64_t bytes_read, bytes_written;
      int64_t input_len = compressed_->size() - compressed_pos_;
      const uint8_t* input = compressed_->data() + compressed_pos_;
      int64_t output_len = decompressed_->size();
      uint8_t* output = decompressed_->mutable_data();

      RETURN_NOT_OK(decompressor_->Decompress(input_len, input, output_len, output,
                                              &bytes_read, &bytes_written,
                                              &need_more_output));
      compressed_pos_ += bytes_read;
      if (bytes_written > 0 || !need_more_output || input_len == 0) {
        RETURN_NOT_OK(decompressed_->Resize(bytes_written));
        break;
      }
      DCHECK_EQ(bytes_written, 0);
      // Need to enlarge output buffer
      decompress_size *= 2;
    }
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) {
    std::lock_guard<std::mutex> guard(lock_);

    *bytes_read = 0;
    auto out_data = reinterpret_cast<uint8_t*>(out);

    while (nbytes > 0) {
      int64_t avail = decompressed_ ? (decompressed_->size() - decompressed_pos_) : 0;
      if (avail > 0) {
        // Pending decompressed data is available, use it
        avail = std::min(avail, nbytes);
        memcpy(out_data, decompressed_->data() + decompressed_pos_, avail);
        decompressed_pos_ += avail;
        out_data += avail;
        *bytes_read += avail;
        nbytes -= avail;
        if (decompressed_pos_ == decompressed_->size()) {
          // Decompressed data is exhausted, release buffer
          decompressed_.reset();
        }
        if (nbytes == 0) {
          // We're done
          break;
        }
      }

      // At this point, no more decompressed data remains,
      // so we need to decompress more
      if (decompressor_->IsFinished()) {
        break;
      }
      // First try to read data from the decompressor
      if (compressed_) {
        RETURN_NOT_OK(DecompressData());
      }
      if (!decompressed_ || decompressed_->size() == 0) {
        // Got nothing, need to read more compressed data
        RETURN_NOT_OK(EnsureCompressedData());
        if (compressed_pos_ == compressed_->size()) {
          // Compressed stream unexpectedly exhausted
          return Status::IOError("Truncated compressed stream");
        }
        RETURN_NOT_OK(DecompressData());
      }
    }
    return Status::OK();
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
    std::shared_ptr<ResizableBuffer> buf;
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &buf));
    int64_t bytes_read;
    RETURN_NOT_OK(Read(nbytes, &bytes_read, buf->mutable_data()));
    RETURN_NOT_OK(buf->Resize(bytes_read));
    *out = buf;
    return Status::OK();
  }

  std::shared_ptr<InputStream> raw() const { return raw_; }

 private:
  // Read 64 KB compressed data at a time
  static const int64_t kChunkSize = 64 * 1024;
  // Decompress 1 MB at a time
  static const int64_t kDecompressSize = 1024 * 1024;

  MemoryPool* pool_;
  std::shared_ptr<InputStream> raw_;
  Codec* codec_;
  bool is_open_;
  std::shared_ptr<Decompressor> decompressor_;
  std::shared_ptr<Buffer> compressed_;
  int64_t compressed_pos_;
  std::shared_ptr<ResizableBuffer> decompressed_;
  int64_t decompressed_pos_;

  mutable std::mutex lock_;
};

Status CompressedInputStream::Make(Codec* codec, const std::shared_ptr<InputStream>& raw,
                                   std::shared_ptr<CompressedInputStream>* out) {
  return Make(default_memory_pool(), codec, raw, out);
}

Status CompressedInputStream::Make(MemoryPool* pool, Codec* codec,
                                   const std::shared_ptr<InputStream>& raw,
                                   std::shared_ptr<CompressedInputStream>* out) {
  std::shared_ptr<CompressedInputStream> res(new CompressedInputStream);
  res->impl_ = std::unique_ptr<Impl>(new Impl(pool, codec, std::move(raw)));
  RETURN_NOT_OK(res->impl_->Init());
  *out = res;
  return Status::OK();
}

CompressedInputStream::~CompressedInputStream() {}

Status CompressedInputStream::Close() { return impl_->Close(); }

bool CompressedInputStream::closed() const { return impl_->closed(); }

Status CompressedInputStream::Tell(int64_t* position) const {
  return impl_->Tell(position);
}

Status CompressedInputStream::Read(int64_t nbytes, int64_t* bytes_read, void* out) {
  return impl_->Read(nbytes, bytes_read, out);
}

Status CompressedInputStream::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  return impl_->Read(nbytes, out);
}

std::shared_ptr<InputStream> CompressedInputStream::raw() const { return impl_->raw(); }

}  // namespace io
}  // namespace arrow
