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

#include "arrow/io/buffered.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// BufferedOutputStream implementation

class BufferedBase {
 public:
  explicit BufferedBase(MemoryPool* pool)
      : pool_(pool),
        is_open_(true),
        buffer_data_(nullptr),
        buffer_pos_(0),
        buffer_size_(0),
        raw_pos_(-1) {}

  bool closed() const {
    std::lock_guard<std::mutex> guard(lock_);
    return !is_open_;
  }

  Status ResetBuffer() {
    if (!buffer_) {
      // On first invocation, or if the buffer has been released, we allocate a
      // new buffer
      RETURN_NOT_OK(AllocateResizableBuffer(pool_, buffer_size_, &buffer_));
    } else if (buffer_->size() != buffer_size_) {
      RETURN_NOT_OK(buffer_->Resize(buffer_size_));
    }
    buffer_data_ = buffer_->mutable_data();
    buffer_pos_ = 0;
    return Status::OK();
  }

  Status ResizeBuffer(int64_t new_buffer_size) {
    buffer_size_ = new_buffer_size;
    return ResetBuffer();
  }

  void AppendToBuffer(const void* data, int64_t nbytes) {
    DCHECK_LE(buffer_pos_ + nbytes, buffer_size_);
    std::memcpy(buffer_data_ + buffer_pos_, data, nbytes);
    buffer_pos_ += nbytes;
  }

  int64_t buffer_size() const { return buffer_size_; }

 protected:
  MemoryPool* pool_;
  bool is_open_;

  std::shared_ptr<ResizableBuffer> buffer_;
  uint8_t* buffer_data_;
  int64_t buffer_pos_;
  int64_t buffer_size_;

  mutable int64_t raw_pos_;
  mutable std::mutex lock_;
};

class BufferedOutputStream::Impl : public BufferedBase {
 public:
  explicit Impl(std::shared_ptr<OutputStream> raw, MemoryPool* pool)
      : BufferedBase(pool), raw_(std::move(raw)) {}

  Status Close() {
    std::lock_guard<std::mutex> guard(lock_);
    if (is_open_) {
      Status st = FlushUnlocked();
      is_open_ = false;
      RETURN_NOT_OK(raw_->Close());
      return st;
    }
    return Status::OK();
  }

  Status Tell(int64_t* position) const {
    std::lock_guard<std::mutex> guard(lock_);
    if (raw_pos_ == -1) {
      RETURN_NOT_OK(raw_->Tell(&raw_pos_));
      DCHECK_GE(raw_pos_, 0);
    }
    *position = raw_pos_ + buffer_pos_;
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);
    if (nbytes < 0) {
      return Status::Invalid("write count should be >= 0");
    }
    if (nbytes == 0) {
      return Status::OK();
    }
    if (nbytes + buffer_pos_ >= buffer_size_) {
      RETURN_NOT_OK(FlushUnlocked());
      DCHECK_EQ(buffer_pos_, 0);
      if (nbytes >= buffer_size_) {
        // Direct write
        return raw_->Write(data, nbytes);
      }
    }
    AppendToBuffer(data, nbytes);
    return Status::OK();
  }

  Status FlushUnlocked() {
    if (buffer_pos_ > 0) {
      // Invalidate cached raw pos
      raw_pos_ = -1;
      RETURN_NOT_OK(raw_->Write(buffer_data_, buffer_pos_));
      buffer_pos_ = 0;
    }
    return Status::OK();
  }

  Status Flush() {
    std::lock_guard<std::mutex> guard(lock_);
    return FlushUnlocked();
  }

  Status Detach(std::shared_ptr<OutputStream>* raw) {
    RETURN_NOT_OK(Flush());
    *raw = std::move(raw_);
    is_open_ = false;
    return Status::OK();
  }

  Status SetBufferSize(int64_t new_buffer_size) {
    std::lock_guard<std::mutex> guard(lock_);
    DCHECK_GT(new_buffer_size, 0);
    if (buffer_pos_ >= new_buffer_size) {
      // If the buffer is shrinking, first flush to the raw OutputStream
      RETURN_NOT_OK(FlushUnlocked());
    }
    return ResizeBuffer(new_buffer_size);
  }

  std::shared_ptr<OutputStream> raw() const { return raw_; }

 private:
  std::shared_ptr<OutputStream> raw_;
};

BufferedOutputStream::BufferedOutputStream(std::shared_ptr<OutputStream> raw,
                                           MemoryPool* pool) {
  impl_.reset(new Impl(std::move(raw), pool));
}

Status BufferedOutputStream::Create(int64_t buffer_size, MemoryPool* pool,
                                    std::shared_ptr<OutputStream> raw,
                                    std::shared_ptr<BufferedOutputStream>* out) {
  auto result = std::shared_ptr<BufferedOutputStream>(
      new BufferedOutputStream(std::move(raw), pool));
  RETURN_NOT_OK(result->SetBufferSize(buffer_size));
  *out = std::move(result);
  return Status::OK();
}

BufferedOutputStream::~BufferedOutputStream() { DCHECK_OK(impl_->Close()); }

Status BufferedOutputStream::SetBufferSize(int64_t new_buffer_size) {
  return impl_->SetBufferSize(new_buffer_size);
}

int64_t BufferedOutputStream::buffer_size() const { return impl_->buffer_size(); }

Status BufferedOutputStream::Detach(std::shared_ptr<OutputStream>* raw) {
  return impl_->Detach(raw);
}

Status BufferedOutputStream::Close() { return impl_->Close(); }

bool BufferedOutputStream::closed() const { return impl_->closed(); }

Status BufferedOutputStream::Tell(int64_t* position) const {
  return impl_->Tell(position);
}

Status BufferedOutputStream::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status BufferedOutputStream::Flush() { return impl_->Flush(); }

std::shared_ptr<OutputStream> BufferedOutputStream::raw() const { return impl_->raw(); }

// ----------------------------------------------------------------------
// BufferedInputStream implementation

class BufferedInputStream::Impl : public BufferedBase {
 public:
  Impl(std::shared_ptr<InputStream> raw, MemoryPool* pool)
      : BufferedBase(pool), raw_(std::move(raw)), bytes_buffered_(0) {}

  ~Impl() { DCHECK_OK(Close()); }

  Status Close() {
    std::lock_guard<std::mutex> guard(lock_);
    if (is_open_) {
      is_open_ = false;
      return raw_->Close();
    }
    return Status::OK();
  }

  Status Tell(int64_t* position) const {
    std::lock_guard<std::mutex> guard(lock_);
    if (raw_pos_ == -1) {
      RETURN_NOT_OK(raw_->Tell(&raw_pos_));
      DCHECK_GE(raw_pos_, 0);
    }
    // Shift by bytes_buffered to return semantic stream position
    *position = raw_pos_ - bytes_buffered_;
    return Status::OK();
  }

  Status SetBufferSize(int64_t new_buffer_size) {
    std::lock_guard<std::mutex> guard(lock_);
    DCHECK_GT(new_buffer_size, 0);
    if ((buffer_pos_ + bytes_buffered_) >= new_buffer_size) {
      return Status::Invalid("Cannot shrink read buffer if buffered data remains");
    }
    return ResizeBuffer(new_buffer_size);
  }

  util::string_view Peek(int64_t nbytes) const {
    int64_t peek_size = std::min(nbytes, bytes_buffered_);
    return util::string_view(reinterpret_cast<const char*>(buffer_data_ + buffer_pos_),
                             static_cast<size_t>(peek_size));
  }

  int64_t bytes_buffered() const { return bytes_buffered_; }

  int64_t buffer_size() const { return buffer_size_; }

  std::shared_ptr<InputStream> Detach() {
    std::lock_guard<std::mutex> guard(lock_);
    is_open_ = false;
    return std::move(raw_);
  }

  void RewindBuffer() {
    // Invalidate buffered data, as with a Seek or large Read
    buffer_pos_ = bytes_buffered_ = 0;
  }

  Status BufferIfNeeded() {
    if (bytes_buffered_ == 0) {
      // Fill buffer
      if (!buffer_) {
        RETURN_NOT_OK(ResetBuffer());
      }
      RETURN_NOT_OK(raw_->Read(buffer_size_, &bytes_buffered_, buffer_data_));
      buffer_pos_ = 0;

      // Do not make assumptions about the raw stream position
      raw_pos_ = -1;
    }
    return Status::OK();
  }

  void ConsumeBuffer(int64_t nbytes) {
    buffer_pos_ += nbytes;
    bytes_buffered_ -= nbytes;
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) {
    std::lock_guard<std::mutex> guard(lock_);
    DCHECK_GT(nbytes, 0);

    if (nbytes < buffer_size_) {
      // Pre-buffer for small reads
      RETURN_NOT_OK(BufferIfNeeded());
    }

    if (nbytes > bytes_buffered_) {
      // Copy buffered bytes into out, then read rest
      memcpy(out, buffer_data_ + buffer_pos_, bytes_buffered_);
      RETURN_NOT_OK(raw_->Read(nbytes - bytes_buffered_, bytes_read,
                               reinterpret_cast<uint8_t*>(out) + bytes_buffered_));
      // Do not make assumptions about the raw stream position
      raw_pos_ = -1;
      *bytes_read += bytes_buffered_;
      RewindBuffer();
    } else {
      memcpy(out, buffer_data_ + buffer_pos_, nbytes);
      *bytes_read = nbytes;
      ConsumeBuffer(nbytes);
    }
    return Status::OK();
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
    std::shared_ptr<ResizableBuffer> buffer;
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, nbytes, &buffer));

    int64_t bytes_read = 0;
    RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));

    if (bytes_read < nbytes) {
      // Change size but do not reallocate internal capacity
      RETURN_NOT_OK(buffer->Resize(bytes_read, false /* shrink_to_fit */));
      buffer->ZeroPadding();
    }

    *out = buffer;
    return Status::OK();
  }

  // For providing access to the raw file handles
  std::shared_ptr<InputStream> raw() const { return raw_; }

 private:
  std::shared_ptr<InputStream> raw_;

  // Number of remaining bytes in the buffer, to be reduced on each read from
  // the buffer
  int64_t bytes_buffered_;
};

BufferedInputStream::BufferedInputStream(std::shared_ptr<InputStream> raw,
                                         MemoryPool* pool) {
  impl_.reset(new Impl(std::move(raw), pool));
}

BufferedInputStream::~BufferedInputStream() { DCHECK_OK(impl_->Close()); }

Status BufferedInputStream::Create(int64_t buffer_size, MemoryPool* pool,
                                   std::shared_ptr<InputStream> raw,
                                   std::shared_ptr<BufferedInputStream>* out) {
  auto result =
      std::shared_ptr<BufferedInputStream>(new BufferedInputStream(std::move(raw), pool));
  RETURN_NOT_OK(result->SetBufferSize(buffer_size));
  *out = std::move(result);
  return Status::OK();
}

Status BufferedInputStream::Close() { return impl_->Close(); }

bool BufferedInputStream::closed() const { return impl_->closed(); }

std::shared_ptr<InputStream> BufferedInputStream::Detach() { return impl_->Detach(); }

std::shared_ptr<InputStream> BufferedInputStream::raw() const { return impl_->raw(); }

Status BufferedInputStream::Tell(int64_t* position) const {
  return impl_->Tell(position);
}

util::string_view BufferedInputStream::Peek(int64_t nbytes) const {
  return impl_->Peek(nbytes);
}

Status BufferedInputStream::SetBufferSize(int64_t new_buffer_size) {
  return impl_->SetBufferSize(new_buffer_size);
}

int64_t BufferedInputStream::bytes_buffered() const { return impl_->bytes_buffered(); }

int64_t BufferedInputStream::buffer_size() const { return impl_->buffer_size(); }

Status BufferedInputStream::Read(int64_t nbytes, int64_t* bytes_read, void* out) {
  return impl_->Read(nbytes, bytes_read, out);
}

Status BufferedInputStream::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  return impl_->Read(nbytes, out);
}

}  // namespace io
}  // namespace arrow
