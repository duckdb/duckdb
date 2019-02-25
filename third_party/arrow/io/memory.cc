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

#include "arrow/io/memory.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <mutex>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/memory.h"

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// OutputStream that writes to resizable buffer

static constexpr int64_t kBufferMinimumSize = 256;

BufferOutputStream::BufferOutputStream()
    : is_open_(false), capacity_(0), position_(0), mutable_data_(nullptr) {}

BufferOutputStream::BufferOutputStream(const std::shared_ptr<ResizableBuffer>& buffer)
    : buffer_(buffer),
      is_open_(true),
      capacity_(buffer->size()),
      position_(0),
      mutable_data_(buffer->mutable_data()) {}

Status BufferOutputStream::Create(int64_t initial_capacity, MemoryPool* pool,
                                  std::shared_ptr<BufferOutputStream>* out) {
  // ctor is private, so cannot use make_shared
  *out = std::shared_ptr<BufferOutputStream>(new BufferOutputStream);
  return (*out)->Reset(initial_capacity, pool);
}

Status BufferOutputStream::Reset(int64_t initial_capacity, MemoryPool* pool) {
  RETURN_NOT_OK(AllocateResizableBuffer(pool, initial_capacity, &buffer_));
  is_open_ = true;
  capacity_ = initial_capacity;
  position_ = 0;
  mutable_data_ = buffer_->mutable_data();
  return Status::OK();
}

BufferOutputStream::~BufferOutputStream() {
  // This can fail, better to explicitly call close
  if (buffer_) {
    DCHECK(Close().ok());
  }
}

Status BufferOutputStream::Close() {
  if (is_open_) {
    is_open_ = false;
    if (position_ < capacity_) {
      RETURN_NOT_OK(buffer_->Resize(position_, false));
    }
  }
  return Status::OK();
}

bool BufferOutputStream::closed() const { return !is_open_; }

Status BufferOutputStream::Finish(std::shared_ptr<Buffer>* result) {
  RETURN_NOT_OK(Close());
  buffer_->ZeroPadding();
  *result = buffer_;
  buffer_ = nullptr;
  is_open_ = false;
  return Status::OK();
}

Status BufferOutputStream::Tell(int64_t* position) const {
  *position = position_;
  return Status::OK();
}

Status BufferOutputStream::Write(const void* data, int64_t nbytes) {
  if (ARROW_PREDICT_FALSE(!is_open_)) {
    return Status::IOError("OutputStream is closed");
  }
  DCHECK(buffer_);
  RETURN_NOT_OK(Reserve(nbytes));
  memcpy(mutable_data_ + position_, data, nbytes);
  position_ += nbytes;
  return Status::OK();
}

Status BufferOutputStream::Reserve(int64_t nbytes) {
  int64_t new_capacity = capacity_;
  while (position_ + nbytes > new_capacity) {
    new_capacity = std::max(kBufferMinimumSize, new_capacity * 2);
  }
  if (new_capacity > capacity_) {
    RETURN_NOT_OK(buffer_->Resize(new_capacity));
    capacity_ = new_capacity;
  }
  mutable_data_ = buffer_->mutable_data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// OutputStream that doesn't write anything

Status MockOutputStream::Close() {
  is_open_ = false;
  return Status::OK();
}

bool MockOutputStream::closed() const { return !is_open_; }

Status MockOutputStream::Tell(int64_t* position) const {
  *position = extent_bytes_written_;
  return Status::OK();
}

Status MockOutputStream::Write(const void* data, int64_t nbytes) {
  extent_bytes_written_ += nbytes;
  return Status::OK();
}

// ----------------------------------------------------------------------
// In-memory buffer writer

static constexpr int kMemcopyDefaultNumThreads = 1;
static constexpr int64_t kMemcopyDefaultBlocksize = 64;
static constexpr int64_t kMemcopyDefaultThreshold = 1024 * 1024;

class FixedSizeBufferWriter::FixedSizeBufferWriterImpl {
 public:
  /// Input buffer must be mutable, will abort if not

  /// Input buffer must be mutable, will abort if not
  explicit FixedSizeBufferWriterImpl(const std::shared_ptr<Buffer>& buffer)
      : is_open_(true),
        memcopy_num_threads_(kMemcopyDefaultNumThreads),
        memcopy_blocksize_(kMemcopyDefaultBlocksize),
        memcopy_threshold_(kMemcopyDefaultThreshold) {
    buffer_ = buffer;
    DCHECK(buffer->is_mutable()) << "Must pass mutable buffer";
    mutable_data_ = buffer->mutable_data();
    size_ = buffer->size();
    position_ = 0;
  }

  Status Close() {
    is_open_ = false;
    return Status::OK();
  }

  bool closed() const { return !is_open_; }

  Status Seek(int64_t position) {
    if (position < 0 || position > size_) {
      return Status::IOError("Seek out of bounds");
    }
    position_ = position;
    return Status::OK();
  }

  Status Tell(int64_t* position) {
    *position = position_;
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) {
    if (position_ + nbytes > size_) {
      return Status::IOError("Write out of bounds");
    }
    if (nbytes > memcopy_threshold_ && memcopy_num_threads_ > 1) {
      internal::parallel_memcopy(mutable_data_ + position_,
                                 reinterpret_cast<const uint8_t*>(data), nbytes,
                                 memcopy_blocksize_, memcopy_num_threads_);
    } else {
      memcpy(mutable_data_ + position_, data, nbytes);
    }
    position_ += nbytes;
    return Status::OK();
  }

  Status WriteAt(int64_t position, const void* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);
    RETURN_NOT_OK(Seek(position));
    return Write(data, nbytes);
  }

  void set_memcopy_threads(int num_threads) { memcopy_num_threads_ = num_threads; }

  void set_memcopy_blocksize(int64_t blocksize) { memcopy_blocksize_ = blocksize; }

  void set_memcopy_threshold(int64_t threshold) { memcopy_threshold_ = threshold; }

 private:
  std::mutex lock_;
  std::shared_ptr<Buffer> buffer_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t position_;
  bool is_open_;

  int memcopy_num_threads_;
  int64_t memcopy_blocksize_;
  int64_t memcopy_threshold_;
};

FixedSizeBufferWriter::FixedSizeBufferWriter(const std::shared_ptr<Buffer>& buffer)
    : impl_(new FixedSizeBufferWriterImpl(buffer)) {}

FixedSizeBufferWriter::~FixedSizeBufferWriter() = default;

Status FixedSizeBufferWriter::Close() { return impl_->Close(); }

bool FixedSizeBufferWriter::closed() const { return impl_->closed(); }

Status FixedSizeBufferWriter::Seek(int64_t position) { return impl_->Seek(position); }

Status FixedSizeBufferWriter::Tell(int64_t* position) const {
  return impl_->Tell(position);
}

Status FixedSizeBufferWriter::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status FixedSizeBufferWriter::WriteAt(int64_t position, const void* data,
                                      int64_t nbytes) {
  return impl_->WriteAt(position, data, nbytes);
}

void FixedSizeBufferWriter::set_memcopy_threads(int num_threads) {
  impl_->set_memcopy_threads(num_threads);
}

void FixedSizeBufferWriter::set_memcopy_blocksize(int64_t blocksize) {
  impl_->set_memcopy_blocksize(blocksize);
}

void FixedSizeBufferWriter::set_memcopy_threshold(int64_t threshold) {
  impl_->set_memcopy_threshold(threshold);
}

// ----------------------------------------------------------------------
// In-memory buffer reader

BufferReader::BufferReader(const std::shared_ptr<Buffer>& buffer)
    : buffer_(buffer),
      data_(buffer->data()),
      size_(buffer->size()),
      position_(0),
      is_open_(true) {}

BufferReader::BufferReader(const uint8_t* data, int64_t size)
    : buffer_(nullptr), data_(data), size_(size), position_(0), is_open_(true) {}

BufferReader::BufferReader(const Buffer& buffer)
    : BufferReader(buffer.data(), buffer.size()) {}

Status BufferReader::Close() {
  is_open_ = false;
  return Status::OK();
}

bool BufferReader::closed() const { return !is_open_; }

Status BufferReader::Tell(int64_t* position) const {
  *position = position_;
  return Status::OK();
}

util::string_view BufferReader::Peek(int64_t nbytes) const {
  const int64_t bytes_available = std::min(nbytes, size_ - position_);
  return util::string_view(reinterpret_cast<const char*>(data_) + position_,
                           static_cast<size_t>(bytes_available));
}

bool BufferReader::supports_zero_copy() const { return true; }

Status BufferReader::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                            void* buffer) {
  if (nbytes < 0) {
    return Status::IOError("Cannot read a negative number of bytes from BufferReader.");
  }
  *bytes_read = std::min(nbytes, size_ - position);
  if (*bytes_read) {
    memcpy(buffer, data_ + position, *bytes_read);
  }
  return Status::OK();
}

Status BufferReader::ReadAt(int64_t position, int64_t nbytes,
                            std::shared_ptr<Buffer>* out) {
  if (nbytes < 0) {
    return Status::IOError("Cannot read a negative number of bytes from BufferReader.");
  }
  int64_t size = std::min(nbytes, size_ - position);

  if (size > 0 && buffer_ != nullptr) {
    *out = SliceBuffer(buffer_, position, size);
  } else {
    *out = std::make_shared<Buffer>(data_ + position, size);
  }
  return Status::OK();
}

Status BufferReader::Read(int64_t nbytes, int64_t* bytes_read, void* buffer) {
  RETURN_NOT_OK(ReadAt(position_, nbytes, bytes_read, buffer));
  position_ += *bytes_read;
  return Status::OK();
}

Status BufferReader::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  RETURN_NOT_OK(ReadAt(position_, nbytes, out));
  position_ += (*out)->size();
  return Status::OK();
}

Status BufferReader::GetSize(int64_t* size) {
  *size = size_;
  return Status::OK();
}

Status BufferReader::Seek(int64_t position) {
  if (position < 0 || position > size_) {
    return Status::IOError("Seek out of bounds");
  }

  position_ = position;
  return Status::OK();
}

}  // namespace io
}  // namespace arrow
