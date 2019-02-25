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

#include "arrow/io/readahead.h"

#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace io {
namespace internal {

// ----------------------------------------------------------------------
// ReadaheadSpooler implementation

class ReadaheadSpooler::Impl {
 public:
  Impl(MemoryPool* pool, std::shared_ptr<InputStream> raw, int64_t read_size,
       int32_t readahead_queue_size, int64_t left_padding, int64_t right_padding)
      : pool_(pool),
        raw_(raw),
        read_size_(read_size),
        readahead_queue_size_(readahead_queue_size),
        left_padding_(left_padding),
        right_padding_(right_padding) {
    DCHECK_NE(raw, nullptr);
    DCHECK_GT(read_size, 0);
    DCHECK_GT(readahead_queue_size, 0);
    io_worker_ = std::thread([&]() { WorkerLoop(); });
  }

  ~Impl() { ARROW_UNUSED(Close()); }

  Status Close() {
    std::unique_lock<std::mutex> lock(mutex_);
    please_close_ = true;
    io_wakeup_.notify_one();
    // Wait for IO thread to finish
    if (io_worker_.joinable()) {
      lock.unlock();
      io_worker_.join();
      lock.lock();
    }
    return raw_->Close();
  }

  Status Read(ReadaheadBuffer* out) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
      // Drain queue before querying other flags
      if (buffer_queue_.size() > 0) {
        *out = std::move(buffer_queue_.front());
        DCHECK_NE(out->buffer, nullptr);
        buffer_queue_.pop_front();
        // Need to fill up queue again
        io_wakeup_.notify_one();
        return Status::OK();
      }
      if (!read_status_.ok()) {
        // Got a read error, bail out
        return read_status_;
      }
      if (eof_) {
        out->buffer.reset();
        return Status::OK();
      }
      // Readahead queue is empty and we're not closed yet, wait for more I/O
      io_progress_.wait(lock);
    }
  }

  int64_t left_padding() {
    std::unique_lock<std::mutex> lock(mutex_);
    return left_padding_;
  }

  void left_padding(int64_t size) {
    std::unique_lock<std::mutex> lock(mutex_);
    left_padding_ = size;
  }

  int64_t right_padding() {
    std::unique_lock<std::mutex> lock(mutex_);
    return right_padding_;
  }

  void right_padding(int64_t size) {
    std::unique_lock<std::mutex> lock(mutex_);
    right_padding_ = size;
  }

 protected:
  // The background thread's main function
  void WorkerLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    Status st;

    while (true) {
      if (please_close_) {
        goto eof;
      }
      // Fill up readahead queue until desired size
      while (buffer_queue_.size() < static_cast<size_t>(readahead_queue_size_)) {
        ReadaheadBuffer buf = {nullptr, left_padding_, right_padding_};
        lock.unlock();
        Status st = ReadOneBufferUnlocked(&buf);
        lock.lock();
        if (!st.ok()) {
          read_status_ = st;
          goto error;
        }
        // Close() could have been called while unlocked above
        if (please_close_) {
          goto eof;
        }
        // Got empty read?
        if (buf.buffer->size() == buf.left_padding + buf.right_padding) {
          goto eof;
        }
        buffer_queue_.push_back(std::move(buf));
        io_progress_.notify_one();
      }
      // Wait for Close() or Read() call
      io_wakeup_.wait(lock);
    }
  eof:
    eof_ = true;
  error:
    // Make sure any pending Read() doesn't block indefinitely
    io_progress_.notify_one();
  }

  Status ReadOneBufferUnlocked(ReadaheadBuffer* buf) {
    // Note that left_padding_ and right_padding_ may be modified while unlocked
    std::shared_ptr<ResizableBuffer> buffer;
    int64_t bytes_read;
    RETURN_NOT_OK(AllocateResizableBuffer(
        pool_, read_size_ + buf->left_padding + buf->right_padding, &buffer));
    DCHECK_NE(buffer->mutable_data(), nullptr);
    RETURN_NOT_OK(
        raw_->Read(read_size_, &bytes_read, buffer->mutable_data() + buf->left_padding));
    if (bytes_read < read_size_) {
      // Got a short read
      RETURN_NOT_OK(buffer->Resize(bytes_read + buf->left_padding + buf->right_padding));
      DCHECK_NE(buffer->mutable_data(), nullptr);
    }
    // Zero padding areas
    memset(buffer->mutable_data(), 0, buf->left_padding);
    memset(buffer->mutable_data() + bytes_read + buf->left_padding, 0,
           buf->right_padding);
    buf->buffer = std::move(buffer);
    return Status::OK();
  }

  MemoryPool* pool_;
  std::shared_ptr<InputStream> raw_;
  int64_t read_size_;
  int32_t readahead_queue_size_;
  int64_t left_padding_ = 0;
  int64_t right_padding_ = 0;

  std::mutex mutex_;
  std::condition_variable io_wakeup_;
  std::condition_variable io_progress_;
  std::thread io_worker_;
  bool please_close_ = false;
  bool eof_ = false;
  std::deque<ReadaheadBuffer> buffer_queue_;
  Status read_status_;
};

ReadaheadSpooler::ReadaheadSpooler(MemoryPool* pool, std::shared_ptr<InputStream> raw,
                                   int64_t read_size, int32_t readahead_queue_size,
                                   int64_t left_padding, int64_t right_padding)
    : impl_(new ReadaheadSpooler::Impl(pool, raw, read_size, readahead_queue_size,
                                       left_padding, right_padding)) {}

ReadaheadSpooler::ReadaheadSpooler(std::shared_ptr<InputStream> raw, int64_t read_size,
                                   int32_t readahead_queue_size, int64_t left_padding,
                                   int64_t right_padding)
    : ReadaheadSpooler(default_memory_pool(), raw, read_size, readahead_queue_size,
                       left_padding, right_padding) {}

int64_t ReadaheadSpooler::GetLeftPadding() { return impl_->left_padding(); }

void ReadaheadSpooler::SetLeftPadding(int64_t size) { impl_->left_padding(size); }

int64_t ReadaheadSpooler::GetRightPadding() { return impl_->right_padding(); }

void ReadaheadSpooler::SetRightPadding(int64_t size) { impl_->right_padding(size); }

Status ReadaheadSpooler::Close() { return impl_->Close(); }

Status ReadaheadSpooler::Read(ReadaheadBuffer* out) { return impl_->Read(out); }

ReadaheadSpooler::~ReadaheadSpooler() {}

}  // namespace internal
}  // namespace io
}  // namespace arrow
