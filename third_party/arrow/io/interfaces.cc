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

#include "arrow/io/interfaces.h"

#include <cstdint>
#include <memory>
#include <mutex>

#include "arrow/status.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace io {

FileInterface::~FileInterface() = default;

Status InputStream::Advance(int64_t nbytes) {
  std::shared_ptr<Buffer> temp;
  return Read(nbytes, &temp);
}

util::string_view InputStream::Peek(int64_t ARROW_ARG_UNUSED(nbytes)) const {
  return util::string_view(nullptr, 0);
}

bool InputStream::supports_zero_copy() const { return false; }

struct RandomAccessFile::RandomAccessFileImpl {
  std::mutex lock_;
};

RandomAccessFile::~RandomAccessFile() = default;

RandomAccessFile::RandomAccessFile()
    : interface_impl_(new RandomAccessFile::RandomAccessFileImpl()) {}

Status RandomAccessFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                                void* out) {
  std::lock_guard<std::mutex> lock(interface_impl_->lock_);
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, bytes_read, out);
}

Status RandomAccessFile::ReadAt(int64_t position, int64_t nbytes,
                                std::shared_ptr<Buffer>* out) {
  std::lock_guard<std::mutex> lock(interface_impl_->lock_);
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, out);
}

Status Writable::Write(const std::string& data) {
  return Write(data.c_str(), static_cast<int64_t>(data.size()));
}

Status Writable::Flush() { return Status::OK(); }

}  // namespace io
}  // namespace arrow
