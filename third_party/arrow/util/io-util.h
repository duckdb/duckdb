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

#ifndef ARROW_UTIL_IO_UTIL_H
#define ARROW_UTIL_IO_UTIL_H

#include <iostream>
#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"

#if defined(_MSC_VER)
#include <boost/filesystem.hpp>  // NOLINT
#endif

namespace arrow {
namespace io {

// Output stream that just writes to stdout.
class ARROW_EXPORT StdoutStream : public OutputStream {
 public:
  StdoutStream() : pos_(0) { set_mode(FileMode::WRITE); }
  ~StdoutStream() override {}

  Status Close() override { return Status::OK(); }
  bool closed() const override { return false; }

  Status Tell(int64_t* position) const override {
    *position = pos_;
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) override {
    pos_ += nbytes;
    std::cout.write(reinterpret_cast<const char*>(data), nbytes);
    return Status::OK();
  }

 private:
  int64_t pos_;
};

// Output stream that just writes to stderr.
class ARROW_EXPORT StderrStream : public OutputStream {
 public:
  StderrStream() : pos_(0) { set_mode(FileMode::WRITE); }
  ~StderrStream() override {}

  Status Close() override { return Status::OK(); }
  bool closed() const override { return false; }

  Status Tell(int64_t* position) const override {
    *position = pos_;
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) override {
    pos_ += nbytes;
    std::cerr.write(reinterpret_cast<const char*>(data), nbytes);
    return Status::OK();
  }

 private:
  int64_t pos_;
};

// Input stream that just reads from stdin.
class ARROW_EXPORT StdinStream : public InputStream {
 public:
  StdinStream() : pos_(0) { set_mode(FileMode::READ); }
  ~StdinStream() override {}

  Status Close() override { return Status::OK(); }
  bool closed() const override { return false; }

  Status Tell(int64_t* position) const override {
    *position = pos_;
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override {
    std::cin.read(reinterpret_cast<char*>(out), nbytes);
    if (std::cin) {
      *bytes_read = nbytes;
      pos_ += nbytes;
    } else {
      *bytes_read = 0;
    }
    return Status::OK();
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override {
    std::shared_ptr<ResizableBuffer> buffer;
    ARROW_RETURN_NOT_OK(AllocateResizableBuffer(nbytes, &buffer));
    int64_t bytes_read;
    ARROW_RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    *out = buffer;
    return Status::OK();
  }

 private:
  int64_t pos_;
};

}  // namespace io

namespace internal {

#if defined(_MSC_VER)
// namespace fs = boost::filesystem;
// #define PlatformFilename fs::path
typedef ::boost::filesystem::path PlatformFilename;

#else

struct PlatformFilename {
  PlatformFilename() {}
  explicit PlatformFilename(const std::string& path) { utf8_path = path; }

  const char* c_str() const { return utf8_path.c_str(); }

  const std::string& string() const { return utf8_path; }

  size_t length() const { return utf8_path.size(); }

  std::string utf8_path;
};
#endif

ARROW_EXPORT
Status FileNameFromString(const std::string& file_name, PlatformFilename* out);

ARROW_EXPORT
Status FileOpenReadable(const PlatformFilename& file_name, int* fd);
ARROW_EXPORT
Status FileOpenWritable(const PlatformFilename& file_name, bool write_only, bool truncate,
                        bool append, int* fd);

ARROW_EXPORT
Status FileRead(int fd, uint8_t* buffer, const int64_t nbytes, int64_t* bytes_read);
ARROW_EXPORT
Status FileReadAt(int fd, uint8_t* buffer, int64_t position, int64_t nbytes,
                  int64_t* bytes_read);
ARROW_EXPORT
Status FileWrite(int fd, const uint8_t* buffer, const int64_t nbytes);
ARROW_EXPORT
Status FileTruncate(int fd, const int64_t size);

ARROW_EXPORT
Status FileTell(int fd, int64_t* pos);
ARROW_EXPORT
Status FileSeek(int fd, int64_t pos);
ARROW_EXPORT
Status FileSeek(int fd, int64_t pos, int whence);
ARROW_EXPORT
Status FileGetSize(int fd, int64_t* size);

ARROW_EXPORT
Status FileClose(int fd);

ARROW_EXPORT
Status CreatePipe(int fd[2]);

ARROW_EXPORT
Status MemoryMapRemap(void* addr, size_t old_size, size_t new_size, int fildes,
                      void** new_addr);

ARROW_EXPORT
Status GetEnvVar(const char* name, std::string* out);
ARROW_EXPORT
Status GetEnvVar(const std::string& name, std::string* out);
ARROW_EXPORT
Status SetEnvVar(const char* name, const char* value);
ARROW_EXPORT
Status SetEnvVar(const std::string& name, const std::string& value);
ARROW_EXPORT
Status DelEnvVar(const char* name);
ARROW_EXPORT
Status DelEnvVar(const std::string& name);

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_IO_UTIL_H
