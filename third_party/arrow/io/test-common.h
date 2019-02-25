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

#ifndef ARROW_IO_TEST_COMMON_H
#define ARROW_IO_TEST_COMMON_H

#include <algorithm>
#include <cstdint>
#include <fstream>  // IWYU pragma: keep
#include <memory>
#include <string>
#include <vector>

#ifdef _WIN32
#include <crtdbg.h>
#include <io.h>
#else
#include <fcntl.h>
#endif

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace io {

static inline void AssertFileContents(const std::string& path,
                                      const std::string& contents) {
  std::shared_ptr<ReadableFile> rf;
  int64_t size;

  ASSERT_OK(ReadableFile::Open(path, &rf));
  ASSERT_OK(rf->GetSize(&size));
  ASSERT_EQ(size, contents.size());

  std::shared_ptr<Buffer> actual_data;
  ASSERT_OK(rf->Read(size, &actual_data));
  ASSERT_TRUE(actual_data->Equals(Buffer(contents)));
}

static inline bool FileExists(const std::string& path) {
  return std::ifstream(path.c_str()).good();
}

#if defined(_WIN32)
static inline void InvalidParamHandler(const wchar_t* expr, const wchar_t* func,
                                       const wchar_t* source_file,
                                       unsigned int source_line, uintptr_t reserved) {
  wprintf(L"Invalid parameter in function '%s'. Source: '%s' line %d expression '%s'\n",
          func, source_file, source_line, expr);
}
#endif

static inline bool FileIsClosed(int fd) {
#if defined(_WIN32)
  // Disables default behavior on wrong params which causes the application to crash
  // https://msdn.microsoft.com/en-us/library/ksazx244.aspx
  _set_invalid_parameter_handler(InvalidParamHandler);

  // Disables possible assertion alert box on invalid input arguments
  _CrtSetReportMode(_CRT_ASSERT, 0);

  int new_fd = _dup(fd);
  if (new_fd == -1) {
    return errno == EBADF;
  }
  _close(new_fd);
  return false;
#else
  if (-1 != fcntl(fd, F_GETFD)) {
    return false;
  }
  return errno == EBADF;
#endif
}

static inline Status ZeroMemoryMap(MemoryMappedFile* file) {
  constexpr int64_t kBufferSize = 512;
  static constexpr uint8_t kZeroBytes[kBufferSize] = {0};

  RETURN_NOT_OK(file->Seek(0));
  int64_t position = 0;
  int64_t file_size;
  RETURN_NOT_OK(file->GetSize(&file_size));

  int64_t chunksize;
  while (position < file_size) {
    chunksize = std::min(kBufferSize, file_size - position);
    RETURN_NOT_OK(file->Write(kZeroBytes, chunksize));
    position += chunksize;
  }
  return Status::OK();
}

class MemoryMapFixture {
 public:
  void TearDown() {
    for (auto path : tmp_files_) {
      ARROW_UNUSED(std::remove(path.c_str()));
    }
  }

  void CreateFile(const std::string& path, int64_t size) {
    std::shared_ptr<MemoryMappedFile> file;
    ASSERT_OK(MemoryMappedFile::Create(path, size, &file));
    tmp_files_.push_back(path);
  }

  Status InitMemoryMap(int64_t size, const std::string& path,
                       std::shared_ptr<MemoryMappedFile>* mmap) {
    RETURN_NOT_OK(MemoryMappedFile::Create(path, size, mmap));
    tmp_files_.push_back(path);
    return Status::OK();
  }

  void AppendFile(const std::string& path) { tmp_files_.push_back(path); }

 private:
  std::vector<std::string> tmp_files_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_IO_TEST_COMMON_H
