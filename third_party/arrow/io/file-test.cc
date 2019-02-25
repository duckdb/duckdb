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

#ifndef _WIN32
#include <fcntl.h>  // IWYU pragma: keep
#include <unistd.h>
#endif

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/test-common.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/util.h"
#include "arrow/util/io-util.h"

namespace arrow {
namespace io {

class FileTestFixture : public ::testing::Test {
 public:
  void SetUp() {
    path_ = "arrow-test-io-file.txt";
    EnsureFileDeleted();
  }

  void TearDown() { EnsureFileDeleted(); }

  void EnsureFileDeleted() {
    if (FileExists(path_)) {
      ARROW_UNUSED(std::remove(path_.c_str()));
    }
  }

 protected:
  std::string path_;
};

// ----------------------------------------------------------------------
// File output tests

class TestFileOutputStream : public FileTestFixture {
 public:
  void OpenFile(bool append = false) {
    ASSERT_OK(FileOutputStream::Open(path_, append, &file_));
    ASSERT_OK(FileOutputStream::Open(path_, append, &stream_));
  }
  void OpenFileDescriptor() {
    internal::PlatformFilename file_name;
    ASSERT_OK(internal::FileNameFromString(path_, &file_name));
    int fd_file, fd_stream;
    ASSERT_OK(internal::FileOpenWritable(file_name, true /* write_only */,
                                         false /* truncate */, false /* append */,
                                         &fd_file));
    ASSERT_OK(FileOutputStream::Open(fd_file, &file_));
    ASSERT_OK(internal::FileOpenWritable(file_name, true /* write_only */,
                                         false /* truncate */, false /* append */,
                                         &fd_stream));
    ASSERT_OK(FileOutputStream::Open(fd_stream, &stream_));
  }

 protected:
  std::shared_ptr<FileOutputStream> file_;
  std::shared_ptr<OutputStream> stream_;
};

#if defined(_MSC_VER)
TEST_F(TestFileOutputStream, FileNameWideCharConversionRangeException) {
  std::shared_ptr<FileOutputStream> file;
  // Form literal string with non-ASCII symbol(127 + 1)
  std::string file_name = "\x80";
  ASSERT_RAISES(Invalid, FileOutputStream::Open(file_name, &file));

  std::shared_ptr<OutputStream> stream;
  ASSERT_RAISES(Invalid, FileOutputStream::Open(file_name, &stream));

  std::shared_ptr<ReadableFile> rd_file;
  ASSERT_RAISES(Invalid, ReadableFile::Open(file_name, &rd_file));
}
#endif

TEST_F(TestFileOutputStream, DestructorClosesFile) {
  int fd_file, fd_stream;

  OpenFile();
  fd_file = file_->file_descriptor();
  fd_stream = std::static_pointer_cast<FileOutputStream>(stream_)->file_descriptor();
  ASSERT_FALSE(FileIsClosed(fd_file));
  file_.reset();
  ASSERT_TRUE(FileIsClosed(fd_file));
  ASSERT_FALSE(FileIsClosed(fd_stream));
  stream_.reset();
  ASSERT_TRUE(FileIsClosed(fd_stream));

  OpenFileDescriptor();
  fd_file = file_->file_descriptor();
  fd_stream = std::static_pointer_cast<FileOutputStream>(stream_)->file_descriptor();
  ASSERT_FALSE(FileIsClosed(fd_file));
  file_.reset();
  ASSERT_TRUE(FileIsClosed(fd_file));
  ASSERT_FALSE(FileIsClosed(fd_stream));
  stream_.reset();
  ASSERT_TRUE(FileIsClosed(fd_stream));
}

TEST_F(TestFileOutputStream, Close) {
  OpenFile();

  const char* data = "testdata";
  ASSERT_OK(file_->Write(data, strlen(data)));

  int fd = file_->file_descriptor();
  ASSERT_FALSE(file_->closed());
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(file_->closed());
  ASSERT_TRUE(FileIsClosed(fd));

  // Idempotent
  ASSERT_OK(file_->Close());

  AssertFileContents(path_, data);

  ASSERT_OK(stream_->Write(data, strlen(data)));

  fd = std::static_pointer_cast<FileOutputStream>(stream_)->file_descriptor();
  ASSERT_FALSE(stream_->closed());
  ASSERT_OK(stream_->Close());
  ASSERT_TRUE(stream_->closed());
  ASSERT_TRUE(FileIsClosed(fd));

  // Idempotent
  ASSERT_OK(stream_->Close());

  AssertFileContents(path_, data);
}

TEST_F(TestFileOutputStream, FromFileDescriptor) {
  OpenFileDescriptor();
  stream_.reset();

  std::string data1 = "test";
  ASSERT_OK(file_->Write(data1.data(), data1.size()));
  int fd = file_->file_descriptor();
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(fd));

  AssertFileContents(path_, data1);

  // Re-open at end of file
  internal::PlatformFilename file_name;
  ASSERT_OK(internal::FileNameFromString(path_, &file_name));
  ASSERT_OK(internal::FileOpenWritable(file_name, true /* write_only */,
                                       false /* truncate */, false /* append */, &fd));
  ASSERT_OK(internal::FileSeek(fd, 0, SEEK_END));
  ASSERT_OK(FileOutputStream::Open(fd, &stream_));

  std::string data2 = "data";
  ASSERT_OK(stream_->Write(data2.data(), data2.size()));
  ASSERT_OK(stream_->Close());

  AssertFileContents(path_, data1 + data2);
}

TEST_F(TestFileOutputStream, InvalidWrites) {
  OpenFile();

  const char* data = "";

  ASSERT_RAISES(IOError, file_->Write(data, -1));
  ASSERT_RAISES(IOError, stream_->Write(data, -1));
}

TEST_F(TestFileOutputStream, Tell) {
  OpenFile();

  int64_t position;
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(0, position);

  const char* data = "testdata";
  ASSERT_OK(file_->Write(data, 8));
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(8, position);

  ASSERT_OK(stream_->Tell(&position));
  ASSERT_EQ(0, position);

  ASSERT_OK(stream_->Write(data, 8));
  ASSERT_OK(stream_->Tell(&position));
  ASSERT_EQ(8, position);
}

TEST_F(TestFileOutputStream, TruncatesNewFile) {
  ASSERT_OK(FileOutputStream::Open(path_, &file_));

  const char* data = "testdata";
  ASSERT_OK(file_->Write(data, strlen(data)));
  ASSERT_OK(file_->Close());

  ASSERT_OK(FileOutputStream::Open(path_, &file_));
  ASSERT_OK(file_->Close());

  AssertFileContents(path_, "");

  // Same with stream-returning API
  ASSERT_OK(FileOutputStream::Open(path_, &stream_));

  ASSERT_OK(stream_->Write(data, strlen(data)));
  ASSERT_OK(stream_->Close());

  ASSERT_OK(FileOutputStream::Open(path_, &stream_));
  ASSERT_OK(stream_->Close());

  AssertFileContents(path_, "");
}

TEST_F(TestFileOutputStream, Append) {
  ASSERT_OK(FileOutputStream::Open(path_, &file_));
  {
    const char* data = "test";
    ASSERT_OK(file_->Write(data, strlen(data)));
  }
  ASSERT_OK(file_->Close());
  ASSERT_OK(FileOutputStream::Open(path_, true /* append */, &file_));
  {
    const char* data = "data";
    ASSERT_OK(file_->Write(data, strlen(data)));
  }
  ASSERT_OK(file_->Close());
  AssertFileContents(path_, "testdata");

  // Same with stream-returning API
  ASSERT_OK(FileOutputStream::Open(path_, &stream_));
  {
    const char* data = "test";
    ASSERT_OK(stream_->Write(data, strlen(data)));
  }
  ASSERT_OK(stream_->Close());

  ASSERT_OK(FileOutputStream::Open(path_, true /* append */, &stream_));
  {
    const char* data = "data";
    ASSERT_OK(stream_->Write(data, strlen(data)));
  }
  ASSERT_OK(stream_->Close());
  AssertFileContents(path_, "testdata");
}

// ----------------------------------------------------------------------
// File input tests

class TestReadableFile : public FileTestFixture {
 public:
  void OpenFile() { ASSERT_OK(ReadableFile::Open(path_, &file_)); }

  void MakeTestFile() {
    std::string data = "testdata";
    std::ofstream stream;
    stream.open(path_.c_str());
    stream << data;
  }

 protected:
  std::shared_ptr<ReadableFile> file_;
};

TEST_F(TestReadableFile, DestructorClosesFile) {
  MakeTestFile();

  int fd;
  {
    std::shared_ptr<ReadableFile> file;
    ASSERT_OK(ReadableFile::Open(path_, &file));
    fd = file->file_descriptor();
  }
  ASSERT_TRUE(FileIsClosed(fd));
}

TEST_F(TestReadableFile, Close) {
  MakeTestFile();
  OpenFile();

  int fd = file_->file_descriptor();
  ASSERT_FALSE(file_->closed());
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(file_->closed());

  ASSERT_TRUE(FileIsClosed(fd));

  // Idempotent
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(fd));
}

TEST_F(TestReadableFile, FromFileDescriptor) {
  MakeTestFile();

  internal::PlatformFilename file_name;
  int fd = -2;
  ASSERT_OK(internal::FileNameFromString(path_, &file_name));
  ASSERT_OK(internal::FileOpenReadable(file_name, &fd));
  ASSERT_GE(fd, 0);
  ASSERT_OK(internal::FileSeek(fd, 4));

  ASSERT_OK(ReadableFile::Open(fd, &file_));
  ASSERT_EQ(file_->file_descriptor(), fd);
  std::shared_ptr<Buffer> buf;
  ASSERT_OK(file_->Read(5, &buf));
  ASSERT_EQ(buf->size(), 4);
  ASSERT_TRUE(buf->Equals(Buffer("data")));

  ASSERT_FALSE(FileIsClosed(fd));
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(fd));
  // Idempotent
  ASSERT_OK(file_->Close());
  ASSERT_TRUE(FileIsClosed(fd));
}

TEST_F(TestReadableFile, Peek) {
  MakeTestFile();
  OpenFile();

  // Cannot peek
  auto view = file_->Peek(4);
  ASSERT_EQ(0, view.size());
}

TEST_F(TestReadableFile, SeekTellSize) {
  MakeTestFile();
  OpenFile();

  int64_t position;
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(0, position);

  ASSERT_OK(file_->Seek(4));
  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(4, position);

  ASSERT_OK(file_->Seek(100));
  ASSERT_OK(file_->Tell(&position));

  // Can seek past end of file
  ASSERT_EQ(100, position);

  int64_t size;
  ASSERT_OK(file_->GetSize(&size));
  ASSERT_EQ(8, size);

  ASSERT_OK(file_->Tell(&position));
  ASSERT_EQ(100, position);

  // does not support zero copy
  ASSERT_FALSE(file_->supports_zero_copy());
}

TEST_F(TestReadableFile, Read) {
  uint8_t buffer[50];

  MakeTestFile();
  OpenFile();

  int64_t bytes_read;
  ASSERT_OK(file_->Read(4, &bytes_read, buffer));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  ASSERT_OK(file_->Read(10, &bytes_read, buffer));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "data", 4));

  // Test incomplete read, ARROW-1094
  std::shared_ptr<Buffer> buf;
  int64_t size;
  ASSERT_OK(file_->GetSize(&size));

  ASSERT_OK(file_->Seek(1));
  ASSERT_OK(file_->Read(size, &buf));
  ASSERT_EQ(size - 1, buf->size());
}

TEST_F(TestReadableFile, ReadAt) {
  uint8_t buffer[50];
  const char* test_data = "testdata";

  MakeTestFile();
  OpenFile();

  int64_t bytes_read;

  ASSERT_OK(file_->ReadAt(0, 4, &bytes_read, buffer));
  ASSERT_EQ(4, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  ASSERT_OK(file_->ReadAt(1, 10, &bytes_read, buffer));
  ASSERT_EQ(7, bytes_read);
  ASSERT_EQ(0, std::memcmp(buffer, "estdata", 7));

  // Check buffer API
  std::shared_ptr<Buffer> buffer2;

  ASSERT_OK(file_->ReadAt(2, 5, &buffer2));
  ASSERT_EQ(5, buffer2->size());

  Buffer expected(reinterpret_cast<const uint8_t*>(test_data + 2), 5);
  ASSERT_TRUE(buffer2->Equals(expected));
}

TEST_F(TestReadableFile, NonExistentFile) {
  std::string path = "0xDEADBEEF.txt";
  Status s = ReadableFile::Open(path, &file_);
  ASSERT_TRUE(s.IsIOError());

  std::string message = s.message();
  ASSERT_NE(std::string::npos, message.find(path));
}

class MyMemoryPool : public MemoryPool {
 public:
  MyMemoryPool() : num_allocations_(0) {}

  Status Allocate(int64_t size, uint8_t** out) override {
    *out = reinterpret_cast<uint8_t*>(std::malloc(size));
    ++num_allocations_;
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override { std::free(buffer); }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    *ptr = reinterpret_cast<uint8_t*>(std::realloc(*ptr, new_size));

    if (*ptr == NULL) {
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }

    return Status::OK();
  }

  int64_t bytes_allocated() const override { return -1; }

  int64_t num_allocations() const { return num_allocations_.load(); }

 private:
  std::atomic<int64_t> num_allocations_;
};

TEST_F(TestReadableFile, CustomMemoryPool) {
  MakeTestFile();

  MyMemoryPool pool;
  ASSERT_OK(ReadableFile::Open(path_, &pool, &file_));

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(file_->ReadAt(0, 4, &buffer));
  ASSERT_OK(file_->ReadAt(4, 8, &buffer));

  ASSERT_EQ(2, pool.num_allocations());
}

TEST_F(TestReadableFile, ThreadSafety) {
  std::string data = "foobar";
  {
    std::ofstream stream;
    stream.open(path_.c_str());
    stream << data;
  }

  MyMemoryPool pool;
  ASSERT_OK(ReadableFile::Open(path_, &pool, &file_));

  std::atomic<int> correct_count(0);
  int niter = 30000;

  auto ReadData = [&correct_count, &data, &niter, this]() {
    std::shared_ptr<Buffer> buffer;

    for (int i = 0; i < niter; ++i) {
      const int offset = i % 3;
      ASSERT_OK(file_->ReadAt(offset, 3, &buffer));
      if (0 == memcmp(data.c_str() + offset, buffer->data(), 3)) {
        correct_count += 1;
      }
    }
  };

  std::thread thread1(ReadData);
  std::thread thread2(ReadData);

  thread1.join();
  thread2.join();

  ASSERT_EQ(niter * 2, correct_count);
}

// ----------------------------------------------------------------------
// Pipe I/O tests using FileOutputStream
// (cannot test using ReadableFile as it currently requires seeking)

class TestPipeIO : public ::testing::Test {
 public:
  void MakePipe() {
    int fd[2];
    ASSERT_OK(internal::CreatePipe(fd));
    r_ = fd[0];
    w_ = fd[1];
    ASSERT_GE(r_, 0);
    ASSERT_GE(w_, 0);
  }
  void ClosePipe() {
    if (r_ != -1) {
      ASSERT_OK(internal::FileClose(r_));
      r_ = -1;
    }
    if (w_ != -1) {
      ASSERT_OK(internal::FileClose(w_));
      w_ = -1;
    }
  }
  void TearDown() { ClosePipe(); }

 protected:
  int r_ = -1, w_ = -1;
};

TEST_F(TestPipeIO, TestWrite) {
  std::string data1 = "test", data2 = "data!";
  std::shared_ptr<FileOutputStream> file;
  uint8_t buffer[10];
  int64_t bytes_read;

  MakePipe();
  ASSERT_OK(FileOutputStream::Open(w_, &file));
  w_ = -1;  // now owned by FileOutputStream

  ASSERT_OK(file->Write(data1.data(), data1.size()));
  ASSERT_OK(internal::FileRead(r_, buffer, 4, &bytes_read));
  ASSERT_EQ(bytes_read, 4);
  ASSERT_EQ(0, std::memcmp(buffer, "test", 4));

  ASSERT_OK(file->Write(data2.data(), data2.size()));
  ASSERT_OK(internal::FileRead(r_, buffer, 4, &bytes_read));
  ASSERT_EQ(bytes_read, 4);
  ASSERT_EQ(0, std::memcmp(buffer, "data", 4));

  ASSERT_FALSE(file->closed());
  ASSERT_OK(file->Close());
  ASSERT_TRUE(file->closed());
  ASSERT_OK(internal::FileRead(r_, buffer, 2, &bytes_read));
  ASSERT_EQ(bytes_read, 1);
  ASSERT_EQ(0, std::memcmp(buffer, "!", 1));
  // EOF reached
  ASSERT_OK(internal::FileRead(r_, buffer, 2, &bytes_read));
  ASSERT_EQ(bytes_read, 0);
}

TEST_F(TestPipeIO, ReadableFileFails) {
  // ReadableFile fails on non-seekable fd
  std::shared_ptr<ReadableFile> file;
  ASSERT_RAISES(IOError, ReadableFile::Open(r_, &file));
}

// ----------------------------------------------------------------------
// Memory map tests

class TestMemoryMappedFile : public ::testing::Test, public MemoryMapFixture {
 public:
  void TearDown() { MemoryMapFixture::TearDown(); }
};

TEST_F(TestMemoryMappedFile, InvalidUsages) {}

TEST_F(TestMemoryMappedFile, ZeroSizeFlie) {
  std::string path = "io-memory-map-zero-size";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(0, path, &result));

  int64_t size = -1;
  ASSERT_OK(result->Tell(&size));
  ASSERT_EQ(0, size);
}

TEST_F(TestMemoryMappedFile, WriteRead) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  const int reps = 5;

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(reps * buffer_size, path, &result));

  int64_t position = 0;
  std::shared_ptr<Buffer> out_buffer;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(result->Write(buffer.data(), buffer_size));
    ASSERT_OK(result->ReadAt(position, buffer_size, &out_buffer));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

    position += buffer_size;
  }
}

TEST_F(TestMemoryMappedFile, WriteResizeRead) {
  const int64_t buffer_size = 1024;
  const int reps = 5;
  std::vector<std::vector<uint8_t>> buffers(reps);
  for (auto& b : buffers) {
    b.resize(buffer_size);
    random_bytes(buffer_size, 0, b.data());
  }

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  int64_t position = 0;
  std::shared_ptr<Buffer> out_buffer;
  for (int i = 0; i < reps; ++i) {
    if (i != 0) {
      ASSERT_OK(result->Resize(buffer_size * (i + 1)));
    }
    ASSERT_OK(result->Write(buffers[i].data(), buffer_size));
    ASSERT_OK(result->ReadAt(position, buffer_size, &out_buffer));

    ASSERT_EQ(out_buffer->size(), buffer_size);
    ASSERT_EQ(0, memcmp(out_buffer->data(), buffers[i].data(), buffer_size));
    out_buffer.reset();

    position += buffer_size;
  }
}

TEST_F(TestMemoryMappedFile, GetConstGetSize) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  const auto& const_result = *result;
  int64_t out_size;
  ASSERT_OK(const_result.GetSize(&out_size));
  ASSERT_EQ(buffer_size, out_size);
}

TEST_F(TestMemoryMappedFile, ResizeRaisesOnExported) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  std::shared_ptr<Buffer> out_buffer1, out_buffer2;
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->ReadAt(0, buffer_size, &out_buffer1));
  ASSERT_OK(result->ReadAt(0, buffer_size, &out_buffer2));
  ASSERT_EQ(0, memcmp(out_buffer1->data(), buffer.data(), buffer_size));
  ASSERT_EQ(0, memcmp(out_buffer2->data(), buffer.data(), buffer_size));

  // attempt resize
  ASSERT_RAISES(IOError, result->Resize(2 * buffer_size));

  out_buffer1.reset();

  ASSERT_RAISES(IOError, result->Resize(2 * buffer_size));

  out_buffer2.reset();

  ASSERT_OK(result->Resize(2 * buffer_size));

  int64_t map_size;
  ASSERT_OK(result->GetSize(&map_size));
  ASSERT_EQ(map_size, 2 * buffer_size);

  int64_t file_size;
  ASSERT_OK(internal::FileGetSize(result->file_descriptor(), &file_size));
  ASSERT_EQ(file_size, buffer_size * 2);
}

TEST_F(TestMemoryMappedFile, WriteReadZeroInitSize) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(0, path, &result));

  std::shared_ptr<Buffer> out_buffer;
  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->ReadAt(0, buffer_size, &out_buffer));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

  int64_t map_size;
  ASSERT_OK(result->GetSize(&map_size));
  ASSERT_EQ(map_size, buffer_size);
}

TEST_F(TestMemoryMappedFile, WriteThenShrink) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size * 2, path, &result));

  std::shared_ptr<Buffer> out_buffer;
  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->Resize(buffer_size / 2));

  ASSERT_OK(result->ReadAt(0, buffer_size / 2, &out_buffer));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size / 2));

  int64_t map_size;
  ASSERT_OK(result->GetSize(&map_size));
  ASSERT_EQ(map_size, buffer_size / 2);

  int64_t file_size;
  ASSERT_OK(internal::FileGetSize(result->file_descriptor(), &file_size));
  ASSERT_EQ(file_size, buffer_size / 2);
}

TEST_F(TestMemoryMappedFile, WriteThenShrinkToHalfThenWrite) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  std::shared_ptr<Buffer> out_buffer;
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->Resize(buffer_size / 2));

  int64_t position;
  ASSERT_OK(result->Tell(&position));
  ASSERT_EQ(position, buffer_size / 2);

  ASSERT_OK(result->ReadAt(0, buffer_size / 2, &out_buffer));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size / 2));
  out_buffer.reset();

  // should resume writing directly at the seam
  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data() + buffer_size / 2, buffer_size / 2));

  ASSERT_OK(result->ReadAt(0, buffer_size, &out_buffer));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));

  int64_t map_size;
  ASSERT_OK(result->GetSize(&map_size));
  ASSERT_EQ(map_size, buffer_size);

  int64_t file_size;
  ASSERT_OK(internal::FileGetSize(result->file_descriptor(), &file_size));
  ASSERT_EQ(file_size, buffer_size);
}

TEST_F(TestMemoryMappedFile, ResizeToZeroThanWrite) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  std::shared_ptr<Buffer> out_buffer;
  // just a sanity check that writing works ook
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->ReadAt(0, buffer_size, &out_buffer));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
  out_buffer.reset();

  ASSERT_OK(result->Resize(0));
  int64_t mapped_size;
  ASSERT_OK(result->GetSize(&mapped_size));
  ASSERT_EQ(mapped_size, 0);

  int64_t position;
  ASSERT_OK(result->Tell(&position));
  ASSERT_EQ(position, 0);

  int64_t file_size;
  ASSERT_OK(internal::FileGetSize(result->file_descriptor(), &file_size));
  ASSERT_EQ(file_size, 0);

  // provision a vector to the buffer size in case ReadAt decides
  // to read even though it shouldn't
  std::vector<uint8_t> should_remain_empty(buffer_size);
  int64_t bytes_read;
  ASSERT_OK(result->ReadAt(0, 1, &bytes_read,
                           reinterpret_cast<void*>(should_remain_empty.data())));
  ASSERT_EQ(bytes_read, 0);

  // just a sanity check that writing works ook
  ASSERT_OK(result->Resize(buffer_size));
  ASSERT_OK(result->Write(buffer.data(), buffer_size));
  ASSERT_OK(result->ReadAt(0, buffer_size, &out_buffer));
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, WriteAt) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  ASSERT_OK(result->WriteAt(0, buffer.data(), buffer_size / 2));

  ASSERT_OK(
      result->WriteAt(buffer_size / 2, buffer.data() + buffer_size / 2, buffer_size / 2));

  std::shared_ptr<Buffer> out_buffer;
  ASSERT_OK(result->ReadAt(0, buffer_size, &out_buffer));

  ASSERT_EQ(memcmp(out_buffer->data(), buffer.data(), buffer_size), 0);
}

TEST_F(TestMemoryMappedFile, WriteBeyondEnd) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  ASSERT_OK(result->Seek(1));
  ASSERT_RAISES(Invalid, result->Write(buffer.data(), buffer_size));

  // The position should remain unchanged afterwards
  int64_t position;
  ASSERT_OK(result->Tell(&position));
  ASSERT_EQ(position, 1);
}

TEST_F(TestMemoryMappedFile, WriteAtBeyondEnd) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);
  random_bytes(buffer_size, 0, buffer.data());

  std::string path = "io-memory-map-write-read-test";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(buffer_size, path, &result));

  ASSERT_RAISES(Invalid, result->WriteAt(1, buffer.data(), buffer_size));

  // The position should remain unchanged afterwards
  int64_t position;
  ASSERT_OK(result->Tell(&position));
  ASSERT_EQ(position, 0);
}

TEST_F(TestMemoryMappedFile, GetSize) {
  std::string path = "io-memory-map-get-size";
  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_OK(InitMemoryMap(16384, path, &result));

  int64_t size = -1;
  ASSERT_OK(result->GetSize(&size));
  ASSERT_EQ(16384, size);

  int64_t position = -1;
  ASSERT_OK(result->Tell(&position));
  ASSERT_EQ(0, position);
}

TEST_F(TestMemoryMappedFile, ReadOnly) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  const int reps = 5;

  std::string path = "ipc-read-only-test";
  std::shared_ptr<MemoryMappedFile> rwmmap;
  ASSERT_OK(InitMemoryMap(reps * buffer_size, path, &rwmmap));

  int64_t position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rwmmap->Close());

  std::shared_ptr<MemoryMappedFile> rommap;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READ, &rommap));

  position = 0;
  std::shared_ptr<Buffer> out_buffer;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rommap->ReadAt(position, buffer_size, &out_buffer));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rommap->Close());
}

TEST_F(TestMemoryMappedFile, DISABLED_ReadWriteOver4GbFile) {
  // ARROW-1096
  const int64_t buffer_size = 1000 * 1000;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(buffer_size, 0, buffer.data());

  const int64_t reps = 5000;

  std::string path = "ipc-read-over-4gb-file-test";
  std::shared_ptr<MemoryMappedFile> rwmmap;
  ASSERT_OK(InitMemoryMap(reps * buffer_size, path, &rwmmap));
  AppendFile(path);

  int64_t position = 0;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rwmmap->Close());

  std::shared_ptr<MemoryMappedFile> rommap;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READ, &rommap));

  position = 0;
  std::shared_ptr<Buffer> out_buffer;
  for (int i = 0; i < reps; ++i) {
    ASSERT_OK(rommap->ReadAt(position, buffer_size, &out_buffer));

    ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
    position += buffer_size;
  }
  ASSERT_OK(rommap->Close());
}

TEST_F(TestMemoryMappedFile, RetainMemoryMapReference) {
  // ARROW-494

  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  std::string path = "ipc-read-only-test";
  CreateFile(path, buffer_size);

  {
    std::shared_ptr<MemoryMappedFile> rwmmap;
    ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READWRITE, &rwmmap));
    ASSERT_OK(rwmmap->Write(buffer.data(), buffer_size));
    ASSERT_FALSE(rwmmap->closed());
    ASSERT_OK(rwmmap->Close());
    ASSERT_TRUE(rwmmap->closed());
  }

  std::shared_ptr<Buffer> out_buffer;

  {
    std::shared_ptr<MemoryMappedFile> rommap;
    ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READ, &rommap));
    ASSERT_OK(rommap->Read(buffer_size, &out_buffer));
    ASSERT_FALSE(rommap->closed());
    ASSERT_OK(rommap->Close());
    ASSERT_TRUE(rommap->closed());
  }

  // valgrind will catch if memory is unmapped
  ASSERT_EQ(0, memcmp(out_buffer->data(), buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, InvalidMode) {
  const int64_t buffer_size = 1024;
  std::vector<uint8_t> buffer(buffer_size);

  random_bytes(1024, 0, buffer.data());

  std::string path = "ipc-invalid-mode-test";
  CreateFile(path, buffer_size);

  std::shared_ptr<MemoryMappedFile> rommap;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READ, &rommap));

  ASSERT_RAISES(IOError, rommap->Write(buffer.data(), buffer_size));
}

TEST_F(TestMemoryMappedFile, InvalidFile) {
  std::string non_existent_path = "invalid-file-name-asfd";

  std::shared_ptr<MemoryMappedFile> result;
  ASSERT_RAISES(IOError,
                MemoryMappedFile::Open(non_existent_path, FileMode::READ, &result));
}

TEST_F(TestMemoryMappedFile, CastableToFileInterface) {
  std::shared_ptr<MemoryMappedFile> memory_mapped_file;
  std::shared_ptr<FileInterface> file = memory_mapped_file;
}

TEST_F(TestMemoryMappedFile, ThreadSafety) {
  std::string data = "foobar";
  std::string path = "ipc-multithreading-test";
  CreateFile(path, static_cast<int>(data.size()));

  std::shared_ptr<MemoryMappedFile> file;
  ASSERT_OK(MemoryMappedFile::Open(path, FileMode::READWRITE, &file));
  ASSERT_OK(file->Write(data.c_str(), static_cast<int64_t>(data.size())));

  std::atomic<int> correct_count(0);
  int niter = 10000;

  auto ReadData = [&correct_count, &data, &file, &niter]() {
    std::shared_ptr<Buffer> buffer;

    for (int i = 0; i < niter; ++i) {
      ASSERT_OK(file->ReadAt(0, 3, &buffer));
      if (0 == memcmp(data.c_str(), buffer->data(), 3)) {
        correct_count += 1;
      }
    }
  };

  std::thread thread1(ReadData);
  std::thread thread2(ReadData);

  thread1.join();
  thread2.join();

  ASSERT_EQ(niter * 2, correct_count);
}

}  // namespace io
}  // namespace arrow
