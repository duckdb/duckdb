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

#include "arrow/api.h"
#include "arrow/io/buffered.h"
#include "arrow/io/file.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io-util.h"

#include "benchmark/benchmark.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <valarray>

#ifndef _WIN32

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#endif

namespace arrow {

#ifndef _WIN32

std::string GetNullFile() { return "/dev/null"; }

const std::valarray<int64_t> small_sizes = {8, 24, 33, 1, 32, 192, 16, 40};
const std::valarray<int64_t> large_sizes = {8192, 100000};

constexpr int64_t kBufferSize = 4096;

class BackgroundReader {
  // A class that reads data in the background from a file descriptor

 public:
  static std::shared_ptr<BackgroundReader> StartReader(int fd) {
    std::shared_ptr<BackgroundReader> reader(new BackgroundReader(fd));
    reader->worker_.reset(new std::thread([=] { reader->LoopReading(); }));
    return reader;
  }
  void Stop() {
    const uint8_t data[] = "x";
    ABORT_NOT_OK(internal::FileWrite(wakeup_w_, data, 1));
  }
  void Join() { worker_->join(); }

  ~BackgroundReader() {
    for (int fd : {fd_, wakeup_r_, wakeup_w_}) {
      ABORT_NOT_OK(internal::FileClose(fd));
    }
  }

 protected:
  explicit BackgroundReader(int fd) : fd_(fd), total_bytes_(0) {
    // Prepare self-pipe trick
    int wakeupfd[2];
    ABORT_NOT_OK(internal::CreatePipe(wakeupfd));
    wakeup_r_ = wakeupfd[0];
    wakeup_w_ = wakeupfd[1];
    // Put fd in non-blocking mode
    fcntl(fd, F_SETFL, O_NONBLOCK);
  }

  void LoopReading() {
    struct pollfd pollfds[2];
    pollfds[0].fd = fd_;
    pollfds[0].events = POLLIN;
    pollfds[1].fd = wakeup_r_;
    pollfds[1].events = POLLIN;
    while (true) {
      int ret = poll(pollfds, 2, -1 /* timeout */);
      if (ret < 1) {
        std::cerr << "poll() failed with code " << ret << "\n";
        abort();
      }
      if (pollfds[1].revents & POLLIN) {
        // We're done
        break;
      }
      if (!(pollfds[0].revents & POLLIN)) {
        continue;
      }
      int64_t bytes_read;
      // There could be a spurious wakeup followed by EAGAIN
      ARROW_UNUSED(internal::FileRead(fd_, buffer_, buffer_size_, &bytes_read));
      total_bytes_ += bytes_read;
    }
  }

  int fd_, wakeup_r_, wakeup_w_;
  int64_t total_bytes_;

  static const int64_t buffer_size_ = 16384;
  uint8_t buffer_[buffer_size_];

  std::unique_ptr<std::thread> worker_;
};

// Set up a pipe with an OutputStream at one end and a BackgroundReader at
// the other end.
static void SetupPipeWriter(std::shared_ptr<io::OutputStream>* stream,
                            std::shared_ptr<BackgroundReader>* reader) {
  int fd[2];
  ABORT_NOT_OK(internal::CreatePipe(fd));
  ABORT_NOT_OK(io::FileOutputStream::Open(fd[1], stream));
  *reader = BackgroundReader::StartReader(fd[0]);
}

static void BenchmarkStreamingWrites(benchmark::State& state,
                                     std::valarray<int64_t> sizes,
                                     io::OutputStream* stream,
                                     BackgroundReader* reader = nullptr) {
  const std::string datastr(*std::max_element(std::begin(sizes), std::end(sizes)), 'x');
  const void* data = datastr.data();
  const int64_t sum_sizes = sizes.sum();

  while (state.KeepRunning()) {
    for (const int64_t size : sizes) {
      ABORT_NOT_OK(stream->Write(data, size));
    }
  }
  const int64_t total_bytes = static_cast<int64_t>(state.iterations()) * sum_sizes;
  state.SetBytesProcessed(total_bytes);

  if (reader != nullptr) {
    // Wake up and stop
    reader->Stop();
    reader->Join();
  }
  ABORT_NOT_OK(stream->Close());
}

// Benchmark writing to /dev/null
//
// This situation is irrealistic as the kernel likely doesn't
// copy the data at all, so we only measure small writes.

static void BM_FileOutputStreamSmallWritesToNull(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  ABORT_NOT_OK(io::FileOutputStream::Open(GetNullFile(), &stream));

  BenchmarkStreamingWrites(state, small_sizes, stream.get());
}

static void BM_BufferedOutputStreamSmallWritesToNull(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> file;
  ABORT_NOT_OK(io::FileOutputStream::Open(GetNullFile(), &file));

  std::shared_ptr<io::BufferedOutputStream> buffered_file;
  ABORT_NOT_OK(io::BufferedOutputStream::Create(kBufferSize, default_memory_pool(), file,
                                                &buffered_file));
  BenchmarkStreamingWrites(state, small_sizes, buffered_file.get());
}

// Benchmark writing a pipe
//
// This is slightly more realistic than the above

static void BM_FileOutputStreamSmallWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  BenchmarkStreamingWrites(state, small_sizes, stream.get(), reader.get());
}

static void BM_FileOutputStreamLargeWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  BenchmarkStreamingWrites(state, large_sizes, stream.get(), reader.get());
}

static void BM_BufferedOutputStreamSmallWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  std::shared_ptr<io::BufferedOutputStream> buffered_stream;
  ABORT_NOT_OK(io::BufferedOutputStream::Create(kBufferSize, default_memory_pool(),
                                                stream, &buffered_stream));
  BenchmarkStreamingWrites(state, small_sizes, buffered_stream.get(), reader.get());
}

static void BM_BufferedOutputStreamLargeWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  std::shared_ptr<io::BufferedOutputStream> buffered_stream;
  ABORT_NOT_OK(io::BufferedOutputStream::Create(kBufferSize, default_memory_pool(),
                                                stream, &buffered_stream));

  BenchmarkStreamingWrites(state, large_sizes, buffered_stream.get(), reader.get());
}

// We use real time as we don't want to count CPU time spent in the
// BackgroundReader thread

BENCHMARK(BM_FileOutputStreamSmallWritesToNull)
    ->Repetitions(2)
    ->MinTime(1.0)
    ->UseRealTime();
BENCHMARK(BM_FileOutputStreamSmallWritesToPipe)
    ->Repetitions(2)
    ->MinTime(1.0)
    ->UseRealTime();
BENCHMARK(BM_FileOutputStreamLargeWritesToPipe)
    ->Repetitions(2)
    ->MinTime(1.0)
    ->UseRealTime();

BENCHMARK(BM_BufferedOutputStreamSmallWritesToNull)
    ->Repetitions(2)
    ->MinTime(1.0)
    ->UseRealTime();
BENCHMARK(BM_BufferedOutputStreamSmallWritesToPipe)
    ->Repetitions(2)
    ->MinTime(1.0)
    ->UseRealTime();
BENCHMARK(BM_BufferedOutputStreamLargeWritesToPipe)
    ->Repetitions(2)
    ->MinTime(1.0)
    ->UseRealTime();

#endif  // ifndef _WIN32

}  // namespace arrow
