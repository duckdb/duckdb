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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/readahead.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace io {
namespace internal {

class LockedInputStream : public InputStream {
 public:
  explicit LockedInputStream(const std::shared_ptr<InputStream>& stream)
      : stream_(stream) {}

  Status Close() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stream_->Close();
  }

  bool closed() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stream_->closed();
  }

  Status Tell(int64_t* position) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stream_->Tell(position);
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* buffer) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stream_->Read(nbytes, bytes_read, buffer);
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stream_->Read(nbytes, out);
  }

  bool supports_zero_copy() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stream_->supports_zero_copy();
  }

  util::string_view Peek(int64_t nbytes) const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stream_->Peek(nbytes);
  }

 protected:
  std::shared_ptr<InputStream> stream_;
  mutable std::mutex mutex_;
};

static void sleep_for(double seconds) {
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(static_cast<int64_t>(seconds * 1e9)));
}

static void busy_wait(double seconds, std::function<bool()> predicate) {
  const double period = 0.001;
  for (int i = 0; !predicate() && i * period < seconds; ++i) {
    sleep_for(period);
  }
}

std::shared_ptr<InputStream> DataReader(const std::string& data) {
  std::shared_ptr<Buffer> buffer;
  ABORT_NOT_OK(Buffer::FromString(data, &buffer));
  return std::make_shared<LockedInputStream>(std::make_shared<BufferReader>(buffer));
}

static int64_t WaitForPosition(const FileInterface& file, int64_t expected,
                               double seconds = 0.2) {
  int64_t pos = -1;
  busy_wait(seconds, [&]() -> bool {
    ABORT_NOT_OK(file.Tell(&pos));
    return pos >= expected;
  });
  return pos;
}

static void AssertEventualPosition(const FileInterface& file, int64_t expected) {
  int64_t pos = WaitForPosition(file, expected);
  ASSERT_EQ(pos, expected) << "File didn't reach expected position";
}

static void AssertPosition(const FileInterface& file, int64_t expected) {
  int64_t pos = -1;
  ABORT_NOT_OK(file.Tell(&pos));
  ASSERT_EQ(pos, expected) << "File didn't reach expected position";
}

template <typename Expected>
static void AssertReadaheadBuffer(const ReadaheadBuffer& buf,
                                  std::set<int64_t> left_paddings,
                                  std::set<int64_t> right_paddings,
                                  const Expected& expected_data) {
  ASSERT_TRUE(left_paddings.count(buf.left_padding))
      << "Left padding (" << buf.left_padding << ") not amongst expected values";
  ASSERT_TRUE(right_paddings.count(buf.right_padding))
      << "Right padding (" << buf.right_padding << ") not amongst expected values";
  auto actual_data =
      SliceBuffer(buf.buffer, buf.left_padding,
                  buf.buffer->size() - buf.left_padding - buf.right_padding);
  AssertBufferEqual(*actual_data, expected_data);
}

static void AssertReadaheadBufferEOF(const ReadaheadBuffer& buf) {
  ASSERT_EQ(buf.buffer.get(), nullptr) << "Expected EOF signalled by null buffer pointer";
}

TEST(ReadaheadSpooler, BasicReads) {
  // Test basic reads
  auto data_reader = DataReader("0123456789");
  ReadaheadSpooler spooler(data_reader, 2, 3);
  ReadaheadBuffer buf;

  AssertEventualPosition(*data_reader, 3 * 2);

  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "01");
  AssertEventualPosition(*data_reader, 4 * 2);
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "23");
  AssertEventualPosition(*data_reader, 5 * 2);
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "45");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "67");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "89");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBufferEOF(buf);
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBufferEOF(buf);
}

TEST(ReadaheadSpooler, ShortReadAtEnd) {
  auto data_reader = DataReader("01234");
  ReadaheadSpooler spooler(data_reader, 3, 2);
  ReadaheadBuffer buf;

  AssertEventualPosition(*data_reader, 5);

  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "012");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "34");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBufferEOF(buf);
}

TEST(ReadaheadSpooler, Close) {
  // Closing should stop reads
  auto data_reader = DataReader("0123456789");
  ReadaheadSpooler spooler(data_reader, 2, 2);
  ReadaheadBuffer buf;

  AssertEventualPosition(*data_reader, 2 * 2);
  ASSERT_OK(spooler.Close());

  // XXX not sure this makes sense
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "01");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {0}, {0}, "23");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBufferEOF(buf);
  AssertPosition(*data_reader, 2 * 2);

  // Idempotency
  ASSERT_OK(spooler.Close());
}

TEST(ReadaheadSpooler, Paddings) {
  auto data_reader = DataReader("0123456789");
  ReadaheadSpooler spooler(data_reader, 2, 2, 1 /* left_padding */,
                           4 /* right_padding */);
  ReadaheadBuffer buf;

  AssertEventualPosition(*data_reader, 2 * 2);
  ASSERT_EQ(spooler.GetLeftPadding(), 1);
  ASSERT_EQ(spooler.GetRightPadding(), 4);
  spooler.SetLeftPadding(3);
  spooler.SetRightPadding(2);
  ASSERT_EQ(spooler.GetLeftPadding(), 3);
  ASSERT_EQ(spooler.GetRightPadding(), 2);

  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {1}, {4}, "01");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {1}, {4}, "23");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {3}, {2}, "45");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {3}, {2}, "67");
  spooler.SetLeftPadding(4);
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBuffer(buf, {3, 4}, {2}, "89");
  ASSERT_OK(spooler.Read(&buf));
  AssertReadaheadBufferEOF(buf);
}

TEST(ReadaheadSpooler, StressReads) {
  // NBYTES % READ_SIZE != 0 ensures a short read at end
#if defined(ARROW_VALGRIND)
  const int64_t NBYTES = 101;
#else
  const int64_t NBYTES = 50001;
#endif
  const int64_t READ_SIZE = 2;

  std::shared_ptr<ResizableBuffer> data;
  ASSERT_OK(MakeRandomByteBuffer(NBYTES, default_memory_pool(), &data));
  auto data_reader = std::make_shared<BufferReader>(data);

  ReadaheadSpooler spooler(data_reader, READ_SIZE, 7);
  int64_t pos = 0;
  std::vector<ReadaheadBuffer> readahead_buffers;

  // Stress Read() calls while the background thread is reading ahead
  while (pos < NBYTES) {
    ReadaheadBuffer buf;
    ASSERT_OK(spooler.Read(&buf));
    ASSERT_NE(buf.buffer.get(), nullptr) << "Got premature EOF at index " << pos;
    pos += buf.buffer->size() - buf.left_padding - buf.right_padding;
    readahead_buffers.push_back(std::move(buf));
  }
  // At EOF
  {
    ReadaheadBuffer buf;
    ASSERT_OK(spooler.Read(&buf));
    AssertReadaheadBufferEOF(buf);
  }

  pos = 0;
  for (const auto& buf : readahead_buffers) {
    auto expected_data = SliceBuffer(data, pos, std::min(READ_SIZE, NBYTES - pos));
    AssertReadaheadBuffer(buf, {0}, {0}, *expected_data);
    pos += expected_data->size();
  }
  // Got exactly the total bytes length
  ASSERT_EQ(pos, NBYTES);
}

}  // namespace internal
}  // namespace io
}  // namespace arrow
