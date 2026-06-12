#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/async_file_writer.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <thread>

using namespace duckdb;

namespace {

class TrackingWriteFileSystem : public LocalFileSystem {
public:
	explicit TrackingWriteFileSystem(bool local_file_p = true) : local_file(local_file_p) {
	}

	string GetName() const override {
		return "TrackingWriteFileSystem";
	}

	bool IsLocalFileSystem() const override {
		return local_file;
	}

	bool OnDiskFile(FileHandle &) override {
		return local_file;
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		if (fail_writes) {
			throw IOException("Injected async write failure");
		}
		RecordWrite(nr_bytes, NumericLimits<idx_t>::Maximum());
		return LocalFileSystem::Write(handle, buffer, nr_bytes);
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		if (nr_bytes == 0) {
			LocalFileSystem::Write(handle, buffer, nr_bytes, location);
			return;
		}
		if (fail_writes) {
			throw IOException("Injected async write failure");
		}
		RecordWrite(nr_bytes, location);
		LocalFileSystem::Write(handle, buffer, nr_bytes, location);
	}

protected:
	void RecordWrite(int64_t nr_bytes, idx_t location) {
		lock_guard<mutex> guard(lock);
		write_sizes.push_back(UnsafeNumericCast<idx_t>(nr_bytes));
		write_offsets.push_back(location);
	}

public:
	mutex lock;
	vector<idx_t> write_sizes;
	vector<idx_t> write_offsets;
	bool fail_writes = false;

private:
	bool local_file;
};

class BlockingWriteFileSystem : public TrackingWriteFileSystem {
public:
	explicit BlockingWriteFileSystem(bool positional_supported_p, bool local_file_p = true)
	    : TrackingWriteFileSystem(local_file_p), positional_supported(positional_supported_p) {
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		if (!positional_supported) {
			throw NotImplementedException("Injected missing positional write support");
		}
		if (nr_bytes == 0) {
			TrackingWriteFileSystem::Write(handle, buffer, nr_bytes, location);
			return;
		}

		EnterWrite();
		try {
			TrackingWriteFileSystem::Write(handle, buffer, nr_bytes, location);
			LeaveWrite();
		} catch (...) {
			LeaveWrite();
			throw;
		}
	}

	bool SupportsPositionalWrites(FileHandle &handle) override {
		return positional_supported;
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		EnterWrite();
		try {
			auto result = TrackingWriteFileSystem::Write(handle, buffer, nr_bytes);
			LeaveWrite();
			return result;
		} catch (...) {
			LeaveWrite();
			throw;
		}
	}

	bool WaitForBlockedWrites(idx_t count) {
		unique_lock<mutex> guard(block_lock);
		return cv.wait_for(guard, std::chrono::seconds(5), [&]() { return blocked_writes >= count; });
	}

	idx_t BlockedWrites() {
		lock_guard<mutex> guard(block_lock);
		return blocked_writes;
	}

	void ReleaseWrites() {
		{
			lock_guard<mutex> guard(block_lock);
			release_writes = true;
		}
		cv.notify_all();
	}

	idx_t MaxActiveWrites() {
		lock_guard<mutex> guard(block_lock);
		return max_active_writes;
	}

private:
	void EnterWrite() {
		unique_lock<mutex> guard(block_lock);
		active_writes++;
		max_active_writes = MaxValue(max_active_writes, active_writes);
		blocked_writes++;
		cv.notify_all();
		cv.wait(guard, [&]() { return release_writes; });
	}

	void LeaveWrite() {
		{
			lock_guard<mutex> guard(block_lock);
			D_ASSERT(active_writes > 0);
			active_writes--;
		}
		cv.notify_all();
	}

private:
	const bool positional_supported;

	mutex block_lock;
	std::condition_variable cv;
	idx_t active_writes = 0;
	idx_t max_active_writes = 0;
	idx_t blocked_writes = 0;
	bool release_writes = false;
};

class SequentialExplicitOffsetWriteFileSystem : public BlockingWriteFileSystem {
public:
	SequentialExplicitOffsetWriteFileSystem() : BlockingWriteFileSystem(false, false) {
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		if (nr_bytes == 0) {
			return;
		}
		if (location != next_write_offset) {
			throw InternalException("Injected non-sequential write");
		}
		next_write_offset += UnsafeNumericCast<idx_t>(nr_bytes);
		BlockingWriteFileSystem::Write(handle, buffer, nr_bytes);
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return BlockingWriteFileSystem::Write(handle, buffer, nr_bytes);
	}

private:
	idx_t next_write_offset = 0;
};

class FailingBlockedWriteFileSystem : public TrackingWriteFileSystem {
public:
	FailingBlockedWriteFileSystem() : TrackingWriteFileSystem(false) {
	}

	string GetName() const override {
		return "FailingBlockedWriteFileSystem";
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		if (nr_bytes == 0) {
			TrackingWriteFileSystem::Write(handle, buffer, nr_bytes, location);
			return;
		}

		idx_t write_id;
		{
			unique_lock<mutex> guard(block_lock);
			write_id = ++entered_writes;
			cv.notify_all();
			if (write_id == 1) {
				cv.wait(guard, [&]() { return fail_first_write; });
			} else if (write_id == 2) {
				cv.wait(guard, [&]() { return release_second_write; });
			}
		}

		if (write_id == 1) {
			throw IOException("Injected async write failure");
		}
		TrackingWriteFileSystem::Write(handle, buffer, nr_bytes, location);
	}

	bool WaitForEnteredWrites(idx_t count) {
		unique_lock<mutex> guard(block_lock);
		return cv.wait_for(guard, std::chrono::seconds(5), [&]() { return entered_writes >= count; });
	}

	void FailFirstWrite() {
		{
			lock_guard<mutex> guard(block_lock);
			fail_first_write = true;
		}
		cv.notify_all();
	}

	void ReleaseSecondWrite() {
		{
			lock_guard<mutex> guard(block_lock);
			release_second_write = true;
		}
		cv.notify_all();
	}

private:
	mutex block_lock;
	std::condition_variable cv;
	idx_t entered_writes = 0;
	bool fail_first_write = false;
	bool release_second_write = false;
};

class StringAsyncWriteBuffer : public AsyncWriteBuffer {
public:
	explicit StringAsyncWriteBuffer(string data_p) : data(std::move(data_p)) {
	}

	data_ptr_t Ptr() override {
		return data_ptr_cast(data.data());
	}

	idx_t Size() const override {
		return data.size();
	}

private:
	string data;
};

class TrackingAsyncWriteTarget : public AsyncWriteTarget {
public:
	void Write(data_ptr_t buffer, idx_t size, idx_t offset) override {
		lock_guard<mutex> guard(lock);
		writes.emplace_back(const_char_ptr_cast(buffer), UnsafeNumericCast<size_t>(size));
		offsets.push_back(offset);
	}

	mutex lock;
	vector<string> writes;
	vector<idx_t> offsets;
};

class BlockingAsyncWriteTarget : public AsyncWriteTarget {
public:
	void Write(data_ptr_t buffer, idx_t size, idx_t offset) override {
		(void)buffer;
		unique_lock<mutex> guard(lock);
		active_writes++;
		max_active_writes = MaxValue(max_active_writes, active_writes);
		entered_writes++;
		cv.notify_all();
		cv.wait(guard, [&]() { return release_writes; });
		write_sizes.push_back(size);
		offsets.push_back(offset);
		active_writes--;
		cv.notify_all();
	}

	bool WaitForEnteredWrites(idx_t count) {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(5), [&]() { return entered_writes >= count; });
	}

	void ReleaseWrites() {
		{
			lock_guard<mutex> guard(lock);
			release_writes = true;
		}
		cv.notify_all();
	}

	idx_t MaxActiveWrites() {
		lock_guard<mutex> guard(lock);
		return max_active_writes;
	}

	mutex lock;
	std::condition_variable cv;
	vector<idx_t> write_sizes;
	vector<idx_t> offsets;
	idx_t entered_writes = 0;
	idx_t active_writes = 0;
	idx_t max_active_writes = 0;
	bool release_writes = false;
};

class BlockingAsyncTaskState {
public:
	bool WaitForStarted(idx_t count) {
		unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(5), [&]() { return started_tasks >= count; });
	}

	void Release() {
		{
			lock_guard<mutex> guard(lock);
			released = true;
		}
		cv.notify_all();
	}

	void Enter() {
		unique_lock<mutex> guard(lock);
		started_tasks++;
		cv.notify_all();
		cv.wait(guard, [&]() { return released; });
	}

private:
	mutex lock;
	std::condition_variable cv;
	idx_t started_tasks = 0;
	bool released = false;
};

class BlockingAsyncTask : public BaseExecutorTask {
public:
	BlockingAsyncTask(TaskExecutor &executor, BlockingAsyncTaskState &state_p)
	    : BaseExecutorTask(executor), state(state_p) {
	}

	void ExecuteTask() override {
		state.Enter();
	}

private:
	BlockingAsyncTaskState &state;
};

class AsyncThreadBlocker {
public:
	AsyncThreadBlocker(ClientContext &context, idx_t task_count_p)
	    : task_count(task_count_p), executor(context, TaskSchedulerType::ASYNC) {
		for (idx_t task_idx = 0; task_idx < task_count; task_idx++) {
			executor.ScheduleTask(make_uniq<BlockingAsyncTask>(executor, state));
		}
	}

	~AsyncThreadBlocker() {
		Release();
	}

	bool WaitForStarted() {
		return state.WaitForStarted(task_count);
	}

	void Release() {
		if (released) {
			return;
		}
		state.Release();
		try {
			executor.WorkOnTasks();
		} catch (...) {
		}
		released = true;
	}

private:
	idx_t task_count;
	BlockingAsyncTaskState state;
	TaskExecutor executor;
	bool released = false;
};

static string ReadFile(const string &path) {
	LocalFileSystem fs;
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = NumericCast<idx_t>(handle->GetFileSize());
	string result(file_size, '\0');
	handle->Read(data_ptr_cast(result.data()), file_size);
	return result;
}

static unique_ptr<Connection> CreateConnectionWithAsyncThreads(DuckDB &db, idx_t async_threads = 1) {
	auto con = make_uniq<Connection>(db);
	REQUIRE_NO_FAIL(con->Query("SET async_threads=" + to_string(async_threads)));
	return con;
}

static unique_ptr<Connection> CreateConnectionWithNoAsyncThreads(DuckDB &db) {
	auto con = make_uniq<Connection>(db);
	REQUIRE_NO_FAIL(con->Query("SET async_threads=0"));
	return con;
}

TEST_CASE("AsyncWriteQueue writes synchronously without async threads", "[async_write_queue]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingAsyncWriteTarget target;
	AsyncWriteQueue queue(*con->context, target);

	mutex completion_lock;
	vector<idx_t> completion_offsets;
	vector<idx_t> completion_sizes;
	bool saw_error = false;
	auto completion = [&](idx_t offset, idx_t size, optional_ptr<const ErrorData> error) {
		lock_guard<mutex> guard(completion_lock);
		completion_offsets.push_back(offset);
		completion_sizes.push_back(size);
		saw_error = saw_error || error;
	};

	queue.Submit(AsyncWriteRequest(make_uniq<StringAsyncWriteBuffer>("abc"), 7, completion));
	queue.Close();

	REQUIRE(!queue.IsAsync());
	REQUIRE(target.writes.size() == 1);
	REQUIRE(target.writes[0] == "abc");
	REQUIRE(target.offsets[0] == 7);
	REQUIRE(completion_offsets.size() == 1);
	REQUIRE(completion_offsets[0] == 7);
	REQUIRE(completion_sizes[0] == 3);
	REQUIRE(!saw_error);
}

TEST_CASE("AsyncWriteQueue drains positional requests on multiple async threads", "[async_write_queue]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	BlockingAsyncWriteTarget target;
	AsyncWriteQueue queue(*con->context, target);

	mutex completion_lock;
	vector<idx_t> completion_offsets;
	vector<idx_t> completion_sizes;
	bool saw_error = false;
	auto completion = [&](idx_t offset, idx_t size, optional_ptr<const ErrorData> error) {
		lock_guard<mutex> guard(completion_lock);
		completion_offsets.push_back(offset);
		completion_sizes.push_back(size);
		saw_error = saw_error || error;
	};

	auto write_size = AsyncWriteQueue::DEFAULT_TASK_BYTE_BUDGET + 1;
	queue.Submit(AsyncWriteRequest(
	    make_uniq<StringAsyncWriteBuffer>(string(UnsafeNumericCast<size_t>(write_size), 'a')), 0, completion));
	queue.Submit(AsyncWriteRequest(
	    make_uniq<StringAsyncWriteBuffer>(string(UnsafeNumericCast<size_t>(write_size), 'b')), write_size, completion));
	REQUIRE(target.WaitForEnteredWrites(2));
	REQUIRE(target.MaxActiveWrites() >= 2);

	target.ReleaseWrites();
	queue.Close();

	REQUIRE(target.write_sizes.size() == 2);
	REQUIRE(completion_offsets.size() == 2);
	REQUIRE(completion_sizes.size() == 2);
	REQUIRE(!saw_error);
	bool saw_first = false;
	bool saw_second = false;
	for (auto offset : completion_offsets) {
		if (offset == 0) {
			saw_first = true;
		}
		if (offset == write_size) {
			saw_second = true;
		}
	}
	REQUIRE(saw_first);
	REQUIRE(saw_second);
}

static void TestQueuedDrainTaskCoversNewTinyTail(bool local_file, const string &path_name) {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	AsyncThreadBlocker async_thread_blocker(*con->context, 2);
	REQUIRE(async_thread_blocker.WaitForStarted());

	BlockingWriteFileSystem fs(true, local_file);
	auto path = TestCreatePath(path_name);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string large(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET * 2 + 1, 'x');
	string tail = "tail";

	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(large));
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(tail));
	CHECK(fs.BlockedWrites() == 0);

	async_thread_blocker.Release();
	CHECK(fs.WaitForBlockedWrites(1));
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	CHECK(fs.BlockedWrites() == 1);

	fs.ReleaseWrites();
	writer.Close();
	REQUIRE(ReadFile(path) == large + tail);
	fs.RemoveFile(path);
}

} // namespace

TEST_CASE("AsyncFileWriter requires a client context", "[async_file_writer]") {
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_no_context.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	REQUIRE_THROWS_AS(AsyncFileWriter(QueryContext(), fs, path), InvalidInputException);
	REQUIRE(!fs.FileExists(path));
}

TEST_CASE("AsyncFileWriter registers writes before async drain", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_register.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(const_data_ptr_cast("ab"), 2);
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("cd"));

		REQUIRE(writer.GetTotalWritten() == 4);
		REQUIRE(fs.write_sizes.empty());
		batch_guard.Finish();
	}

	writer.Close();
	REQUIRE(ReadFile(path) == "abcd");
	REQUIRE(fs.write_sizes.size() == 1);
	REQUIRE(fs.write_sizes[0] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter writes synchronously without async threads", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_sync.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(const_data_ptr_cast("ab"), 2);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("cd"));

	REQUIRE(writer.GetTotalWritten() == 4);
	REQUIRE(fs.write_sizes.empty());

	writer.Close();
	REQUIRE(ReadFile(path) == "abcd");
	REQUIRE(fs.write_sizes.size() == 1);
	REQUIRE(fs.write_sizes[0] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter keeps synchronous direct writes aligned with staged writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_sync_direct_then_staged.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string large(2 * AsyncFileWriter::DEFAULT_COPIED_BUFFER_CAPACITY, 'x');
	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(const_data_ptr_cast("head"), 4);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(large));
	writer.WriteData(const_data_ptr_cast("tail"), 4);

	writer.Close();
	REQUIRE(ReadFile(path) == "head" + large + "tail");
	REQUIRE(fs.write_sizes.size() == 3);
	REQUIRE(fs.write_sizes[0] == AsyncFileWriter::DEFAULT_COPIED_BUFFER_CAPACITY);
	REQUIRE(fs.write_sizes[1] == large.size() + 4 - AsyncFileWriter::DEFAULT_COPIED_BUFFER_CAPACITY);
	REQUIRE(fs.write_sizes[2] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter buffers small copied writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_small_copied.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(const_data_ptr_cast("PA"), 2);
	writer.WriteData(const_data_ptr_cast("R1"), 2);

	REQUIRE(writer.GetTotalWritten() == 4);
	REQUIRE(fs.write_sizes.empty());

	writer.Close();
	REQUIRE(ReadFile(path) == "PAR1");
	REQUIRE(fs.write_sizes.size() == 1);
	REQUIRE(fs.write_sizes[0] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter preserves order around large copied writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_small_large_small.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		string large(8192, 'x');
		writer.WriteData(const_data_ptr_cast("PAR1"), 4);
		writer.WriteData(const_data_ptr_cast(large.data()), large.size());
		writer.WriteData(const_data_ptr_cast("PARE"), 4);
		REQUIRE(writer.GetTotalWritten() == 8200);
		batch_guard.Finish();
	}
	writer.Close();

	REQUIRE(ReadFile(path) == "PAR1" + string(8192, 'x') + "PARE");
	REQUIRE(fs.write_sizes.size() == 3);
	REQUIRE(fs.write_sizes[0] == 4);
	REQUIRE(fs.write_sizes[1] == 8192);
	REQUIRE(fs.write_sizes[2] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter copies transient WriteData input", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_copy.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string payload = "abcdef";
	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(const_data_ptr_cast(payload.data()), payload.size());
	payload = "XXXXXX";
	writer.Close();

	REQUIRE(ReadFile(path) == "abcdef");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter writes at truncated offset", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_truncate_write.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(const_data_ptr_cast("abcdef"), 6);
	writer.Truncate(3);
	writer.WriteData(const_data_ptr_cast("XYZ"), 3);
	writer.Close();

	REQUIRE(ReadFile(path) == "abcXYZ");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter flush waits for pending writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_flush.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	auto batch_guard = writer.StartBatch();
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	REQUIRE(fs.write_sizes.empty());

	writer.Flush();
	REQUIRE(ReadFile(path) == "abcd");
	REQUIRE(fs.write_sizes.size() == 1);
	batch_guard.Finish();
	writer.Close();
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter flush preserves an open batch", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_flush_preserves_batch.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("ab"));

		writer.Flush();
		REQUIRE(ReadFile(path) == "ab");
		REQUIRE(fs.write_sizes.size() == 1);

		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("cd"));
		REQUIRE(fs.write_sizes.size() == 1);
		batch_guard.Finish();
	}

	writer.Close();
	REQUIRE(ReadFile(path) == "abcd");
	REQUIRE(fs.write_sizes.size() == 2);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter keeps large owned buffers as separate writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_large.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string large(AsyncFileWriter::DEFAULT_LOCAL_COALESCE_THRESHOLD, 'a');
	string small_a(AsyncFileWriter::DEFAULT_LOCAL_COALESCE_THRESHOLD / 2, 'b');
	string small_b(AsyncFileWriter::DEFAULT_LOCAL_COALESCE_THRESHOLD / 2, 'c');

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(large));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(small_a));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(small_b));
		batch_guard.Finish();
	}
	writer.Close();

	REQUIRE(ReadFile(path) == large + small_a + small_b);
	REQUIRE(fs.write_sizes.size() == 2);
	REQUIRE(fs.write_sizes[0] == large.size());
	REQUIRE(fs.write_sizes[1] == small_a.size() + small_b.size());
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter drains positional writes on multiple async threads", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	BlockingWriteFileSystem fs(true, false);
	auto path = TestCreatePath("async_file_writer_parallel_positional.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'a');
	string second(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'b');

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(second));
		batch_guard.Finish();
	}

	auto saw_two_blocked_writes = fs.WaitForBlockedWrites(2);
	auto max_active_writes = fs.MaxActiveWrites();
	fs.ReleaseWrites();
	writer.Close();

	REQUIRE(saw_two_blocked_writes);
	REQUIRE(max_active_writes >= 2);
	REQUIRE(ReadFile(path) == first + second);
	REQUIRE(fs.write_sizes.size() == 2);

	bool saw_first_offset = false;
	bool saw_second_offset = false;
	for (auto offset : fs.write_offsets) {
		if (offset == 0) {
			saw_first_offset = true;
		}
		if (offset == first.size()) {
			saw_second_offset = true;
		}
	}
	REQUIRE(saw_first_offset);
	REQUIRE(saw_second_offset);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter drains local positional writes on multiple async threads", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	BlockingWriteFileSystem fs(true);
	auto path = TestCreatePath("async_file_writer_local_parallel_positional.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'a');
	string second(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'b');

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(second));
		batch_guard.Finish();
	}

	auto saw_two_blocked_writes = fs.WaitForBlockedWrites(2);
	auto max_active_writes = fs.MaxActiveWrites();

	fs.ReleaseWrites();
	writer.Close();
	REQUIRE(saw_two_blocked_writes);
	REQUIRE(max_active_writes >= 2);
	REQUIRE(ReadFile(path) == first + second);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter waits for remote coalesce threshold before first drain", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	BlockingWriteFileSystem fs(true, false);
	auto path = TestCreatePath("async_file_writer_remote_coalesce_start.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(AsyncFileWriter::DEFAULT_REMOTE_COALESCE_THRESHOLD / 2, 'a');
	string second(AsyncFileWriter::DEFAULT_REMOTE_COALESCE_THRESHOLD / 2, 'b');

	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(fs.BlockedWrites() == 0);

	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(second));
	REQUIRE(fs.WaitForBlockedWrites(1));
	fs.ReleaseWrites();
	writer.Close();

	REQUIRE(ReadFile(path) == first + second);
	REQUIRE(fs.write_sizes.size() == 1);
	REQUIRE(fs.write_sizes[0] == AsyncFileWriter::DEFAULT_REMOTE_COALESCE_THRESHOLD);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter avoids sub-threshold remote writes when selected bytes allow coalescing",
          "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	TrackingWriteFileSystem fs(false);
	auto path = TestCreatePath("async_file_writer_remote_coalesce_small_run.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(5ULL * 1024ULL * 1024ULL, 'a');
	string second(1ULL * 1024ULL * 1024ULL, 'b');
	string third(3ULL * 1024ULL * 1024ULL, 'c');

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(second));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(third));
		batch_guard.Finish();
	}
	writer.Close();

	REQUIRE(ReadFile(path) == first + second + third);
	REQUIRE(fs.write_sizes.size() == 1);
	REQUIRE(fs.write_sizes[0] == first.size() + second.size() + third.size());
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter schedules extra remote drain tasks only after a full budget", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 4);
	BlockingWriteFileSystem fs(true, false);
	auto path = TestCreatePath("async_file_writer_remote_extra_task_threshold.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	REQUIRE(AsyncFileWriter::DEFAULT_REMOTE_COALESCE_THRESHOLD * 2 == AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET);
	string chunk(AsyncFileWriter::DEFAULT_REMOTE_COALESCE_THRESHOLD, 'x');

	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(chunk));
	REQUIRE(fs.WaitForBlockedWrites(1));

	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(chunk));
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(fs.BlockedWrites() == 1);

	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(chunk));
	REQUIRE(fs.WaitForBlockedWrites(2));

	fs.ReleaseWrites();
	writer.Close();
	REQUIRE(ReadFile(path) == chunk + chunk + chunk);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter does not eagerly schedule tiny remote tails after one large write", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 4);
	BlockingWriteFileSystem fs(true, false);
	auto path = TestCreatePath("async_file_writer_remote_large_then_tiny.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string large(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET * 2 + 1, 'x');
	string tail = "tail";

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(large));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(tail));
		batch_guard.Finish();
	}

	REQUIRE(fs.WaitForBlockedWrites(1));
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(fs.BlockedWrites() == 1);

	fs.ReleaseWrites();
	writer.Close();
	REQUIRE(ReadFile(path) == large + tail);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter local queued drain task covers newly registered tiny tail", "[async_file_writer]") {
	TestQueuedDrainTaskCoversNewTinyTail(true, "async_file_writer_local_queued_large_then_tiny.tmp");
}

TEST_CASE("AsyncFileWriter remote queued drain task covers newly registered tiny tail", "[async_file_writer]") {
	TestQueuedDrainTaskCoversNewTinyTail(false, "async_file_writer_remote_queued_large_then_tiny.tmp");
}

TEST_CASE("AsyncFileWriter does not treat sequential explicit-offset writes as positional", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	SequentialExplicitOffsetWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_sequential_explicit_offset.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'a');
	string second(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'b');

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(second));
		batch_guard.Finish();
	}

	auto saw_first_blocked_write = fs.WaitForBlockedWrites(1);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	auto max_active_writes = fs.MaxActiveWrites();
	fs.ReleaseWrites();
	writer.Close();

	REQUIRE(saw_first_blocked_write);
	REQUIRE(max_active_writes == 1);
	REQUIRE(ReadFile(path) == first + second);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter falls back to one drain task without positional writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	BlockingWriteFileSystem fs(false);
	auto path = TestCreatePath("async_file_writer_sequential_fallback.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'a');
	string second(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'b');

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(second));
		batch_guard.Finish();
	}

	auto saw_first_blocked_write = fs.WaitForBlockedWrites(1);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	auto max_active_writes = fs.MaxActiveWrites();
	fs.ReleaseWrites();
	writer.Close();

	REQUIRE(saw_first_blocked_write);
	REQUIRE(max_active_writes == 1);
	REQUIRE(ReadFile(path) == first + second);
	REQUIRE(fs.write_sizes.size() == 2);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter does not apply backpressure during a batch", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_backpressure.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("efgh"));
		REQUIRE(fs.write_sizes.empty());
		batch_guard.Finish();
	}

	writer.Close();
	REQUIRE(ReadFile(path) == "abcdefgh");
	REQUIRE(fs.write_sizes.size() == 1);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter batches writes before scheduling", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_batch.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("efgh"));
		REQUIRE(writer.GetTotalWritten() == 8);

		writer.ApplyBackpressure();
		REQUIRE(fs.write_sizes.empty());
		batch_guard.Finish();
	}

	writer.Close();
	REQUIRE(ReadFile(path) == "abcdefgh");
	REQUIRE(fs.write_sizes.size() == 1);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter close drains an open batch", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_close_batch.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path);
	auto batch_guard = writer.StartBatch();
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	writer.Close();
	batch_guard.Finish();

	REQUIRE(ReadFile(path) == "abcd");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter rethrows asynchronous write errors on close", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_error.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
	fs.fail_writes = true;

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
		batch_guard.Finish();
	}
	try {
		writer.Close();
		FAIL("Expected async write failure");
	} catch (const Exception &ex) {
		string error = ex.what();
		REQUIRE(error.find("Async write failed for range [offset=0, size=4]") != string::npos);
		REQUIRE(error.find("Injected async write failure") != string::npos);
	}

	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

TEST_CASE("AsyncFileWriter close drains scheduled tasks after async write error", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 2);
	FailingBlockedWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_error_close_drains.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'a');
	string second(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'b');

	AsyncFileWriter writer(*con->context, fs, path);
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>(second));
		batch_guard.Finish();
	}
	REQUIRE(fs.WaitForEnteredWrites(2));

	writer.WriteData(const_data_ptr_cast("x"), 1);
	fs.FailFirstWrite();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));

	std::atomic<bool> close_started(false);
	std::atomic<bool> close_finished(false);
	std::exception_ptr close_error;
	std::thread close_thread([&]() {
		close_started.store(true);
		try {
			writer.Close();
		} catch (...) {
			close_error = std::current_exception();
		}
		close_finished.store(true);
	});

	while (!close_started.load()) {
		std::this_thread::yield();
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(!close_finished.load());

	fs.ReleaseSecondWrite();
	close_thread.join();
	REQUIRE(close_finished.load());
	REQUIRE(close_error != nullptr);

	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

TEST_CASE("AsyncFileWriter close discards unscheduled writes after async write error", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db, 1);
	FailingBlockedWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_error_unscheduled_tail.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string first(AsyncFileWriter::DEFAULT_DRAIN_TASK_BYTE_BUDGET + 1, 'a');

	AsyncFileWriter writer(*con->context, fs, path);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>(first));
	REQUIRE(fs.WaitForEnteredWrites(1));
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("tail"));

	fs.FailFirstWrite();
	try {
		writer.Close();
		FAIL("Expected async write failure");
	} catch (const Exception &ex) {
		string error = ex.what();
		REQUIRE(error.find("Async write failed for range [offset=0, size=") != string::npos);
		REQUIRE(error.find("Injected async write failure") != string::npos);
	}

	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}
