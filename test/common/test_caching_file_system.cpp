#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "test_helpers.hpp"

#include <thread>

namespace duckdb {

// RAII wrapper for test file creation and cleanup
class TestFileGuard {
public:
	TestFileGuard(const string &filename, const string &content) : file_path(TestCreatePath(filename)) {
		auto local_fs = FileSystem::CreateLocal();
		auto handle = local_fs->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(QueryContext(), const_cast<char *>(content.data()), content.size(), 0);
		handle->Sync();
	}

	~TestFileGuard() {
		auto local_fs = FileSystem::CreateLocal();
		local_fs->TryRemoveFile(file_path);
	}

	const string &GetPath() const {
		return file_path;
	}

private:
	string file_path;
};

// A test filesystem that tracks read operations in the order of invocation.
class TrackingFileSystem : public LocalFileSystem {
public:
	struct ReadCall {
		string path;
		idx_t location;
		idx_t size;
	};

	mutable mutex read_calls_mutex;
	vector<ReadCall> read_calls;

	string GetName() const override {
		return "TrackingFileSystem";
	}
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		const lock_guard<mutex> lock(read_calls_mutex);
		read_calls.push_back({handle.GetPath(), location, UnsafeNumericCast<idx_t>(nr_bytes)});
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	void Clear() {
		const lock_guard<mutex> lock(read_calls_mutex);
		read_calls.clear();
	}

	size_t GetReadCount(const string &path, idx_t location, idx_t size) const {
		const lock_guard<mutex> lock(read_calls_mutex);
		size_t count = 0;
		for (const auto &call : read_calls) {
			if (call.path == path && call.location == location && call.size == size) {
				++count;
			}
		}
		return count;
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, TestDirectoryPath());
	}

	bool CanSeek() override {
		return true;
	}
};

// A file system that blocks Read() calls until a barrier is signaled,
// allowing controlled interleaving of concurrent reads.
class BarrierFileSystem : public LocalFileSystem {
public:
	mutable mutex mu;
	bool block_reads DUCKDB_GUARDED_BY(mu) = false;
	std::condition_variable cv;

	string GetName() const override {
		return "BarrierFileSystem";
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		{
			unique_lock<mutex> lock(mu);
			cv.wait(lock, [&]() DUCKDB_REQUIRES(mu) { return !block_reads; });
		}
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	void BlockReads() {
		lock_guard<mutex> lock(mu);
		block_reads = true;
	}

	void UnblockReads() {
		lock_guard<mutex> lock(mu);
		block_reads = false;
		cv.notify_all();
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, TestDirectoryPath());
	}

	bool CanSeek() override {
		return true;
	}
};

// A file system that counts OpenFile calls to verify when the underlying file is (not) opened.
class CountingFileSystem : public LocalFileSystem {
public:
	mutable mutex mu;
	size_t open_count DUCKDB_GUARDED_BY(mu) = 0;

	string GetName() const override {
		return "CountingFileSystem";
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		const lock_guard<mutex> lock(mu);
		open_count++;
		return LocalFileSystem::OpenFile(path, flags, opener);
	}

	size_t GetOpenCount() const {
		const lock_guard<mutex> lock(mu);
		return open_count;
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, TestDirectoryPath());
	}

	bool CanSeek() override {
		return true;
	}
};

TEST_CASE("CachingFileHandle Read returns correct FileBufferHandleGroup", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();

	const idx_t BLOCK_SIZE = ExternalFileCache::LOCAL_FILE_CACHE_BLOCK_SIZE;
	const idx_t EXTRA = 100;
	const idx_t FILE_SIZE = BLOCK_SIZE + EXTRA;

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	TestFileGuard test_file("test_multi_block.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);

	// Full file read: should produce 2 handles
	{
		auto group = handle->Read(FILE_SIZE, 0);
		auto &handles = group.GetHandles();
		REQUIRE(handles.size() == 2);
		REQUIRE(handles[0].start_offset == 0);
		REQUIRE(handles[0].length == BLOCK_SIZE);
		REQUIRE(handles[1].start_offset == 0);
		REQUIRE(handles[1].length == EXTRA);

		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
	}

	// Cross-boundary read: last 200 bytes of block 0 + first 50 bytes of block 1
	{
		const idx_t read_offset = BLOCK_SIZE - 200;
		const idx_t read_size = 250;
		auto group = handle->Read(read_size, read_offset);
		auto &handles = group.GetHandles();
		REQUIRE(handles.size() == 2);
		REQUIRE(handles[0].start_offset == BLOCK_SIZE - 200);
		REQUIRE(handles[0].length == 200);
		REQUIRE(handles[1].start_offset == 0);
		REQUIRE(handles[1].length == 50);

		string result(read_size, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), read_size);
		REQUIRE(result == content.substr(read_offset, read_size));
	}

	// Single block read: should produce 1 handle
	{
		auto group = handle->Read(100, 0);
		auto &handles = group.GetHandles();
		REQUIRE(handles.size() == 1);
		REQUIRE(handles[0].start_offset == 0);
		REQUIRE(handles[0].length == 100);
	}
}

TEST_CASE("FetchBlockTask does not cache stale data after invalidation", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto barrier_fs = make_uniq<BarrierFileSystem>();
	auto *barrier_fs_ptr = barrier_fs.get();
	auto normal_fs = make_uniq<TrackingFileSystem>();

	const idx_t BLOCK_SIZE = ExternalFileCache::LOCAL_FILE_CACHE_BLOCK_SIZE;
	const idx_t FILE_SIZE = BLOCK_SIZE;

	string content_v1(FILE_SIZE, 'A');
	TestFileGuard test_file("test_invalidation_race.bin", content_v1);

	// Thread A uses barrier_fs, which will be blocked during read.
	CachingFileSystem cfs_slow(*barrier_fs, db_instance);
	// Thread B uses normal_fs, which read immediately.
	CachingFileSystem cfs_fast(*normal_fs, db_instance);

	auto make_file_info = [&](bool validate) {
		OpenFileInfo info(test_file.GetPath());
		info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(validate);
		return info;
	};

	// Step 1: Thread A opens file at v1 and starts reading
	barrier_fs_ptr->BlockReads();
	auto file_info_a = make_file_info(true);
	auto handle_a = cfs_slow.OpenFile(file_info_a, FileFlags::FILE_FLAGS_READ);

	std::thread thread_a([&]() {
		// This will block in the barrier FS until unblocked
		handle_a->Read(FILE_SIZE, 0);
	});

	// Step 2: Overwrite file with v2 content (all 'B's) and wait for mtime to change
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	{
		auto local_fs = FileSystem::CreateLocal();
		auto write_handle =
		    local_fs->OpenFile(test_file.GetPath(), FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		string content_v2(FILE_SIZE, 'B');
		write_handle->Write(QueryContext(), const_cast<char *>(content_v2.data()), content_v2.size(), 0);
		write_handle->Sync();
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	// Step 3: Thread B opens the same file (with validation), triggering invalidation and caching v2
	auto file_info_b = make_file_info(true);
	auto handle_b = cfs_fast.OpenFile(file_info_b, FileFlags::FILE_FLAGS_READ);
	auto group_b = handle_b->Read(FILE_SIZE, 0);

	// Verify Thread B got v2 data
	string result_b(FILE_SIZE, '\0');
	group_b.CopyTo(reinterpret_cast<data_ptr_t>(&result_b[0]), FILE_SIZE);
	REQUIRE(result_b == string(FILE_SIZE, 'B'));

	// Step 4: Unblock Thread A — its FetchBlockTask should detect the version mismatch
	barrier_fs_ptr->UnblockReads();
	thread_a.join();

	// Step 5: Verify the cache still contains v2 data
	auto file_info_check = make_file_info(true);
	auto handle_check = cfs_fast.OpenFile(file_info_check, FileFlags::FILE_FLAGS_READ);
	auto group_check = handle_check->Read(FILE_SIZE, 0);
	string result_check(FILE_SIZE, '\0');
	group_check.CopyTo(reinterpret_cast<data_ptr_t>(&result_check[0]), FILE_SIZE);
	REQUIRE(result_check == string(FILE_SIZE, 'B'));
}

TEST_CASE("Fully cached read skips GetFileHandle", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto counting_fs = make_uniq<CountingFileSystem>();
	auto *counting_fs_ptr = counting_fs.get();

	const idx_t BLOCK_SIZE = ExternalFileCache::LOCAL_FILE_CACHE_BLOCK_SIZE;
	const idx_t FILE_SIZE = BLOCK_SIZE;

	string content(FILE_SIZE, 'X');
	TestFileGuard test_file("test_skip_open.bin", content);

	CachingFileSystem cfs(*counting_fs, db_instance);

	auto make_file_info = [&]() {
		OpenFileInfo info(test_file.GetPath());
		info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		return info;
	};

	// First read: populates the cache (requires opening the file)
	{
		auto handle = cfs.OpenFile(make_file_info(), FileFlags::FILE_FLAGS_READ);
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
		REQUIRE(counting_fs_ptr->GetOpenCount() >= 1);
	}

	// Second read: all blocks are cached, validation is off, so we should not open the underlying file
	{
		auto handle = cfs.OpenFile(make_file_info(), FileFlags::FILE_FLAGS_READ);
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
		REQUIRE(counting_fs_ptr->GetOpenCount() == 1);
	}
}

} // namespace duckdb
