#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"
#include "test_helpers.hpp"

#include <thread>

namespace duckdb {

constexpr idx_t TEST_BUFFER_SIZE = 200;

class FailingFileSystem : public LocalFileSystem {
public:
	mutable annotated_mutex mu;
	bool should_fail DUCKDB_GUARDED_BY(mu) = false;

	string GetName() const override {
		return "FailingFileSystem";
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, duckdb::idx_t location) override {
		const annotated_lock_guard<duckdb::annotated_mutex> lock(mu);
		if (should_fail) {
			throw IOException("Injected read failure");
		}
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	void SetShouldFail(bool value) DUCKDB_EXCLUDES(mu) {
		const annotated_lock_guard<duckdb::annotated_mutex> lock(mu);
		should_fail = value;
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, TestDirectoryPath());
	}

	bool CanSeek() override {
		return true;
	}
};

// A file system that blocks [Read] calls until a barrier is signaled, allowing controlled interleaving of concurrent
// reads.
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

	// Clear all read invocations track.
	void Clear() {
		const lock_guard<mutex> lock(read_calls_mutex);
		read_calls.clear();
	}

	// Get read operation counts with the given operation to match.
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

	// Tracking filesystem can only deal files in the testing directory.
	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, TestDirectoryPath());
	}

	// Tracking filesystem is a derived class of local filesystem and could seek.
	bool CanSeek() override {
		return true;
	}
};

TEST_CASE("CachingFileSystemWrapper write operations not allowed", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content = "This is test content for write testing.";
	TestFileGuard test_file("test_caching_write.txt", test_content);

	// Open file for reading through caching wrapper
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	auto handle = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);

	// Test that write operations are not allowed - CachingFileSystemWrapper is read-only
	const string write_data = "Attempted write data";
	string write_buffer(100, '\0');
	memcpy(const_cast<char *>(write_buffer.data()), write_data.c_str(), write_data.size());

	// Try to write at a location, which should throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper->Write(*handle, &write_buffer[0], write_data.size(), /*location=*/0),
	                  NotImplementedException);

	// Try truncate, which should also throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper->Truncate(*handle, 0), NotImplementedException);

	// Try FileSync, which should also throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper->FileSync(*handle), NotImplementedException);

	// Try Trim, which should also throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper->Trim(*handle, 0, 10), NotImplementedException);

	handle.reset();

	// Test that opening file with write flags is rejected
	REQUIRE_THROWS_AS(caching_wrapper->OpenFile(test_file.GetPath(), FileFlags::FILE_FLAGS_WRITE),
	                  NotImplementedException);
	REQUIRE_THROWS_AS(
	    caching_wrapper->OpenFile(test_file.GetPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE),
	    NotImplementedException);
}

TEST_CASE("CachingFileSystemWrapper caches reads", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto tracking_fs_ptr = tracking_fs.get();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content = "This is test content for caching verification. It should only be read once.";
	TestFileGuard test_file("test_caching_file.txt", test_content);

	// Test 1: Read the same content multiple times, which should only hit underlying FS once
	{
		tracking_fs_ptr->Clear();

		// Create OpenFileInfo with validation disabled to allow caching to work
		OpenFileInfo file_info(test_file.GetPath());
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

		// First read
		auto handle1 = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer1(TEST_BUFFER_SIZE, '\0');
		handle1->Read(QueryContext(), &buffer1[0], test_content.size(), /*location=*/0);
		handle1.reset();

		// Second read of the same location
		auto handle2 = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer2(TEST_BUFFER_SIZE, '\0');
		handle2->Read(QueryContext(), &buffer2[0], test_content.size(), /*location=*/0);
		handle2.reset();

		// Third read of the same location
		auto handle3 = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer3(TEST_BUFFER_SIZE, '\0');
		handle3->Read(QueryContext(), &buffer3[0], test_content.size(), /*location=*/0);
		handle3.reset();

		// Verify content is correct
		REQUIRE(buffer1.substr(0, test_content.size()) == test_content);
		REQUIRE(buffer2.substr(0, test_content.size()) == test_content);
		REQUIRE(buffer3.substr(0, test_content.size()) == test_content);

		// Verify the underlying filesystem was only called once for this read
		auto read_count = tracking_fs_ptr->GetReadCount(test_file.GetPath(), /*location=*/0, test_content.size());
		REQUIRE(read_count == 1);
	}

	// Test 2: Read different locations, with each request hitting underlying FS once
	{
		// Use a different file for this test to avoid interference from Test 1's cache
		const string test_content2 = "This is test content for chunked read testing. It has enough content.";
		TestFileGuard test_file2("test_caching_file2.txt", test_content2);

		tracking_fs_ptr->Clear();

		// Create OpenFileInfo with validation disabled
		OpenFileInfo file_info(test_file2.GetPath());
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

		const idx_t chunk_size = 20;
		auto handle1 = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer1(TEST_BUFFER_SIZE, '\0');
		handle1->Read(QueryContext(), &buffer1[0], chunk_size, /*location=*/0);
		handle1.reset();

		auto handle2 = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer2(TEST_BUFFER_SIZE, '\0');
		handle2->Read(QueryContext(), &buffer2[0], chunk_size, chunk_size);
		handle2.reset();

		// Read first chunk again - should use cache
		auto handle3 = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer3(TEST_BUFFER_SIZE, '\0');
		handle3->Read(QueryContext(), &buffer3[0], chunk_size, /*location=*/0);
		handle3.reset();

		// With block-aligned caching, both 20-byte reads fall within block 0.
		// Only one underlying read of the full file at offset 0.
		REQUIRE(tracking_fs_ptr->GetReadCount(test_file2.GetPath(), 0, test_content2.size()) == 1);
	}
}

TEST_CASE("CachingFileSystemWrapper sequential reads", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto tracking_fs_ptr = tracking_fs.get();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content = "This is test content for sequential read testing.";
	TestFileGuard test_file("test_caching_sequential.txt", test_content);

	// Test sequential reads using location-based reads
	{
		tracking_fs_ptr->Clear();

		auto handle = caching_wrapper->OpenFile(test_file.GetPath(), FileFlags::FILE_FLAGS_READ);
		string buffer(TEST_BUFFER_SIZE, '\0');

		// First read from position 0
		handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/10, /*location=*/0);

		// Second read from position 10
		handle->Read(QueryContext(), &buffer[10], /*nr_bytes=*/10, /*location=*/10);

		// Verify content
		REQUIRE(buffer.substr(0, 20) == test_content.substr(0, 20));

		handle.reset();
	}
}

TEST_CASE("CachingFileSystemWrapper seek operations", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	TestFileGuard test_file("test_caching_seek.txt", test_content);

	// Open file for reading through caching wrapper
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	auto handle = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);

	string buffer(100, '\0');

	// Test 1: Basic seek and read
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/5, /*location=*/0);
	REQUIRE(buffer.substr(0, 5) == "01234");

	// Seek to position 10
	caching_wrapper->Seek(*handle, /*location=*/10);
	REQUIRE(caching_wrapper->SeekPosition(*handle) == 10);

	// Read from position 10
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/5, /*location=*/10);
	REQUIRE(buffer.substr(0, 5) == "ABCDE");

	// Seek to position 5
	caching_wrapper->Seek(*handle, /*location=*/5);
	REQUIRE(caching_wrapper->SeekPosition(*handle) == 5);

	// Read from position 5
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/5, /*location=*/5);
	REQUIRE(buffer.substr(0, 5) == "56789");

	// Test 2: Reset.
	caching_wrapper->Reset(*handle);
	REQUIRE(caching_wrapper->SeekPosition(*handle) == 0);

	// Read from beginning again
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/5, /*location=*/0);
	REQUIRE(buffer.substr(0, 5) == "01234");

	// Test 3: Multiple seek + get offset + read operations
	struct SeekReadTest {
		idx_t seek_pos;
		idx_t read_size;
		string expected;
	};

	vector<SeekReadTest> tests = {
	    {0, 3, "012"}, {5, 4, "5678"}, {10, 5, "ABCDE"}, {15, 4, "FGHI"}, {20, 3, "KLM"}, {25, 4, "PQRS"},
	};

	for (const auto &test : tests) {
		caching_wrapper->Seek(*handle, test.seek_pos);
		REQUIRE(caching_wrapper->SeekPosition(*handle) == test.seek_pos);
		handle->Read(QueryContext(), &buffer[0], test.read_size, test.seek_pos);
		REQUIRE(buffer.substr(0, test.read_size) == test.expected);
	}

	// Test 4: Read after seek
	caching_wrapper->Seek(*handle, 30);
	REQUIRE(caching_wrapper->SeekPosition(*handle) == 30);
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/6, /*location=*/30);
	REQUIRE(buffer.substr(0, 6) == "UVWXYZ");

	// Test 5: Seek back and verify position
	caching_wrapper->Seek(*handle, 12);
	REQUIRE(caching_wrapper->SeekPosition(*handle) == 12);
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/3, /*location=*/12);
	REQUIRE(buffer.substr(0, 3) == "CDE");

	handle.reset();
}

TEST_CASE("CachingFileSystemWrapper list operations", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	// Create a test directory
	auto test_dir = TestCreatePath("test_list_dir");
	auto local_fs = FileSystem::CreateLocal();
	local_fs->CreateDirectory(test_dir);

	// Create several test files in the directory.
	vector<string> expected_files = {"file1.txt", "file2.txt", "file3.txt", "file4.txt"};
	vector<string> file_paths;
	for (const auto &filename : expected_files) {
		auto file_path = local_fs->JoinPath(test_dir, filename);
		file_paths.push_back(file_path);
		auto handle = local_fs->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		const string content = "Test content for " + filename;
		handle->Write(QueryContext(), const_cast<char *>(content.data()), content.size(), /*location=*/0);
		handle->Sync();
		handle.reset();
	}

	// List files using the caching wrapper
	vector<string> actual_files;
	caching_wrapper->ListFiles(test_dir, [&actual_files](const string &path, bool is_dir) {
		if (!is_dir) {
			actual_files.emplace_back(path);
		}
	});

	std::sort(expected_files.begin(), expected_files.end());
	std::sort(actual_files.begin(), actual_files.end());
	REQUIRE(actual_files.size() == expected_files.size());
	for (size_t idx = 0; idx < expected_files.size(); ++idx) {
		REQUIRE(actual_files[idx] == expected_files[idx]);
	}

	for (const auto &file_path : file_paths) {
		local_fs->TryRemoveFile(file_path);
	}
	local_fs->RemoveDirectory(test_dir);
}

TEST_CASE("CachingFileSystemWrapper read with parallel accesses", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content =
	    "Test content for parallel read access. This is a longer string to allow multiple reads."
	    " Adding more content so that eight threads can each read twenty bytes without going past the end of the file.";
	TestFileGuard test_file("test_caching_parallel.txt", test_content);
	constexpr idx_t THREAD_COUNT = 8;

	// Open file with parallel access flag - single handle shared by all threads
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	auto shared_handle =
	    caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_PARALLEL_ACCESS);

	// Use multiple threads to read from the same file handle in parallel using pread semantics
	vector<std::thread> threads;
	mutex results_mutex;
	vector<bool> results(THREAD_COUNT, false);

	const idx_t chunk_size = 20;
	for (size_t idx = 0; idx < THREAD_COUNT; ++idx) {
		threads.emplace_back([&, idx]() {
			const idx_t read_location = idx * chunk_size;
			string buffer(TEST_BUFFER_SIZE, '\0');
			shared_handle->Read(QueryContext(), &buffer[0], chunk_size, read_location);
			const bool ok = (buffer.substr(0, chunk_size) == test_content.substr(read_location, chunk_size));
			{
				const lock_guard<mutex> lock(results_mutex);
				results[idx] = ok;
			}
		});
	}
	for (auto &thd : threads) {
		thd.join();
	}

	for (size_t idx = 0; idx < THREAD_COUNT; ++idx) {
		REQUIRE(results[idx]);
	}
}

// Testing scenario: mimic open file with duckdb instance, which open a file goes through opener filesystem, meanwhile
// with caching enabled.
//
// Example usage in production:
// auto &fs = FileSystem::GetFileSystem(context);
// auto file_handle = fs.OpenFile(path, flag);
TEST_CASE("Open file in opener filesystem cache modes", "[file_system][caching]") {
	const string test_content = "File used for caching enabled testing";
	TestFileGuard test_file("test_caching_parallel.txt", test_content);

	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &opener_filesystem = db_instance.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	vfs.RegisterSubSystem(make_uniq<TrackingFileSystem>());

	// Shared variable both all caching modes.
	string buffer(TEST_BUFFER_SIZE, '\0');
	const auto &external_file_cache = db_instance.GetExternalFileCache();

	auto run_case = [&](CachingMode mode) {
		FileOpenFlags flags {FileFlags::FILE_FLAGS_READ};
		flags.SetCachingMode(mode);

		// Perform read operation and check correctness.
		auto handle = opener_filesystem.OpenFile(test_file.GetPath(), flags);
		handle->Read(QueryContext(), &buffer[0], test_content.length(), /*location=*/0);
		REQUIRE(buffer.substr(0, test_content.length()) == test_content);

		// Check seeability.
		REQUIRE(handle->CanSeek());
	};

	SECTION("cache enabled") {
		run_case(CachingMode::ALWAYS_CACHE);
		// Check external cache file has something cached.
		REQUIRE(!external_file_cache.GetCachedFileInformation().empty());
	}

	SECTION("cache disabled") {
		run_case(CachingMode::NO_CACHING);
		// Check external cache file has nothing cached.
		REQUIRE(external_file_cache.GetCachedFileInformation().empty());
	}
}

// Testing scenario: read end offset exceeds file size, caching filesystem is expected to truncate.
TEST_CASE("Request over-sized range read", "[file_system][caching]") {
	const string test_content = "File used for over-sized read testing";
	TestFileGuard test_file("test_oversized_read.txt", test_content);

	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &opener_filesystem = db_instance.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	vfs.RegisterSubSystem(make_uniq<TrackingFileSystem>());

	FileOpenFlags flags {FileFlags::FILE_FLAGS_READ};
	flags.SetCachingMode(CachingMode::ALWAYS_CACHE);

	// Perform read operation and check correctness.
	auto handle = opener_filesystem.OpenFile(test_file.GetPath(), flags);
	string buffer(TEST_BUFFER_SIZE, '\0');
	const idx_t actual_read = handle->Read(QueryContext(), &buffer[0], test_content.length() + 1);
	REQUIRE(actual_read == test_content.length());
	REQUIRE(buffer.substr(0, test_content.length()) == test_content);
}

TEST_CASE("CachingFileSystemWrapper concurrent reads same block", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto tracking_fs_ptr = tracking_fs.get();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content =
	    "Test content for concurrent same-block access. All threads read the same region of this file.";
	TestFileGuard test_file("test_caching_concurrent_block.txt", test_content);
	constexpr idx_t THREAD_COUNT = 8;

	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	tracking_fs_ptr->Clear();

	mutex results_mutex;
	vector<bool> results(THREAD_COUNT, false);
	vector<std::thread> threads;

	for (size_t idx = 0; idx < THREAD_COUNT; ++idx) {
		threads.emplace_back([&, idx]() {
			auto handle = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
			string buffer(TEST_BUFFER_SIZE, '\0');
			handle->Read(QueryContext(), &buffer[0], test_content.size(), /*location=*/0);
			bool ok = (buffer.substr(0, test_content.size()) == test_content);
			{
				const lock_guard<mutex> lock(results_mutex);
				results[idx] = ok;
			}
		});
	}
	for (auto &thd : threads) {
		thd.join();
	}

	for (size_t idx = 0; idx < THREAD_COUNT; ++idx) {
		REQUIRE(results[idx]);
	}
	REQUIRE(tracking_fs_ptr->GetReadCount(test_file.GetPath(), 0, test_content.size()) == 1);
}

TEST_CASE("CachingFileSystemWrapper IO error propagates to waiters", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto failing_fs = make_uniq<FailingFileSystem>();
	auto failing_fs_ptr = failing_fs.get();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*failing_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content = "Test content for IO error propagation testing across multiple threads.";
	TestFileGuard test_file("test_caching_io_error.txt", test_content);

	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	failing_fs_ptr->SetShouldFail(true);

	constexpr idx_t THREAD_COUNT = 4;
	mutex results_mutex;
	vector<bool> got_error(THREAD_COUNT, false);
	vector<std::thread> threads;

	for (size_t idx = 0; idx < THREAD_COUNT; ++idx) {
		threads.emplace_back([&, idx]() {
			try {
				auto handle = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
				string buffer(TEST_BUFFER_SIZE, '\0');
				handle->Read(QueryContext(), &buffer[0], test_content.size(), /*location=*/0);
			} catch (...) {
				const lock_guard<mutex> lock(results_mutex);
				got_error[idx] = true;
			}
		});
	}
	for (auto &thd : threads) {
		thd.join();
	}

	for (size_t idx = 0; idx < THREAD_COUNT; ++idx) {
		REQUIRE(got_error[idx]);
	}
}

TEST_CASE("CachingFileSystemWrapper transient IO error recovery", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto failing_fs = make_uniq<FailingFileSystem>();
	auto failing_fs_ptr = failing_fs.get();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*failing_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content = "Transient error recovery test content for caching file system.";
	TestFileGuard test_file("test_caching_transient.txt", test_content);

	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	// First attempt: fail
	failing_fs_ptr->SetShouldFail(true);
	{
		auto handle = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer(TEST_BUFFER_SIZE, '\0');
		REQUIRE_THROWS(handle->Read(QueryContext(), &buffer[0], test_content.size(), /*location=*/0));
	}

	// Second attempt: succeed after clearing the failure
	failing_fs_ptr->SetShouldFail(false);
	{
		auto handle = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer(TEST_BUFFER_SIZE, '\0');
		REQUIRE_NOTHROW(handle->Read(QueryContext(), &buffer[0], test_content.size(), /*location=*/0));
		REQUIRE(buffer.substr(0, test_content.size()) == test_content);
	}
}

TEST_CASE("CachingFileSystemWrapper zero-byte read", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto caching_wrapper =
	    make_shared_ptr<CachingFileSystemWrapper>(*tracking_fs, db_instance, CachingMode::ALWAYS_CACHE);

	const string test_content = "Some content for zero-byte read testing.";
	TestFileGuard test_file("test_caching_zero_read.txt", test_content);

	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = caching_wrapper->OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	string buffer(TEST_BUFFER_SIZE, '\0');

	REQUIRE_NOTHROW(handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/0, /*location=*/0));
	REQUIRE_NOTHROW(handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/0, /*location=*/10));
}

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

} // namespace duckdb
