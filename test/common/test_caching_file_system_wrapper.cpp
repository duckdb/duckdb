#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"
#include "test_helpers.hpp"

#include <thread>
#include <mutex>

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

	mutable std::mutex read_calls_mutex;
	vector<ReadCall> read_calls;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		std::lock_guard<std::mutex> lock(read_calls_mutex);
		read_calls.push_back({handle.GetPath(), location, UnsafeNumericCast<idx_t>(nr_bytes)});
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	void Reset() {
		std::lock_guard<std::mutex> lock(read_calls_mutex);
		read_calls.clear();
	}

	// Get read operation counts with the given operation to match.
	size_t GetReadCount(const string &path, idx_t location, idx_t size) const {
		std::lock_guard<std::mutex> lock(read_calls_mutex);
		size_t count = 0;
		for (const auto &call : read_calls) {
			if (call.path == path && call.location == location && call.size == size) {
				++count;
			}
		}
		return count;
	}
};

TEST_CASE("CachingFileSystemWrapper write operations not allowed", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

	const string test_content = "This is test content for write testing.";
	TestFileGuard test_file("test_caching_write.txt", test_content);

	// Open file for reading through caching wrapper
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	auto handle = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);

	// Test that write operations are not allowed - CachingFileSystemWrapper is read-only
	const string write_data = "Attempted write data";
	string write_buffer(100, '\0');
	memcpy(const_cast<char *>(write_buffer.data()), write_data.c_str(), write_data.size());

	// Try to write at a location, which should throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper.Write(*handle, &write_buffer[0], write_data.size(), /*location=*/0),
	                  NotImplementedException);

	// Try truncate, which should also throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper.Truncate(*handle, 0), NotImplementedException);

	// Try FileSync, which should also throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper.FileSync(*handle), NotImplementedException);

	// Try Trim, which should also throw NotImplementedException
	REQUIRE_THROWS_AS(caching_wrapper.Trim(*handle, 0, 10), NotImplementedException);

	handle.reset();

	// Test that opening file with write flags is rejected
	REQUIRE_THROWS_AS(caching_wrapper.OpenFile(test_file.GetPath(), FileFlags::FILE_FLAGS_WRITE),
	                  NotImplementedException);
	REQUIRE_THROWS_AS(
	    caching_wrapper.OpenFile(test_file.GetPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE),
	    NotImplementedException);
}

TEST_CASE("CachingFileSystemWrapper caches reads", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto tracking_fs_ptr = tracking_fs.get();
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

	const string test_content = "This is test content for caching verification. It should only be read once.";
	TestFileGuard test_file("test_caching_file.txt", test_content);

	// Test 1: Read the same content multiple times, which should only hit underlying FS once
	{
		tracking_fs_ptr->Reset();

		// Create OpenFileInfo with validation disabled to allow caching to work
		OpenFileInfo file_info(test_file.GetPath());
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

		// First read
		auto handle1 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer1(200, '\0');
		handle1->Read(QueryContext(), &buffer1[0], test_content.size(), /*location=*/0);
		handle1.reset();

		// Second read of the same location
		auto handle2 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer2(200, '\0');
		handle2->Read(QueryContext(), &buffer2[0], test_content.size(), /*location=*/0);
		handle2.reset();

		// Third read of the same location
		auto handle3 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer3(200, '\0');
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

		tracking_fs_ptr->Reset();

		// Create OpenFileInfo with validation disabled
		OpenFileInfo file_info(test_file2.GetPath());
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

		const idx_t chunk_size = 20;
		auto handle1 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer1(200, '\0');
		handle1->Read(QueryContext(), &buffer1[0], chunk_size, /*location=*/0);
		handle1.reset();

		auto handle2 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer2(200, '\0');
		handle2->Read(QueryContext(), &buffer2[0], chunk_size, chunk_size);
		handle2.reset();

		// Read first chunk again - should use cache
		auto handle3 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		string buffer3(200, '\0');
		handle3->Read(QueryContext(), &buffer3[0], chunk_size, /*location=*/0);
		handle3.reset();

		// Verify underlying FS access
		REQUIRE(tracking_fs_ptr->GetReadCount(test_file2.GetPath(), 0, chunk_size) == 1);
		REQUIRE(tracking_fs_ptr->GetReadCount(test_file2.GetPath(), chunk_size, chunk_size) == 1);
	}
}

TEST_CASE("CachingFileSystemWrapper sequential reads", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto tracking_fs_ptr = tracking_fs.get();
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

	const string test_content = "This is test content for sequential read testing.";
	TestFileGuard test_file("test_caching_sequential.txt", test_content);

	// Test sequential reads using location-based reads
	{
		tracking_fs_ptr->Reset();

		auto handle = caching_wrapper.OpenFile(test_file.GetPath(), FileFlags::FILE_FLAGS_READ);
		string buffer(200, '\0');

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
	auto tracking_fs_ptr = tracking_fs.get();
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

	const string test_content = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	TestFileGuard test_file("test_caching_seek.txt", test_content);

	// Open file for reading through caching wrapper
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	auto handle = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);

	string buffer(100, '\0');

	// Test 1: Basic seek and read
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/5, /*location=*/0);
	REQUIRE(buffer.substr(0, 5) == "01234");

	// Seek to position 10
	caching_wrapper.Seek(*handle, /*location=*/10);
	REQUIRE(caching_wrapper.SeekPosition(*handle) == 10);

	// Read from position 10
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/5, /*location=*/10);
	REQUIRE(buffer.substr(0, 5) == "ABCDE");

	// Seek to position 5
	caching_wrapper.Seek(*handle, /*location=*/5);
	REQUIRE(caching_wrapper.SeekPosition(*handle) == 5);

	// Read from position 5
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/5, /*location=*/5);
	REQUIRE(buffer.substr(0, 5) == "56789");

	// Test 2: Reset.
	caching_wrapper.Reset(*handle);
	REQUIRE(caching_wrapper.SeekPosition(*handle) == 0);

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
		caching_wrapper.Seek(*handle, test.seek_pos);
		REQUIRE(caching_wrapper.SeekPosition(*handle) == test.seek_pos);
		handle->Read(QueryContext(), &buffer[0], test.read_size, test.seek_pos);
		REQUIRE(buffer.substr(0, test.read_size) == test.expected);
	}

	// Test 4: Read after seek
	caching_wrapper.Seek(*handle, 30);
	REQUIRE(caching_wrapper.SeekPosition(*handle) == 30);
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/6, /*location=*/30);
	REQUIRE(buffer.substr(0, 6) == "UVWXYZ");

	// Test 5: Seek back and verify position
	caching_wrapper.Seek(*handle, 12);
	REQUIRE(caching_wrapper.SeekPosition(*handle) == 12);
	handle->Read(QueryContext(), &buffer[0], /*nr_bytes=*/3, /*location=*/12);
	REQUIRE(buffer.substr(0, 3) == "CDE");

	handle.reset();
}

TEST_CASE("CachingFileSystemWrapper list operations", "[file_system][caching]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

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
	caching_wrapper.ListFiles(test_dir, [&actual_files](const string &path, bool is_dir) {
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
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

	const string test_content = "Test content for parallel read access.";
	TestFileGuard test_file("test_caching_parallel.txt", test_content);
	constexpr idx_t THREAD_COUNT = 2;

	// Open file with parallel access flag
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	// Use two threads to read from the file in parallel.
	vector<std::thread> threads;
	vector<bool> results(THREAD_COUNT, false);

	for (size_t idx = 0; idx < THREAD_COUNT; ++idx) {
		threads.emplace_back([&, idx]() {
			auto handle =
			    caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_PARALLEL_ACCESS);
			string buffer(200, '\0');
			handle->Read(QueryContext(), &buffer[0], test_content.size(), /*location=*/0);
			results[idx] = (buffer.substr(0, test_content.size()) == test_content);
			handle.reset();
		});
	}
	for (auto &thd : threads) {
		REQUIRE(thd.joinable());
		thd.join();
	}

	// Verify both threads read correctly
	REQUIRE(results[0]);
	REQUIRE(results[1]);
}

} // namespace duckdb
