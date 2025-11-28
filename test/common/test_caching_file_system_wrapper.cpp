#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"
#include "test_helpers.hpp"

using namespace std;

namespace duckdb {

// A tracking filesystem that counts read operations
class TrackingFileSystem : public LocalFileSystem {
public:
	struct ReadCall {
		string path;
		idx_t location;
		idx_t size;
	};

	vector<ReadCall> read_calls;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		read_calls.push_back({handle.GetPath(), location, UnsafeNumericCast<idx_t>(nr_bytes)});
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		// For non-seeking reads, we track with location = -1 to indicate sequential read
		read_calls.push_back({handle.GetPath(), idx_t(-1), UnsafeNumericCast<idx_t>(nr_bytes)});
		return LocalFileSystem::Read(handle, buffer, nr_bytes);
	}

	void Reset() {
		read_calls.clear();
	}

	size_t GetReadCount(const string &path, idx_t location, idx_t size) const {
		size_t count = 0;
		for (const auto &call : read_calls) {
			if (call.path == path && call.location == location && call.size == size) {
				count++;
			}
		}
		return count;
	}
};

TEST_CASE("CachingFileSystemWrapper caches reads", "[file_system][caching]") {
	// Create an in-memory database instance
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;

	// Create a tracking filesystem
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto tracking_fs_ptr = tracking_fs.get();

	// Create the caching wrapper
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

	// Create a test file
	auto test_file = TestCreatePath("test_caching_file.txt");
	const string test_content = "This is test content for caching verification. It should only be read once.";

	// Write test content to file using local filesystem
	{
		auto local_fs = FileSystem::CreateLocal();
		auto handle = local_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(QueryContext(), (void *)test_content.c_str(), test_content.size(), 0);
		handle->Sync();
		handle.reset();
	}

	// Test 1: Read the same content multiple times - should only hit underlying FS once
	{
		tracking_fs_ptr->Reset();

		// Create OpenFileInfo with validation disabled to allow caching to work
		OpenFileInfo file_info(test_file);
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

		// First read
		auto handle1 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		char buffer1[200];
		handle1->Read(QueryContext(), buffer1, test_content.size(), 0);
		handle1.reset();

		// Second read of the same location
		auto handle2 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		char buffer2[200];
		handle2->Read(QueryContext(), buffer2, test_content.size(), 0);
		handle2.reset();

		// Third read of the same location
		auto handle3 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		char buffer3[200];
		handle3->Read(QueryContext(), buffer3, test_content.size(), 0);
		handle3.reset();

		// Verify content is correct
		REQUIRE(string(buffer1, test_content.size()) == test_content);
		REQUIRE(string(buffer2, test_content.size()) == test_content);
		REQUIRE(string(buffer3, test_content.size()) == test_content);

		// Verify the underlying filesystem was only called once for this read
		auto read_count = tracking_fs_ptr->GetReadCount(test_file, 0, test_content.size());
		REQUIRE(read_count == 1);
	}

	// Test 2: Read different locations - each should hit underlying FS once
	{
		// Use a different file for this test to avoid interference from Test 1's cache
		auto test_file2 = TestCreatePath("test_caching_file2.txt");
		const string test_content2 = "This is test content for chunked read testing. It has enough content.";

		// Write test content to file
		{
			auto local_fs = FileSystem::CreateLocal();
			auto handle = local_fs->OpenFile(test_file2, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
			handle->Write(QueryContext(), (void *)test_content2.c_str(), test_content2.size(), 0);
			handle->Sync();
			handle.reset();
		}

		tracking_fs_ptr->Reset();

		// Create OpenFileInfo with validation disabled
		OpenFileInfo file_info(test_file2);
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

		const idx_t chunk_size = 20;
		auto handle1 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		char buffer1[200];
		handle1->Read(QueryContext(), buffer1, chunk_size, 0);
		handle1.reset();

		auto handle2 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		char buffer2[200];
		handle2->Read(QueryContext(), buffer2, chunk_size, chunk_size);
		handle2.reset();

		// Read first chunk again - should use cache
		auto handle3 = caching_wrapper.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
		char buffer3[200];
		handle3->Read(QueryContext(), buffer3, chunk_size, 0);
		handle3.reset();

		// Verify first chunk was read once from underlying FS, second chunk once
		// Third read should use cache (no additional read)
		REQUIRE(tracking_fs_ptr->GetReadCount(test_file2, 0, chunk_size) == 1);
		REQUIRE(tracking_fs_ptr->GetReadCount(test_file2, chunk_size, chunk_size) == 1);

		// Cleanup
		{
			auto local_fs = FileSystem::CreateLocal();
			local_fs->RemoveFile(test_file2);
		}
	}

	// Cleanup
	{
		auto local_fs = FileSystem::CreateLocal();
		local_fs->RemoveFile(test_file);
	}
}

TEST_CASE("CachingFileSystemWrapper sequential reads", "[file_system][caching]") {
	// Create an in-memory database instance
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;

	// Create a tracking filesystem
	auto tracking_fs = make_uniq<TrackingFileSystem>();
	auto tracking_fs_ptr = tracking_fs.get();

	// Create the caching wrapper
	CachingFileSystemWrapper caching_wrapper(*tracking_fs, db_instance);

	// Create a test file
	auto test_file = TestCreatePath("test_caching_sequential.txt");
	const string test_content = "This is test content for sequential read testing.";

	// Write test content to file
	{
		auto local_fs = FileSystem::CreateLocal();
		auto handle = local_fs->OpenFile(test_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(QueryContext(), (void *)test_content.c_str(), test_content.size(), 0);
		handle->Sync();
		handle.reset();
	}

	// Test sequential reads
	{
		tracking_fs_ptr->Reset();

		auto handle = caching_wrapper.OpenFile(test_file, FileFlags::FILE_FLAGS_READ);
		char buffer[200];

		// First sequential read
		auto bytes_read1 = handle->Read(QueryContext(), buffer, 10);
		REQUIRE(bytes_read1 == 10);

		// Second sequential read
		auto bytes_read2 = handle->Read(QueryContext(), buffer + 10, 10);
		REQUIRE(bytes_read2 == 10);

		// Verify content
		REQUIRE(string(buffer, 20) == test_content.substr(0, 20));

		handle.reset();
	}

	// Cleanup
	{
		auto local_fs = FileSystem::CreateLocal();
		local_fs->RemoveFile(test_file);
	}
}

} // namespace duckdb

