#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "test_helpers.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Test Utilities
//===----------------------------------------------------------------------===//

class EFCTestFileGuard {
public:
	EFCTestFileGuard(const string &filename, const string &content) : file_path(TestCreatePath(filename)) {
		auto local_fs = FileSystem::CreateLocal();
		auto handle = local_fs->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(QueryContext(), const_cast<char *>(content.data()), content.size(), 0);
		handle->Sync();
	}

	~EFCTestFileGuard() {
		auto local_fs = FileSystem::CreateLocal();
		local_fs->TryRemoveFile(file_path);
	}

	const string &GetPath() const {
		return file_path;
	}

private:
	string file_path;
};

class EFCTrackingFileSystem : public LocalFileSystem {
public:
	string GetName() const override {
		return "TrackingFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, TestDirectoryPath());
	}

	bool CanSeek() override {
		return true;
	}

	string GetVersionTag(FileHandle &handle) override {
		return StringUtil::Format("%lld:%lld", GetFileSize(handle), GetLastModifiedTime(handle).value);
	}
};

//===----------------------------------------------------------------------===//
// ReindexCachedFiles Tests
//===----------------------------------------------------------------------===//

TEST_CASE("ReindexCachedFiles split large blocks into smaller blocks", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 16384; // 16 KiB
	const idx_t NEW_BLOCK_SIZE = 4096;  // 4 KiB
	const idx_t FILE_SIZE = OLD_BLOCK_SIZE * 3 + 100;

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	EFCTestFileGuard test_file("test_reindex_split.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	// Read full file to populate cache at OLD_BLOCK_SIZE.
	{
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
	}

	auto cached_before = cache.GetCachedFileInformation();
	idx_t total_bytes_before = 0;
	for (auto &info : cached_before) {
		total_bytes_before += info.nr_bytes;
	}
	REQUIRE(cached_before.size() == 4); // 3 full blocks + 1 partial
	REQUIRE(total_bytes_before == FILE_SIZE);

	// Re-index: split 16KiB blocks into 4KiB blocks.
	cache.ReindexCachedFiles(/*is_remote=*/false, OLD_BLOCK_SIZE, NEW_BLOCK_SIZE);

	auto cached_after = cache.GetCachedFileInformation();
	idx_t total_bytes_after = 0;
	for (auto &info : cached_after) {
		total_bytes_after += info.nr_bytes;
	}
	// 3 * (16384/4096) + 1 = 13 blocks
	REQUIRE(cached_after.size() == 13);
	REQUIRE(total_bytes_after == FILE_SIZE);
}

TEST_CASE("ReindexCachedFiles merge small blocks into larger blocks", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 4096;  // 4 KiB
	const idx_t NEW_BLOCK_SIZE = 16384; // 16 KiB
	const idx_t FILE_SIZE = OLD_BLOCK_SIZE * 8;

	// Set the block size to 4KiB before populating the cache.
	Connection con(db);
	con.Query("SET external_file_cache_local_block_size=" + to_string(OLD_BLOCK_SIZE));

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	EFCTestFileGuard test_file("test_reindex_merge.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	// Read full file to populate cache at OLD_BLOCK_SIZE.
	{
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
	}

	REQUIRE(cache.GetCachedFileInformation().size() == 8); // 8 x 4KiB blocks

	// Re-index: merge 4KiB blocks into 16KiB blocks.
	cache.ReindexCachedFiles(/*is_remote=*/false, OLD_BLOCK_SIZE, NEW_BLOCK_SIZE);

	auto cached_after = cache.GetCachedFileInformation();
	idx_t total_bytes_after = 0;
	for (auto &info : cached_after) {
		total_bytes_after += info.nr_bytes;
	}
	// 8 * 4KiB = 32KiB = 2 x 16KiB blocks
	REQUIRE(cached_after.size() == 2);
	REQUIRE(total_bytes_after == FILE_SIZE);
}

TEST_CASE("ReindexCachedFiles same block size is a no-op", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 16384;
	const idx_t FILE_SIZE = BLOCK_SIZE * 2;

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	EFCTestFileGuard test_file("test_reindex_noop.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	{
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
	}

	auto cached_before = cache.GetCachedFileInformation();
	REQUIRE(cached_before.size() == 2);

	// Same block size: should be a no-op.
	cache.ReindexCachedFiles(/*is_remote=*/false, BLOCK_SIZE, BLOCK_SIZE);

	auto cached_after = cache.GetCachedFileInformation();
	REQUIRE(cached_after.size() == 2);
}

TEST_CASE("ReindexCachedFiles non-aligned block sizes", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 7000;
	const idx_t NEW_BLOCK_SIZE = 3000;
	const idx_t FILE_SIZE = 21000; // 3 old blocks exactly

	// Set the block size to 7000 before populating the cache.
	Connection con(db);
	con.Query("SET external_file_cache_local_block_size=" + to_string(OLD_BLOCK_SIZE));

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	EFCTestFileGuard test_file("test_reindex_nonaligned.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	{
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
	}

	REQUIRE(cache.GetCachedFileInformation().size() == 3); // 3 x 7000 byte blocks

	cache.ReindexCachedFiles(/*is_remote=*/false, OLD_BLOCK_SIZE, NEW_BLOCK_SIZE);

	auto cached_after = cache.GetCachedFileInformation();
	idx_t total_bytes_after = 0;
	for (auto &info : cached_after) {
		total_bytes_after += info.nr_bytes;
	}
	// 21000 / 3000 = 7 new blocks. All should be present since all old blocks are pinned.
	REQUIRE(cached_after.size() == 7);
	REQUIRE(total_bytes_after == FILE_SIZE);
}

TEST_CASE("ReindexCachedFiles non-aligned merge", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 3000;
	const idx_t NEW_BLOCK_SIZE = 7000;
	const idx_t FILE_SIZE = 21000; // 7 old blocks exactly

	Connection con(db);
	con.Query("SET external_file_cache_local_block_size=" + to_string(OLD_BLOCK_SIZE));

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	EFCTestFileGuard test_file("test_reindex_nonaligned_merge.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	{
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
	}

	REQUIRE(cache.GetCachedFileInformation().size() == 7); // 7 x 3000 byte blocks

	cache.ReindexCachedFiles(/*is_remote=*/false, OLD_BLOCK_SIZE, NEW_BLOCK_SIZE);

	auto cached_after = cache.GetCachedFileInformation();
	idx_t total_bytes_after = 0;
	for (auto &info : cached_after) {
		total_bytes_after += info.nr_bytes;
	}
	// 21000 / 7000 = 3 new blocks. Each needs multiple old blocks (e.g., new block 0
	// needs old blocks 0,1,2 since [0,7000) spans [0,3000),[3000,6000),[6000,9000)).
	// New block 1 at [7000,14000) spans old blocks 2,3,4 — old block 2 covers [6000,9000),
	// so it contributes [7000,9000). All old blocks pinned, so all new blocks created.
	REQUIRE(cached_after.size() == 3);
	REQUIRE(total_bytes_after == FILE_SIZE);
}

TEST_CASE("ReindexCachedFiles local vs remote isolation", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 16384;
	const idx_t FILE_SIZE = BLOCK_SIZE * 2;

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	EFCTestFileGuard test_file("test_reindex_isolation.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	{
		auto group = handle->Read(FILE_SIZE, 0);
		string result(FILE_SIZE, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), FILE_SIZE);
		REQUIRE(result == content);
	}

	auto cached_before = cache.GetCachedFileInformation();
	REQUIRE(cached_before.size() == 2);

	// Re-index for remote files only — local cache should be untouched.
	cache.ReindexCachedFiles(/*is_remote=*/true, BLOCK_SIZE, 4096);

	auto cached_after = cache.GetCachedFileInformation();
	REQUIRE(cached_after.size() == 2); // local file, unchanged

	// Now re-index for local files — should change.
	cache.ReindexCachedFiles(/*is_remote=*/false, BLOCK_SIZE, 4096);

	auto cached_local = cache.GetCachedFileInformation();
	REQUIRE(cached_local.size() == 8); // 2 * (16384/4096) = 8
}

} // namespace duckdb
