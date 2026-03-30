#include "catch.hpp"
#include "caching_test_utils.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"

namespace duckdb {

using EFCTestFileGuard = CachingTestFileGuard;
using EFCTrackingFileSystem = SimpleTrackingFileSystem;

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

TEST_CASE("ReindexCachedFiles with holes in cached content", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 4096;
	const idx_t NEW_BLOCK_SIZE = 16384;
	const idx_t FILE_SIZE = OLD_BLOCK_SIZE * 8; // 32KiB = 8 old blocks

	Connection con(db);
	con.Query("SET external_file_cache_local_block_size=" + to_string(OLD_BLOCK_SIZE));

	string content(FILE_SIZE, '\0');
	for (idx_t i = 0; i < FILE_SIZE; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	EFCTestFileGuard test_file("test_reindex_holes.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	OpenFileInfo file_info(test_file.GetPath());
	file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);

	auto handle = cfs.OpenFile(file_info, FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	// Only read blocks 0-1 and 4-7, skipping blocks 2-3 to create a hole.
	// Blocks 0-1: [0, 8192)
	{
		auto group = handle->Read(OLD_BLOCK_SIZE * 2, 0);
		string result(OLD_BLOCK_SIZE * 2, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), OLD_BLOCK_SIZE * 2);
		REQUIRE(result == content.substr(0, OLD_BLOCK_SIZE * 2));
	}
	// Blocks 4-7: [16384, 32768)
	{
		auto group = handle->Read(OLD_BLOCK_SIZE * 4, OLD_BLOCK_SIZE * 4);
		string result(OLD_BLOCK_SIZE * 4, '\0');
		group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), OLD_BLOCK_SIZE * 4);
		REQUIRE(result == content.substr(OLD_BLOCK_SIZE * 4, OLD_BLOCK_SIZE * 4));
	}

	// 6 cached blocks: 0,1 and 4,5,6,7 (blocks 2,3 are the hole)
	REQUIRE(cache.GetCachedFileInformation().size() == 6);

	// Merge 4KiB -> 16KiB. Two new blocks possible:
	// New block 0 [0, 16384): needs old blocks 0,1,2,3 — blocks 2,3 missing → skipped
	// New block 1 [16384, 32768): needs old blocks 4,5,6,7 — all present → created
	cache.ReindexCachedFiles(/*is_remote=*/false, OLD_BLOCK_SIZE, NEW_BLOCK_SIZE);

	auto cached_after = cache.GetCachedFileInformation();
	idx_t total_bytes_after = 0;
	for (auto &info : cached_after) {
		total_bytes_after += info.nr_bytes;
	}
	REQUIRE(cached_after.size() == 1);
	REQUIRE(total_bytes_after == NEW_BLOCK_SIZE); // only the second 16KiB block
}

} // namespace duckdb
