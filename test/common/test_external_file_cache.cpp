#include "catch.hpp"
#include "caching_test_utils.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <chrono>

namespace duckdb {

namespace {

using EFCTestFileGuard = CachingTestFileGuard;
using EFCTrackingFileSystem = SimpleTrackingFileSystem;

OpenFileInfo MakeTestOpenFileInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	return info;
}

string MakeTestContent(idx_t size) {
	string content(size, '\0');
	for (idx_t i = 0; i < size; i++) {
		content[i] = static_cast<char>('A' + (i % 26));
	}
	return content;
}

string ReadFull(CachingFileHandle &handle, idx_t size, idx_t offset = 0) {
	auto group = handle.Read(size, offset);
	string result(size, '\0');
	group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), size);
	return result;
}

void WriteTestContent(const string &path, const string &content) {
	auto local_fs = FileSystem::CreateLocal();
	auto handle = local_fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(QueryContext(), const_cast<char *>(content.data()), content.size(), 0);
	handle->Sync();
}

idx_t CountCachedBlocks(ExternalFileCache &cache) {
	return cache.GetCachedFileInformation().size();
}

idx_t TotalCachedBytes(ExternalFileCache &cache) {
	idx_t total = 0;
	for (auto &info : cache.GetCachedFileInformation()) {
		total += info.nr_bytes;
	}
	return total;
}

void EvictObjectCache(ObjectCache &object_cache) {
	const auto memory = object_cache.GetCurrentMemory();
	REQUIRE(memory > 0);
	REQUIRE(object_cache.EvictToReduceMemory(memory) > 0);
}

} // namespace

TEST_CASE("Lazy reindex splits large blocks on next read", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 16384;
	const idx_t NEW_BLOCK_SIZE = 4096;
	const idx_t FILE_SIZE = OLD_BLOCK_SIZE * 3 + 100;

	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_reindex_split.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(CountCachedBlocks(cache) == 4);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);

	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", NEW_BLOCK_SIZE));

	// Cache still has 4 old blocks (not yet reindexed).
	REQUIRE(CountCachedBlocks(cache) == 4);

	// Next read triggers lazy reindex: 16KiB -> 4KiB.
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);

	// 3 * (16384/4096) + 1 = 13 blocks
	REQUIRE(CountCachedBlocks(cache) == 13);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);
}

TEST_CASE("Lazy reindex merges small blocks on next read", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 4096;
	const idx_t NEW_BLOCK_SIZE = 16384;
	const idx_t FILE_SIZE = OLD_BLOCK_SIZE * 8;

	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", OLD_BLOCK_SIZE));

	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_reindex_merge.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(CountCachedBlocks(cache) == 8);

	// Change block size.
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", NEW_BLOCK_SIZE));

	// Still 8 old blocks.
	REQUIRE(CountCachedBlocks(cache) == 8);

	// Next read triggers lazy reindex: 4KiB -> 16KiB.
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);

	REQUIRE(CountCachedBlocks(cache) == 2);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);
}

TEST_CASE("Lazy reindex is a no-op for same block size", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 16384;
	const idx_t FILE_SIZE = BLOCK_SIZE * 2;

	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_reindex_noop.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(CountCachedBlocks(cache) == 2);

	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", BLOCK_SIZE));

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(CountCachedBlocks(cache) == 2);
}

TEST_CASE("Lazy reindex with holes in cached content", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 4096;
	const idx_t NEW_BLOCK_SIZE = 16384;
	const idx_t FILE_SIZE = OLD_BLOCK_SIZE * 8;

	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", OLD_BLOCK_SIZE));

	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_reindex_holes.bin", content);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	// Only read blocks 0-1 and 4-7, skipping blocks 2-3 to create a hole.
	REQUIRE(ReadFull(*handle, OLD_BLOCK_SIZE * 2, 0) == content.substr(0, OLD_BLOCK_SIZE * 2));
	REQUIRE(ReadFull(*handle, OLD_BLOCK_SIZE * 4, OLD_BLOCK_SIZE * 4) ==
	        content.substr(OLD_BLOCK_SIZE * 4, OLD_BLOCK_SIZE * 4));
	REQUIRE(CountCachedBlocks(cache) == 6);

	// Change block size.
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", NEW_BLOCK_SIZE));

	// Still 6 old blocks.
	REQUIRE(CountCachedBlocks(cache) == 6);

	// Read the second half, which triggers lazy reindex of all blocks in this file.
	// Reindex: blocks 4-7 merge into 1 new 16KiB block. Blocks 0-1 can't form a complete 16KiB block thus dropped.
	REQUIRE(ReadFull(*handle, NEW_BLOCK_SIZE, NEW_BLOCK_SIZE) == content.substr(NEW_BLOCK_SIZE, NEW_BLOCK_SIZE));

	REQUIRE(CountCachedBlocks(cache) == 1);
	REQUIRE(TotalCachedBytes(cache) == NEW_BLOCK_SIZE);
}

TEST_CASE("Lazy reindex: only touched file is reindexed", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t OLD_BLOCK_SIZE = 4096;
	const idx_t NEW_BLOCK_SIZE = 16384;
	const idx_t FILE_SIZE = OLD_BLOCK_SIZE * 8;

	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", OLD_BLOCK_SIZE));

	auto content_a = MakeTestContent(FILE_SIZE);
	auto content_b = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard file_a("test_lazy_multi_a.bin", content_a);
	EFCTestFileGuard file_b("test_lazy_multi_b.bin", content_b);

	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto handle_a = cfs.OpenFile(MakeTestOpenFileInfo(file_a.GetPath()), FileFlags::FILE_FLAGS_READ);
	auto handle_b = cfs.OpenFile(MakeTestOpenFileInfo(file_b.GetPath()), FileFlags::FILE_FLAGS_READ);
	auto &cache = db_instance.GetExternalFileCache();

	// Populate both files at 4KiB.
	REQUIRE(ReadFull(*handle_a, FILE_SIZE) == content_a);
	REQUIRE(ReadFull(*handle_b, FILE_SIZE) == content_b);
	REQUIRE(CountCachedBlocks(cache) == 16); // 8 blocks per file

	// Change block size.
	con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", NEW_BLOCK_SIZE));
	REQUIRE(CountCachedBlocks(cache) == 16);

	// Read only file A — triggers lazy reindex of A only.
	REQUIRE(ReadFull(*handle_a, FILE_SIZE) == content_a);

	// Count blocks per file.
	auto infos = cache.GetCachedFileInformation();
	idx_t blocks_a = 0, blocks_b = 0;
	for (auto &info : infos) {
		if (info.path == file_a.GetPath()) {
			blocks_a++;
		} else {
			blocks_b++;
		}
	}
	REQUIRE(blocks_a == 2); // reindexed: 8 x 4KiB -> 2 x 16KiB
	REQUIRE(blocks_b == 8); // untouched: still 8 x 4KiB
}

TEST_CASE("Disabled external file cache does not insert into cached_files", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	const idx_t FILE_SIZE = 16384;
	const auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_disabled.bin", content);

	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);

	// Disable the cache.
	cache.SetEnabled(false);
	REQUIRE_FALSE(cache.IsEnabled());
	REQUIRE(cache.GetCachedFileCount() == 0);

	// Open and fully read the file with caching disabled.
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	}

	// With caching disabled, no entry should exist in the cache file map.
	REQUIRE(cache.GetCachedFileCount() == 0);
	REQUIRE(CountCachedBlocks(cache) == 0);

	// When cache enabled, opening and reading the file does populate the map.
	cache.SetEnabled(true);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	}
	REQUIRE(cache.GetCachedFileCount() == 1);
}

TEST_CASE("Re-enabled external file cache refreshes live handle metadata", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	const string content_a(64, 'A');
	const string content_b(128, 'B');
	EFCTestFileGuard test_file("test_efc_reenabled_live_handle_metadata.bin", content_a);

	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);

	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(handle->GetFileSize() == content_a.size());
	REQUIRE(cache.GetCachedFileCount() == 1);

	cache.SetEnabled(false);
	REQUIRE(cache.GetCachedFileCount() == 0);
	WriteTestContent(test_file.GetPath(), content_b);

	cache.SetEnabled(true);
	REQUIRE(handle->GetFileSize() == content_b.size());
	REQUIRE(cache.GetCachedFileCount() == 1);
}

TEST_CASE("Concurrent SET and Read do not corrupt data or cache state", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;

	constexpr idx_t FILE_SIZE = 64 * 1024 + 137; // odd tail to stress boundaries
	const auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_set_vs_read.bin", content);

	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);

	constexpr idx_t READER_COUNT = 4;
	constexpr idx_t SETTER_COUNT = 2;
	constexpr array<idx_t, 3> BLOCK_SIZES = {4096, 8192, 16384};

	atomic<bool> stop {false};
	atomic<idx_t> mismatches {0};

	struct ReadFixture {
		idx_t off;
		idx_t len;
	};
	const array<ReadFixture, 5> FIXTURES = {{
	    {0, 4096},
	    {123, 8000},
	    {17000, 17000},
	    {FILE_SIZE - 1024, 1024},
	    {0, FILE_SIZE},
	}};

	vector<std::thread> threads;
	threads.reserve(READER_COUNT + SETTER_COUNT);

	for (idx_t r = 0; r < READER_COUNT; r++) {
		threads.emplace_back([&, r]() {
			auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
			idx_t i = r;
			while (!stop.load()) {
				const auto &fx = FIXTURES[i % FIXTURES.size()];
				if (ReadFull(*handle, fx.len, fx.off) != content.substr(fx.off, fx.len)) {
					mismatches.fetch_add(1);
				}
				i++;
			}
		});
	}

	for (idx_t s = 0; s < SETTER_COUNT; s++) {
		threads.emplace_back([&, s]() {
			Connection con(db);
			idx_t i = s;
			while (!stop.load()) {
				const idx_t bs = BLOCK_SIZES[i % BLOCK_SIZES.size()];
				con.Query(StringUtil::Format("SET external_file_cache_local_block_size=%llu", bs));
				i++;
			}
		});
	}

	// Run the set and read for a while.
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	stop.store(true);
	for (auto &t : threads) {
		t.join();
	}
	REQUIRE(mismatches.load() == 0);

	auto &cache = db_instance.GetExternalFileCache();
	idx_t total_cached_bytes = 0;
	for (auto &info : cache.GetCachedFileInformation()) {
		REQUIRE(info.location + info.nr_bytes <= FILE_SIZE);
		total_cached_bytes += info.nr_bytes;
	}

	REQUIRE(total_cached_bytes <= FILE_SIZE);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
}

TEST_CASE("ObjectCache eviction removes zero-ref external file cache entry", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto &cache = db_instance.GetExternalFileCache();
	auto &object_cache = db_instance.GetObjectCache();

	const auto block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const auto content = MakeTestContent(block_size);
	EFCTestFileGuard test_file("test_efc_object_cache_eviction.bin", content);

	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, block_size) == content);
		REQUIRE(CountCachedBlocks(cache) == 1);
		REQUIRE(object_cache.GetCurrentMemory() > 0);
	}

	REQUIRE(CountCachedBlocks(cache) == 1);
	EvictObjectCache(object_cache);
	REQUIRE(CountCachedBlocks(cache) == 0);
}

TEST_CASE("ObjectCache eviction removes active external file cache map entry", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto &cache = db_instance.GetExternalFileCache();
	auto &object_cache = db_instance.GetObjectCache();

	const auto block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const auto content = MakeTestContent(block_size);
	EFCTestFileGuard test_file("test_efc_object_cache_active.bin", content);

	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, block_size) == content);
	}

	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadFull(*handle, block_size) == content);
	REQUIRE(object_cache.GetCurrentMemory() > 0);
	REQUIRE(CountCachedBlocks(cache) == 1);

	const auto object_cache_memory = object_cache.GetCurrentMemory();
	REQUIRE(object_cache.EvictToReduceMemory(object_cache_memory) == object_cache_memory);
	REQUIRE(CountCachedBlocks(cache) == 0);
	REQUIRE(object_cache.GetCurrentMemory() == 0);

	REQUIRE(ReadFull(*handle, block_size) == content);
}

TEST_CASE("Disabling external file cache clears ObjectCache sentinels", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto &cache = db_instance.GetExternalFileCache();
	auto &object_cache = db_instance.GetObjectCache();

	const auto block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const auto content = MakeTestContent(block_size);
	EFCTestFileGuard test_file("test_efc_object_cache_disable.bin", content);

	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, block_size) == content);
	}

	REQUIRE(CountCachedBlocks(cache) == 1);
	REQUIRE(object_cache.GetCurrentMemory() > 0);

	cache.SetEnabled(false);
	REQUIRE(CountCachedBlocks(cache) == 0);
	REQUIRE(object_cache.GetCurrentMemory() == 0);

	cache.SetEnabled(true);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadFull(*handle, block_size) == content);
	REQUIRE(CountCachedBlocks(cache) == 1);
}

TEST_CASE("Failed CachingFileHandle construction leaves evictable cached file entries", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto &cache = db_instance.GetExternalFileCache();

	auto local_fs = FileSystem::CreateLocal();
	const auto missing_a = TestCreatePath("test_efc_missing_a.bin");
	const auto missing_b = TestCreatePath("test_efc_missing_b.bin");
	local_fs->TryRemoveFile(missing_a);
	local_fs->TryRemoveFile(missing_b);

	REQUIRE_THROWS(cfs.OpenFile(MakeTestOpenFileInfo(missing_a), FileFlags::FILE_FLAGS_READ));
	REQUIRE_THROWS(cfs.OpenFile(MakeTestOpenFileInfo(missing_b), FileFlags::FILE_FLAGS_READ));

	REQUIRE(cache.GetCachedFileCount() == 2);
	auto &object_cache = db_instance.GetObjectCache();
	EvictObjectCache(object_cache);
	REQUIRE(cache.GetCachedFileCount() == 0);
}

} // namespace duckdb
