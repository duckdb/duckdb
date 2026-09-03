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
#include <condition_variable>

namespace duckdb {

namespace {

using EFCTestFileGuard = CachingTestFileGuard;
using EFCTrackingFileSystem = SimpleTrackingFileSystem;
using EFCNoMetadataFileSystem = NoValidationMetadataFileSystem;

class CachePolicyFileSystem : public SimpleTrackingFileSystem {
public:
	FileMetadata Stats(FileHandle &handle) override {
		stats_count++;
		auto metadata = SimpleTrackingFileSystem::Stats(handle);
		metadata.version_tag = version_tag;
		metadata.cache_valid_until = cache_valid_until;
		return metadata;
	}

	optional<timestamp_t> GetCacheValidUntil(FileHandle &handle) override {
		return cache_valid_until;
	}

	string version_tag = "v1";
	timestamp_t cache_valid_until = timestamp_t::infinity();
	idx_t stats_count = 0;
};

//! File system whose positional reads can be held back, to control the timing of in-flight block fetches.
class BlockingCachePolicyFileSystem : public CachePolicyFileSystem {
public:
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		{
			annotated_unique_lock<annotated_mutex> guard(lock);
			read_count++;
			read_started.notify_all();
			read_released.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return !block_reads; });
		}
		CachePolicyFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	void BlockReads() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		block_reads = true;
	}

	void WaitForReadCount(idx_t count) {
		annotated_unique_lock<annotated_mutex> guard(lock);
		read_started.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return read_count >= count; });
	}

	void ReleaseReads() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		block_reads = false;
		read_released.notify_all();
	}

	idx_t GetReadCount() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return read_count;
	}

private:
	annotated_mutex lock;
	std::condition_variable read_started DUCKDB_GUARDED_BY(lock);
	std::condition_variable read_released DUCKDB_GUARDED_BY(lock);
	bool block_reads DUCKDB_GUARDED_BY(lock) = false;
	idx_t read_count DUCKDB_GUARDED_BY(lock) = 0;
};

OpenFileInfo MakeTestOpenFileInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	return info;
}

OpenFileInfo MakeValidatingOpenFileInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(true);
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

string ReadSequential(CachingFileHandle &handle, idx_t size) {
	auto read_size = size;
	auto group = handle.Read(read_size);
	string result(read_size, '\0');
	group.CopyTo(reinterpret_cast<data_ptr_t>(&result[0]), read_size);
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
	DuckDB db = MakeCacheLocalFilesDB();
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
	DuckDB db = MakeCacheLocalFilesDB();
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
	DuckDB db = MakeCacheLocalFilesDB();
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
	DuckDB db = MakeCacheLocalFilesDB();
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
	DuckDB db = MakeCacheLocalFilesDB();
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

TEST_CASE("Disabled external file cache does not insert into ObjectCache", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
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

TEST_CASE("Sequential read preserves its position when the cache is disabled", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const string first_block(block_size, 'A');
	const string second_block(block_size, 'B');
	EFCTestFileGuard test_file("test_efc_disabled_sequential_position.bin", first_block + second_block);

	CachingFileSystem cfs(*tracking_fs, *db.instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadSequential(*handle, block_size) == first_block);
	REQUIRE(CountCachedBlocks(cache) == 1);

	cache.SetEnabled(false);
	REQUIRE(ReadSequential(*handle, block_size) == second_block);
}

TEST_CASE("Re-enabled external file cache refreshes live handle metadata", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
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
	DuckDB db = MakeCacheLocalFilesDB();
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

TEST_CASE("Disabling external file cache clears ObjectCache sentinels", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
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
	REQUIRE(cache.GetCachedFileCount() == 0);
	REQUIRE(object_cache.GetCurrentMemory() == 0);

	cache.SetEnabled(true);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadFull(*handle, block_size) == content);
	REQUIRE(CountCachedBlocks(cache) == 1);
	REQUIRE(cache.GetCachedFileCount() == 1);
}

TEST_CASE("Entry evicted while referenced allows re-creation of the same path", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto tracking_fs = make_uniq<EFCTrackingFileSystem>();
	CachingFileSystem cfs(*tracking_fs, db_instance);
	auto &cache = db_instance.GetExternalFileCache();
	auto &object_cache = db_instance.GetObjectCache();

	const auto block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const auto content = MakeTestContent(block_size);
	EFCTestFileGuard test_file("test_efc_evict_referenced_entry.bin", content);

	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, content.size()) == content);
	}
	REQUIRE(cache.GetCachedFileCount() == 1);

	auto held_entry = object_cache.GetObject(StringUtil::Format("external_file_cache-%s", test_file.GetPath()));
	REQUIRE(held_entry);

	EvictObjectCache(object_cache);
	REQUIRE(cache.GetCachedFileCount() == 1);

	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, content.size()) == content);
	}
	REQUIRE(cache.GetCachedFileCount() == 1);

	held_entry.reset();
	REQUIRE(cache.GetCachedFileCount() == 1);
}

TEST_CASE("Failed CachingFileHandle construction leaves evictable cached file entries", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
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

	const auto content = MakeTestContent(cache.GetCacheBlockSize(missing_a));
	WriteTestContent(missing_a, content);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(missing_a), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, content.size()) == content);
	}
	REQUIRE(cache.GetCachedFileCount() == 2);

	auto &object_cache = db_instance.GetObjectCache();
	EvictObjectCache(object_cache);
	REQUIRE(cache.GetCachedFileCount() == 0);
}

TEST_CASE("File with freshness deadline but no validators is cached and reused", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	auto fresh_fs = make_uniq<FreshnessOnlyFileSystem>();

	const idx_t BLOCK_SIZE = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(BLOCK_SIZE, 'A');
	const string content_b(BLOCK_SIZE, 'B'); // same size as content_a
	EFCTestFileGuard test_file("test_efc_freshness_reuse.bin", content_a);

	CachingFileSystem cfs(*fresh_fs, db_instance);

	// First read populates the cache.
	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_a);
	}
	REQUIRE(CountCachedBlocks(cache) == 1);
	auto cached_file = cache.GetOrCreateCachedFile(test_file.GetPath());
	optional<timestamp_t> original_valid_until;
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file->meta_lock);
		original_valid_until = cached_file->validation_info.cache_valid_until;
	}

	// Overwrite with same-size content. Within the freshness deadline the cached content is still served,
	// because the file provides no validators to detect the change.
	WriteTestContent(test_file.GetPath(), content_b);
	fresh_fs->max_age_micros *= 2;
	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_a);
	}
	REQUIRE(CountCachedBlocks(cache) == 1);
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file->meta_lock);
		REQUIRE(cached_file->validation_info.cache_valid_until == original_valid_until);
	}
}

TEST_CASE("NO_VALIDATION retains and enforces the initial freshness deadline", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	auto fresh_fs = make_uniq<FreshnessOnlyFileSystem>();

	const idx_t BLOCK_SIZE = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(BLOCK_SIZE, 'A');
	const string content_b(BLOCK_SIZE, 'B');
	const string content_c(BLOCK_SIZE, 'C');
	EFCTestFileGuard test_file("test_efc_no_validation_freshness.bin", content_a);

	CachingFileSystem cfs(*fresh_fs, db_instance);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_a);
	}

	auto cached_file = cache.GetOrCreateCachedFile(test_file.GetPath());
	const auto expired_deadline = timestamp_t(0);
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file->meta_lock);
		REQUIRE(cached_file->validation_info.cache_valid_until != nullopt);
		cached_file->validation_info.cache_valid_until = expired_deadline;
	}

	WriteTestContent(test_file.GetPath(), content_b);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_b);
	}

	// Validate cache metadata.
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file->meta_lock);
		REQUIRE(cached_file->validation_info.cache_valid_until != nullopt);
		REQUIRE(*cached_file->validation_info.cache_valid_until != expired_deadline);
	}

	// The refreshed metadata allows the current file contents to be cached again.
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_b);
	}
	REQUIRE(CountCachedBlocks(cache) == 1);

	// NO_VALIDATION reuses the refreshed cache while its new deadline is fresh.
	WriteTestContent(test_file.GetPath(), content_c);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_b);
	}
}

TEST_CASE("File with freshness deadline is invalidated when the file size changes", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	auto fresh_fs = make_uniq<FreshnessOnlyFileSystem>();

	const idx_t BLOCK_SIZE = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(BLOCK_SIZE, 'A');
	const string content_b(BLOCK_SIZE * 2, 'B');
	EFCTestFileGuard test_file("test_efc_freshness_size_change.bin", content_a);

	CachingFileSystem cfs(*fresh_fs, db_instance);

	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_a);
	}
	REQUIRE(CountCachedBlocks(cache) == 1);

	// Overwrite with larger content: the size mismatch invalidates the cache despite the freshness deadline.
	WriteTestContent(test_file.GetPath(), content_b);
	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(handle->GetFileSize() == content_b.size());
		REQUIRE(ReadFull(*handle, content_b.size()) == content_b);
	}
	REQUIRE(TotalCachedBytes(cache) == content_b.size());
}

TEST_CASE("Long-lived handle does not use cache after the freshness deadline", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	auto fresh_fs = make_uniq<FreshnessOnlyFileSystem>();

	const idx_t BLOCK_SIZE = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(BLOCK_SIZE, 'A');
	const string content_b(BLOCK_SIZE, 'B');
	EFCTestFileGuard test_file("test_efc_freshness_long_lived_handle.bin", content_a);

	CachingFileSystem cfs(*fresh_fs, db_instance);
	auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_a);

	auto cached_file = cache.GetOrCreateCachedFile(test_file.GetPath());
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file->meta_lock);
		cached_file->validation_info.cache_valid_until = timestamp_t(Timestamp::GetCurrentTimestamp().value - 1);
	}

	WriteTestContent(test_file.GetPath(), content_b);
	REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_b);
}

TEST_CASE("Long-lived handle with validators stops using cache after the freshness deadline", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto validating_fs = make_uniq<CachePolicyFileSystem>();
	validating_fs->cache_valid_until = timestamp_t(Timestamp::GetCurrentTimestamp().value + 600 * 1000000LL);

	const idx_t block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(block_size, 'A');
	const string content_b(block_size, 'B');
	EFCTestFileGuard test_file("test_efc_validator_freshness_long_lived_handle.bin", content_a);

	CachingFileSystem cfs(*validating_fs, *db.instance);
	auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadFull(*handle, block_size) == content_a);
	REQUIRE(validating_fs->stats_count == 1);

	auto cached_file = cache.GetOrCreateCachedFile(test_file.GetPath());
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file->meta_lock);
		cached_file->validation_info.cache_valid_until = timestamp_t(Timestamp::GetCurrentTimestamp().value - 1);
	}
	WriteTestContent(test_file.GetPath(), content_b);

	REQUIRE(ReadFull(*handle, block_size) == content_b);
	REQUIRE(validating_fs->stats_count == 1);
}

TEST_CASE("File marked as not cacheable does not retain cached blocks", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto policy_fs = make_uniq<CachePolicyFileSystem>();

	const idx_t block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(block_size, 'A');
	const string content_b(block_size, 'B');
	EFCTestFileGuard test_file("test_efc_no_store.bin", content_a);
	CachingFileSystem cfs(*policy_fs, *db.instance);

	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, block_size) == content_a);
	}
	REQUIRE(CountCachedBlocks(cache) == 1);

	WriteTestContent(test_file.GetPath(), content_b);
	policy_fs->cache_valid_until = timestamp_t::ninfinity();
	policy_fs->version_tag = "v2";
	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, block_size) == content_b);
	}
	REQUIRE(CountCachedBlocks(cache) == 0);
}

TEST_CASE("Explicit cache reuse prohibition is honored under NO_VALIDATION", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto policy_fs = make_uniq<CachePolicyFileSystem>();
	policy_fs->cache_valid_until = timestamp_t::ninfinity();

	const idx_t block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(block_size, 'A');
	const string content_b(block_size, 'B');
	EFCTestFileGuard test_file("test_efc_no_reuse_no_validation.bin", content_a);
	CachingFileSystem cfs(*policy_fs, *db.instance);

	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, block_size) == content_a);
	}
	REQUIRE(CountCachedBlocks(cache) == 0);

	WriteTestContent(test_file.GetPath(), content_b);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, block_size) == content_b);
	}
	REQUIRE(CountCachedBlocks(cache) == 0);
}

TEST_CASE("Expired freshness deadline is not served from cache", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	auto fresh_fs = make_uniq<FreshnessOnlyFileSystem>();
	fresh_fs->max_age_micros = -1000000; // deadline is always in the past

	const idx_t BLOCK_SIZE = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(BLOCK_SIZE, 'A');
	const string content_b(BLOCK_SIZE, 'B'); // same size as content_a
	EFCTestFileGuard test_file("test_efc_freshness_expired.bin", content_a);

	CachingFileSystem cfs(*fresh_fs, db_instance);

	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_a);
	}

	// Overwrite with same-size content: the expired deadline prevents serving stale cached data.
	WriteTestContent(test_file.GetPath(), content_b);
	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_b);
	}
}

TEST_CASE("Waiter on a loading block refetches when the response prohibits sharing", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto policy_fs = make_uniq<BlockingCachePolicyFileSystem>();

	const idx_t block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content(block_size, 'A');
	EFCTestFileGuard test_file("test_efc_loading_waiter.bin", content);
	CachingFileSystem cfs(*policy_fs, *db.instance);

	// Open both handles while the policy still allows sharing.
	auto handle_a = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	auto handle_b = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);

	// Content responses prohibit reuse, and hang until released.
	policy_fs->cache_valid_until = timestamp_t::ninfinity();
	policy_fs->BlockReads();

	string result_a;
	string result_b;
	std::thread reader_a([&]() { result_a = ReadFull(*handle_a, block_size); });
	// Once reader A's fetch is in flight, the block is LOADING, so reader B waits on it.
	policy_fs->WaitForReadCount(1);

	annotated_mutex reader_b_lock;
	std::condition_variable reader_b_started;
	bool reader_b_is_started = false;
	std::thread reader_b([&]() {
		{
			annotated_lock_guard<annotated_mutex> guard(reader_b_lock);
			reader_b_is_started = true;
			reader_b_started.notify_one();
		}
		result_b = ReadFull(*handle_b, block_size);
	});
	{
		annotated_unique_lock<annotated_mutex> guard(reader_b_lock);
		reader_b_started.wait(guard, [&]() DUCKDB_REQUIRES(reader_b_lock) { return reader_b_is_started; });
	}
	policy_fs->ReleaseReads();

	reader_a.join();
	reader_b.join();
	REQUIRE(result_a == content);
	REQUIRE(result_b == content);
	// Reader B must not consume reader A's response: each reader issues its own request.
	REQUIRE(policy_fs->GetReadCount() == 2);
	REQUIRE(CountCachedBlocks(cache) == 0);
}

TEST_CASE("Content response can prohibit cache reuse", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto policy_fs = make_uniq<CachePolicyFileSystem>();

	const idx_t block_size = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(block_size, 'A');
	const string content_b(block_size, 'B');
	EFCTestFileGuard test_file("test_efc_content_policy.bin", content_a);
	CachingFileSystem cfs(*policy_fs, *db.instance);

	auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	// Cache validation expiration is found after a successful open.
	policy_fs->cache_valid_until = timestamp_t::ninfinity();
	REQUIRE(ReadFull(*handle, block_size) == content_a);
	REQUIRE(CountCachedBlocks(cache) == 0);

	WriteTestContent(test_file.GetPath(), content_b);
	REQUIRE(ReadFull(*handle, block_size) == content_b);
	REQUIRE(CountCachedBlocks(cache) == 0);
}

TEST_CASE("No-metadata file is not cached and always returns fresh content", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	auto no_meta_fs = make_uniq<EFCNoMetadataFileSystem>();

	const idx_t BLOCK_SIZE = cache.GetCacheBlockSize(TestDirectoryPath());
	const string content_a(BLOCK_SIZE, 'A');
	const string content_b(BLOCK_SIZE * 2, 'B');
	EFCTestFileGuard test_file("test_efc_no_metadata.bin", content_a);

	CachingFileSystem cfs(*no_meta_fs, db_instance);

	// First read: data is fetched from source.  No blocks should be stored in the cache.
	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(handle->GetFileSize() == content_a.size());
		REQUIRE(ReadFull(*handle, BLOCK_SIZE) == content_a);
	}
	REQUIRE(CountCachedBlocks(cache) == 0);

	// Overwrite the file with larger content.
	WriteTestContent(test_file.GetPath(), content_b);

	// Second read: file size and content must reflect the new version, not the cached one.
	{
		auto handle = cfs.OpenFile(MakeValidatingOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(handle->GetFileSize() == content_b.size());
		REQUIRE(ReadFull(*handle, content_b.size()) == content_b);
	}
	REQUIRE(CountCachedBlocks(cache) == 0);
}

TEST_CASE("Retiring cache blocks preserves existing readers and cannot erase replacements", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto cached_file = cache.GetOrCreateCachedFile("retire-blocks-test");

	constexpr idx_t BLOCK_SIZE = 4096;
	constexpr idx_t FIRST_BLOCK = 7;
	auto acquired = cache.ReindexAndAcquireBlocks(*cached_file, BLOCK_SIZE, FIRST_BLOCK, 2);
	{
		const annotated_lock_guard<annotated_mutex> guard(acquired[0]->mtx);
		acquired[0]->state = CacheBlockState::LOADED;
		acquired[0]->nr_bytes = BLOCK_SIZE;
	}
	{
		const annotated_lock_guard<annotated_mutex> guard(acquired[1]->mtx);
		acquired[1]->state = CacheBlockState::LOADED;
		acquired[1]->nr_bytes = 123;
	}

	cache.RetireBlocks(*cached_file, FIRST_BLOCK, acquired);

	// Existing readers retain the immutable block metadata needed to finish their reads.
	{
		const annotated_lock_guard<annotated_mutex> guard(acquired[0]->mtx);
		REQUIRE(acquired[0]->state == CacheBlockState::LOADED);
		REQUIRE(acquired[0]->nr_bytes == BLOCK_SIZE);
	}
	{
		const annotated_lock_guard<annotated_mutex> guard(acquired[1]->mtx);
		REQUIRE(acquired[1]->state == CacheBlockState::LOADED);
		REQUIRE(acquired[1]->nr_bytes == 123);
	}

	// Future readers receive fresh cache blocks.
	auto replacements = cache.ReindexAndAcquireBlocks(*cached_file, BLOCK_SIZE, FIRST_BLOCK, 2);
	REQUIRE(replacements[0] != acquired[0]);
	REQUIRE(replacements[1] != acquired[1]);

	// A delayed retirement of the old range must not erase its replacements.
	cache.RetireBlocks(*cached_file, FIRST_BLOCK, acquired);
	auto reacquired = cache.ReindexAndAcquireBlocks(*cached_file, BLOCK_SIZE, FIRST_BLOCK, 2);
	REQUIRE(reacquired[0] == replacements[0]);
	REQUIRE(reacquired[1] == replacements[1]);
}

} // namespace duckdb
