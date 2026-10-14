#include "catch.hpp"
#include "caching_test_utils.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <chrono>
#include <condition_variable>
#include <future>

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

class LocationBlockingFileSystem : public SimpleTrackingFileSystem {
public:
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		{
			annotated_unique_lock<annotated_mutex> guard(lock);
			if (block_reads && location == blocked_location) {
				read_blocked = true;
				read_started.notify_all();
				read_released.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return !block_reads; });
			}
		}
		SimpleTrackingFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	void BlockReadsAt(idx_t location) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		blocked_location = location;
		block_reads = true;
	}

	void WaitForBlockedRead() {
		annotated_unique_lock<annotated_mutex> guard(lock);
		read_started.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return read_blocked; });
	}

	void ReleaseReads() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		block_reads = false;
		read_released.notify_all();
	}

private:
	annotated_mutex lock;
	std::condition_variable read_started DUCKDB_GUARDED_BY(lock);
	std::condition_variable read_released DUCKDB_GUARDED_BY(lock);
	idx_t blocked_location DUCKDB_GUARDED_BY(lock) = 0;
	bool block_reads DUCKDB_GUARDED_BY(lock) = false;
	bool read_blocked DUCKDB_GUARDED_BY(lock) = false;
};

class SuffixTrackingFileSystem : public SimpleTrackingFileSystem {
public:
	FileMetadata Stats(FileHandle &handle) override {
		stats_count++;
		auto metadata = LocalFileSystem::Stats(handle);
		metadata.version_tag = stats_version_tag;
		return metadata;
	}

	bool TryReadSuffix(FileHandle &handle, data_ptr_t buffer, idx_t buffer_len, SuffixReadResult &result) override {
		suffix_requests.push_back(buffer_len);
		if (!suffix_supported) {
			return false;
		}

		auto metadata = LocalFileSystem::Stats(handle);
		metadata.version_tag = suffix_version_tag;
		const auto file_size = NumericCast<idx_t>(metadata.file_size);
		const auto bytes_read = MinValue(file_size, buffer_len);
		LocalFileSystem::Read(handle, buffer, bytes_read, file_size - bytes_read);
		result.bytes_read = bytes_read;
		result.metadata = std::move(metadata);
		return true;
	}

	bool suffix_supported = true;
	string suffix_version_tag = "suffix-v1";
	string stats_version_tag = "stats-v1";
	idx_t stats_count = 0;
	vector<idx_t> suffix_requests;
};

class BlockingSuffixFileSystem : public SimpleTrackingFileSystem {
public:
	FileMetadata Stats(FileHandle &handle) override {
		auto metadata = LocalFileSystem::Stats(handle);
		metadata.version_tag = StringUtil::Format("version-%lld", metadata.file_size);
		return metadata;
	}

	bool TryReadSuffix(FileHandle &handle, data_ptr_t buffer, idx_t buffer_len, SuffixReadResult &result) override {
		auto metadata = Stats(handle);
		const auto file_size = NumericCast<idx_t>(metadata.file_size);
		const auto bytes_read = MinValue(file_size, buffer_len);
		LocalFileSystem::Read(handle, buffer, bytes_read, file_size - bytes_read);
		{
			annotated_unique_lock<annotated_mutex> guard(lock);
			suffix_started = true;
			started.notify_all();
			released.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return release_suffix; });
		}
		result.bytes_read = bytes_read;
		result.metadata = std::move(metadata);
		return true;
	}

	void WaitForSuffix() {
		annotated_unique_lock<annotated_mutex> guard(lock);
		started.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return suffix_started; });
	}

	void ReleaseSuffix() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		release_suffix = true;
		released.notify_all();
	}

private:
	annotated_mutex lock;
	std::condition_variable started DUCKDB_GUARDED_BY(lock);
	std::condition_variable released DUCKDB_GUARDED_BY(lock);
	bool suffix_started DUCKDB_GUARDED_BY(lock) = false;
	bool release_suffix DUCKDB_GUARDED_BY(lock) = false;
};

class VersionedSuffixFileSystem : public LocalFileSystem {
public:
	static const string &Prefix() {
		static const string prefix = "s3://suffix-version/";
		return prefix;
	}

	string GetName() const override {
		return "VersionedSuffixFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, Prefix());
	}

	bool IsLocalFileSystem() const override {
		return false;
	}

	bool FileExists(const string &path, optional_ptr<FileOpener> opener) override {
		return CanHandleFile(path) && LocalFileSystem::FileExists(GetCurrentPath(), opener);
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override {
		if (FileExists(path, opener)) {
			return {OpenFileInfo(path)};
		}
		return {};
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		return LocalFileSystem::OpenFile(GetCurrentPath(), flags, opener);
	}

	FileMetadata Stats(FileHandle &handle) override {
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			stats_requests++;
		}
		return GetMetadata(handle);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		annotated_lock_guard<annotated_mutex> guard(lock);
		file_size_requests++;
		return GetMetadata(handle).file_size;
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		annotated_lock_guard<annotated_mutex> guard(lock);
		last_modified_requests++;
		return GetMetadata(handle).last_modification_time;
	}

	string GetVersionTag(FileHandle &handle) override {
		annotated_lock_guard<annotated_mutex> guard(lock);
		version_tag_requests++;
		return GetMetadata(handle).version_tag;
	}

	FileType GetFileType(FileHandle &handle) override {
		return FileType::FILE_TYPE_REGULAR;
	}

	FileMetadata GetMetadata(FileHandle &handle) {
		auto metadata = LocalFileSystem::Stats(handle);
		metadata.version_tag = handle.GetPath();
		return metadata;
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			read_requests++;
		}
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	bool TryReadSuffix(FileHandle &handle, data_ptr_t buffer, idx_t buffer_len, SuffixReadResult &result) override {
		auto metadata = GetMetadata(handle);
		const auto file_size = NumericCast<idx_t>(metadata.file_size);
		const auto bytes_read = MinValue(file_size, buffer_len);
		LocalFileSystem::Read(handle, buffer, bytes_read, file_size - bytes_read);
		{
			annotated_unique_lock<annotated_mutex> guard(lock);
			suffix_requests++;
			if (block_suffix) {
				suffix_started = true;
				started.notify_all();
				released.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return !block_suffix; });
			}
		}
		result.bytes_read = bytes_read;
		result.metadata = std::move(metadata);
		return true;
	}

	void SetCurrentPath(string path) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		current_path = std::move(path);
	}

	void BlockSuffix() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		block_suffix = true;
	}

	bool WaitForSuffix() {
		annotated_unique_lock<annotated_mutex> guard(lock);
		return started.wait_for(guard, std::chrono::seconds(5), [&]() DUCKDB_REQUIRES(lock) { return suffix_started; });
	}

	void ReleaseSuffix() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		block_suffix = false;
		released.notify_all();
	}

	idx_t GetSuffixRequests() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return suffix_requests;
	}

	idx_t GetReadRequests() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return read_requests;
	}

	idx_t GetStatsRequests() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return stats_requests;
	}

	idx_t GetFileSizeRequests() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return file_size_requests;
	}

	idx_t GetLastModifiedRequests() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return last_modified_requests;
	}

	idx_t GetVersionTagRequests() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return version_tag_requests;
	}

private:
	string GetCurrentPath() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return current_path;
	}

	annotated_mutex lock;
	std::condition_variable started DUCKDB_GUARDED_BY(lock);
	std::condition_variable released DUCKDB_GUARDED_BY(lock);
	string current_path DUCKDB_GUARDED_BY(lock);
	idx_t suffix_requests DUCKDB_GUARDED_BY(lock) = 0;
	idx_t read_requests DUCKDB_GUARDED_BY(lock) = 0;
	idx_t stats_requests DUCKDB_GUARDED_BY(lock) = 0;
	idx_t file_size_requests DUCKDB_GUARDED_BY(lock) = 0;
	idx_t last_modified_requests DUCKDB_GUARDED_BY(lock) = 0;
	idx_t version_tag_requests DUCKDB_GUARDED_BY(lock) = 0;
	bool block_suffix DUCKDB_GUARDED_BY(lock) = false;
	bool suffix_started DUCKDB_GUARDED_BY(lock) = false;
};

class TemporaryParquetFile {
public:
	explicit TemporaryParquetFile(string path_p) : path(std::move(path_p)) {
		TestDeleteFile(path);
	}
	~TemporaryParquetFile() {
		TestDeleteFile(path);
	}

	const string path;
};

OpenFileInfo MakeTestOpenFileInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	return info;
}

OpenFileInfo MakeDeferredOpenFileInfo(const string &path) {
	auto info = MakeTestOpenFileInfo(path);
	info.extended_info->options["defer_file_info"] = Value::BOOLEAN(true);
	return info;
}

OpenFileInfo MakeValidatingOpenFileInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(true);
	return info;
}

FileOpenFlags ReaderSizedFlags() {
	auto flags = FileFlags::FILE_FLAGS_READ;
	flags.SetRequestSizing(RequestSizing::BY_READER);
	return flags;
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

string ReadSuffixTail(const FileBufferHandleGroup &suffix, const SuffixReadResult &result, idx_t size) {
	D_ASSERT(size <= result.bytes_read);
	string tail(size, '\0');
	memcpy(data_ptr_cast(&tail[0]), suffix.Ptr() + result.bytes_read - size, size);
	return tail;
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

TEST_CASE("Reads cache exactly the requested bytes", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t FILE_SIZE = 65536;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_exact_ranges.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, 5000, 1000) == content.substr(1000, 5000));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{1000, 5000}});
	REQUIRE(CountCachedBlocks(cache) == 1);
	REQUIRE(TotalCachedBytes(cache) == 5000);

	// A repeated read is served from the cache
	REQUIRE(ReadFull(*handle, 5000, 1000) == content.substr(1000, 5000));
	REQUIRE(recording_fs->TakeReads().empty());

	// An overlapping read only fetches the bytes that are not cached yet
	REQUIRE(ReadFull(*handle, 8000, 2000) == content.substr(2000, 8000));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{6000, 4000}});
	REQUIRE(CountCachedBlocks(cache) == 2);
	REQUIRE(TotalCachedBytes(cache) == 9000);
}

TEST_CASE("Large reads are split at the cache block size", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 4096;
	const idx_t FILE_SIZE = BLOCK_SIZE * 3 + 100;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE));

	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_split_reads.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads() ==
	        vector<pair<idx_t, idx_t>> {
	            {0, BLOCK_SIZE}, {BLOCK_SIZE, BLOCK_SIZE}, {2 * BLOCK_SIZE, BLOCK_SIZE}, {3 * BLOCK_SIZE, 100}});
	REQUIRE(CountCachedBlocks(cache) == 4);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);

	// Cached blocks stay valid when the block size changes
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE * 4));
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads().empty());
	REQUIRE(CountCachedBlocks(cache) == 4);
}

TEST_CASE("Suffix reads use the remote cache block size and populate the cache", "[external_file_cache]") {
	DuckDB db(nullptr);
	auto &db_instance = *db.instance;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET GLOBAL external_file_cache_remote_max_block_size=4096"));

	const idx_t FILE_SIZE = 8192 + 123;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_suffix_cache.bin", content);
	const auto remote_path = CACHING_TEST_REMOTE_PREFIX + test_file.GetPath();
	auto recording_fs = make_uniq<SuffixTrackingFileSystem>();

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeDeferredOpenFileInfo(remote_path), ReaderSizedFlags());
	REQUIRE(recording_fs->stats_count == 0);
	REQUIRE(handle->CanSeek());
	REQUIRE(recording_fs->stats_count == 0);

	FileBufferHandleGroup suffix;
	SuffixReadResult result;
	REQUIRE(handle->TryReadSuffix(8, suffix, result));
	auto suffix_bytes = ReadSuffixTail(suffix, result, 8);

	REQUIRE(recording_fs->suffix_requests == vector<idx_t> {4096});
	REQUIRE(recording_fs->stats_count == 0);
	REQUIRE(result.bytes_read == 4096);
	REQUIRE(result.metadata.file_size == NumericCast<int64_t>(FILE_SIZE));
	REQUIRE(result.metadata.version_tag == recording_fs->suffix_version_tag);
	REQUIRE(handle->GetVersionTag() == recording_fs->suffix_version_tag);
	REQUIRE(suffix_bytes == content.substr(FILE_SIZE - suffix_bytes.size()));
	REQUIRE(TotalCachedBytes(db_instance.GetExternalFileCache()) == 4096);

	REQUIRE(ReadFull(*handle, 32, FILE_SIZE - 32) == content.substr(FILE_SIZE - 32));
	REQUIRE(recording_fs->TakeReads().empty());
	REQUIRE(recording_fs->stats_count == 0);
}

TEST_CASE("Repeated suffix reads reuse cached bytes", "[external_file_cache]") {
	DuckDB db(nullptr);
	auto &db_instance = *db.instance;
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET GLOBAL external_file_cache_remote_max_block_size=4096"));

	const idx_t FILE_SIZE = 8192 + 123;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_suffix_reuse.bin", content);
	const auto remote_path = CACHING_TEST_REMOTE_PREFIX + test_file.GetPath();
	auto recording_fs = make_uniq<SuffixTrackingFileSystem>();

	CachingFileSystem cfs(*recording_fs, db_instance);
	for (idx_t read = 0; read < 2; read++) {
		auto handle = cfs.OpenFile(MakeDeferredOpenFileInfo(remote_path), ReaderSizedFlags());
		FileBufferHandleGroup suffix;
		SuffixReadResult result;
		REQUIRE(handle->TryReadSuffix(8, suffix, result));
		auto suffix_bytes = ReadSuffixTail(suffix, result, 8);
		REQUIRE(suffix_bytes == content.substr(FILE_SIZE - suffix_bytes.size()));
	}
	REQUIRE(recording_fs->suffix_requests == vector<idx_t> {4096});
}

TEST_CASE("Suffix cache publication ignores partially covered blocks", "[external_file_cache]") {
	DuckDB db(nullptr);
	auto &db_instance = *db.instance;
	Connection con(db);
	const idx_t BLOCK_SIZE = 2 * 1024 * 1024;
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("SET GLOBAL external_file_cache_remote_max_block_size=%llu", BLOCK_SIZE)));

	const idx_t FILE_SIZE = 2 * BLOCK_SIZE + 100;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_suffix_overlapping_block.bin", content);
	const auto remote_path = CACHING_TEST_REMOTE_PREFIX + test_file.GetPath();
	auto recording_fs = make_uniq<SuffixTrackingFileSystem>();

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeDeferredOpenFileInfo(remote_path), ReaderSizedFlags());
	const auto suffix_start = FILE_SIZE - BLOCK_SIZE;
	REQUIRE(ReadFull(*handle, BLOCK_SIZE, suffix_start - 100) == content.substr(suffix_start - 100, BLOCK_SIZE));

	FileBufferHandleGroup suffix;
	SuffixReadResult result;
	REQUIRE(handle->TryReadSuffix(8, suffix, result));
	auto suffix_bytes = ReadSuffixTail(suffix, result, 8);
	REQUIRE(suffix_bytes == content.substr(FILE_SIZE - suffix_bytes.size()));
}

TEST_CASE("Suffix reads discard responses from an obsolete cache generation", "[external_file_cache]") {
	DuckDB db(nullptr);
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	const string content_a(8192, 'A');
	const string content_b(16384, 'B');
	EFCTestFileGuard test_file("test_efc_suffix_generation.bin", content_a);
	const auto remote_path = CACHING_TEST_REMOTE_PREFIX + test_file.GetPath();
	auto blocking_fs = make_uniq<BlockingSuffixFileSystem>();

	CachingFileSystem cfs(*blocking_fs, db_instance);
	auto handle = cfs.OpenFile(MakeDeferredOpenFileInfo(remote_path), ReaderSizedFlags());
	bool suffix_succeeded = true;
	std::thread reader([&]() {
		FileBufferHandleGroup suffix;
		SuffixReadResult result;
		suffix_succeeded = handle->TryReadSuffix(8, suffix, result);
	});

	blocking_fs->WaitForSuffix();
	cache.SetEnabled(false);
	WriteTestContent(test_file.GetPath(), content_b);
	cache.SetEnabled(true);
	blocking_fs->ReleaseSuffix();
	reader.join();

	REQUIRE_FALSE(suffix_succeeded);
	FileBufferHandleGroup suffix;
	SuffixReadResult result;
	REQUIRE(handle->TryReadSuffix(8, suffix, result));
	REQUIRE(result.metadata.file_size == NumericCast<int64_t>(content_b.size()));
	auto suffix_bytes = ReadSuffixTail(suffix, result, 8);
	REQUIRE(suffix_bytes == string(8, 'B'));
}

TEST_CASE("Parquet suffix metadata retains the version that supplied its bytes", "[external_file_cache]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET parquet_metadata_cache=true"));
	REQUIRE_NO_FAIL(con.Query("SET validate_external_file_cache='VALIDATE_REMOTE'"));

	TemporaryParquetFile file_a(TestCreatePath("test_efc_suffix_version_a.parquet"));
	TemporaryParquetFile file_b(TestCreatePath("test_efc_suffix_version_b.parquet"));
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT i FROM range(100000) t(i)) TO '%s' (FORMAT parquet)", file_a.path)));
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT i FROM range(200000) t(i)) TO '%s' (FORMAT parquet)", file_b.path)));

	auto versioned_fs = make_uniq<VersionedSuffixFileSystem>();
	auto versioned_fs_ptr = versioned_fs.get();
	versioned_fs_ptr->SetCurrentPath(file_a.path);
	versioned_fs_ptr->BlockSuffix();
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(versioned_fs));

	const auto remote_path = VersionedSuffixFileSystem::Prefix() + "test.parquet";
	auto pending = std::async(std::launch::async, [&]() {
		return con.Query(StringUtil::Format("SELECT count(*) FROM read_parquet('%s')", remote_path));
	});
	REQUIRE(versioned_fs_ptr->WaitForSuffix());
	db.instance->GetExternalFileCache().SetEnabled(false);
	versioned_fs_ptr->SetCurrentPath(file_b.path);
	db.instance->GetExternalFileCache().SetEnabled(true);
	versioned_fs_ptr->ReleaseSuffix();

	auto first_result = pending.get();
	REQUIRE_NO_FAIL(*first_result);
	REQUIRE(CHECK_COLUMN(first_result, 0, {200000}));
	auto second_result = con.Query(StringUtil::Format("SELECT count(*) FROM read_parquet('%s')", remote_path));
	REQUIRE_NO_FAIL(*second_result);
	REQUIRE(CHECK_COLUMN(second_result, 0, {200000}));
}

TEST_CASE("Parquet suffix reads widen sub-footer cache blocks", "[external_file_cache]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET GLOBAL external_file_cache_remote_max_block_size=4"));

	TemporaryParquetFile file(TestCreatePath("test_efc_suffix_minimum.parquet"));
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT i FROM range(42) t(i)) TO '%s' (FORMAT parquet)", file.path)));

	auto versioned_fs = make_uniq<VersionedSuffixFileSystem>();
	auto versioned_fs_ptr = versioned_fs.get();
	versioned_fs_ptr->SetCurrentPath(file.path);
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(versioned_fs));

	const auto remote_path = VersionedSuffixFileSystem::Prefix() + "minimum.parquet";
	auto result = con.Query(StringUtil::Format("SELECT count(*) FROM read_parquet('%s')", remote_path));
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(versioned_fs_ptr->GetSuffixRequests() == 1);
}

TEST_CASE("Parquet reuses cached footer bytes without metadata caching", "[external_file_cache]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET parquet_metadata_cache=false"));
	REQUIRE_NO_FAIL(con.Query("SET validate_external_file_cache='NO_VALIDATION'"));

	TemporaryParquetFile file(TestCreatePath("test_efc_suffix_footer_reuse.parquet"));
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT i FROM range(100000) t(i)) TO '%s' (FORMAT parquet)", file.path)));

	auto versioned_fs = make_uniq<VersionedSuffixFileSystem>();
	auto versioned_fs_ptr = versioned_fs.get();
	versioned_fs_ptr->SetCurrentPath(file.path);
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(versioned_fs));

	const auto remote_path = VersionedSuffixFileSystem::Prefix() + "footer-reuse.parquet";
	for (idx_t query = 0; query < 2; query++) {
		auto result = con.Query(StringUtil::Format("SELECT count(*) FROM read_parquet('%s')", remote_path));
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(result, 0, {100000}));
	}
	REQUIRE(versioned_fs_ptr->GetSuffixRequests() == 1);
	REQUIRE(versioned_fs_ptr->GetReadRequests() == 0);
}

TEST_CASE("Parquet execution reader reuses suffix metadata without a file info request", "[external_file_cache]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET parquet_metadata_cache=false"));
	REQUIRE_NO_FAIL(con.Query("SET validate_external_file_cache='VALIDATE_REMOTE'"));

	TemporaryParquetFile file(TestCreatePath("test_efc_suffix_metadata_seed.parquet"));
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT i FROM range(100000) t(i)) TO '%s' (FORMAT parquet)", file.path)));

	auto versioned_fs = make_uniq<VersionedSuffixFileSystem>();
	auto versioned_fs_ptr = versioned_fs.get();
	versioned_fs_ptr->SetCurrentPath(file.path);
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(versioned_fs));

	const auto remote_path = VersionedSuffixFileSystem::Prefix() + "metadata-seed.parquet";
	auto result = con.Query(StringUtil::Format("SELECT sum(i) FROM read_parquet('%s')", remote_path));
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(4999950000)}));
	REQUIRE(versioned_fs_ptr->GetSuffixRequests() == 1);
	REQUIRE(versioned_fs_ptr->GetStatsRequests() == 0);
	REQUIRE(versioned_fs_ptr->GetFileSizeRequests() == 0);
	REQUIRE(versioned_fs_ptr->GetLastModifiedRequests() == 0);
	REQUIRE(versioned_fs_ptr->GetVersionTagRequests() == 0);
}

TEST_CASE("Cached Parquet suffix metadata initializes later data reads", "[external_file_cache]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET parquet_metadata_cache=false"));
	REQUIRE_NO_FAIL(con.Query("SET validate_external_file_cache='NO_VALIDATION'"));

	TemporaryParquetFile file(TestCreatePath("test_efc_cached_suffix_metadata.parquet"));
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT i FROM range(100000) t(i)) TO '%s' (FORMAT parquet)", file.path)));

	auto versioned_fs = make_uniq<VersionedSuffixFileSystem>();
	auto versioned_fs_ptr = versioned_fs.get();
	versioned_fs_ptr->SetCurrentPath(file.path);
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(versioned_fs));

	const auto remote_path = VersionedSuffixFileSystem::Prefix() + "cached-metadata.parquet";
	auto footer_result = con.Query(StringUtil::Format("SELECT count(*) FROM read_parquet('%s')", remote_path));
	REQUIRE_NO_FAIL(*footer_result);
	REQUIRE(CHECK_COLUMN(footer_result, 0, {100000}));

	auto data_result = con.Query(StringUtil::Format("SELECT sum(i) FROM read_parquet('%s')", remote_path));
	REQUIRE_NO_FAIL(*data_result);
	REQUIRE(CHECK_COLUMN(data_result, 0, {Value::BIGINT(4999950000)}));
	REQUIRE(versioned_fs_ptr->GetSuffixRequests() == 1);
	REQUIRE(versioned_fs_ptr->GetStatsRequests() == 0);
	REQUIRE(versioned_fs_ptr->GetFileSizeRequests() == 0);
	REQUIRE(versioned_fs_ptr->GetLastModifiedRequests() == 0);
	REQUIRE(versioned_fs_ptr->GetVersionTagRequests() == 0);
}

TEST_CASE("Uncached suffix reads preserve the reader request size", "[external_file_cache]") {
	DuckDB db(nullptr);
	auto &db_instance = *db.instance;
	db_instance.GetExternalFileCache().SetEnabled(false);

	const idx_t FILE_SIZE = 8192 + 123;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_suffix_uncached.bin", content);
	const auto remote_path = CACHING_TEST_REMOTE_PREFIX + test_file.GetPath();
	auto recording_fs = make_uniq<SuffixTrackingFileSystem>();

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeDeferredOpenFileInfo(remote_path), ReaderSizedFlags());
	FileBufferHandleGroup suffix;
	SuffixReadResult result;
	REQUIRE(handle->TryReadSuffix(8, suffix, result));

	REQUIRE(recording_fs->suffix_requests == vector<idx_t> {8});
	REQUIRE(recording_fs->stats_count == 0);
	REQUIRE(result.bytes_read == 8);
	REQUIRE(TotalCachedBytes(db_instance.GetExternalFileCache()) == 0);
}

TEST_CASE("Unsupported suffix reads retain lazy metadata fallback", "[external_file_cache]") {
	DuckDB db(nullptr);
	auto &db_instance = *db.instance;

	const idx_t FILE_SIZE = 8192 + 123;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_suffix_fallback.bin", content);
	const auto remote_path = CACHING_TEST_REMOTE_PREFIX + test_file.GetPath();
	auto recording_fs = make_uniq<SuffixTrackingFileSystem>();
	recording_fs->suffix_supported = false;

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeDeferredOpenFileInfo(remote_path), ReaderSizedFlags());
	FileBufferHandleGroup suffix;
	SuffixReadResult result;
	REQUIRE_FALSE(handle->TryReadSuffix(8, suffix, result));
	REQUIRE(recording_fs->stats_count == 0);

	REQUIRE(handle->GetFileSize() == FILE_SIZE);
	REQUIRE(recording_fs->stats_count == 1);
}

TEST_CASE("Uncached reads are split at the cache block size", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 4096;
	const idx_t FILE_SIZE = BLOCK_SIZE * 3 + 100;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE));
	auto &cache = db_instance.GetExternalFileCache();
	cache.SetEnabled(false);

	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_uncached_split_reads.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);

	// The same reads as with the cache enabled, but nothing is kept
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads() ==
	        vector<pair<idx_t, idx_t>> {
	            {0, BLOCK_SIZE}, {BLOCK_SIZE, BLOCK_SIZE}, {2 * BLOCK_SIZE, BLOCK_SIZE}, {3 * BLOCK_SIZE, 100}});
	REQUIRE(CountCachedBlocks(cache) == 0);

	// A read of at most one block stays a single read
	REQUIRE(ReadFull(*handle, BLOCK_SIZE, 100) == content.substr(100, BLOCK_SIZE));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{100, BLOCK_SIZE}});

	// Without async threads, worker threads still run the pieces in parallel, so the read is still split
	REQUIRE(!con.Query("SET threads=4")->HasError());
	REQUIRE(!con.Query("SET async_threads=0")->HasError());
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads().size() == 4);

	// Without any other thread the pieces cannot run in parallel, so the read stays whole
	REQUIRE(!con.Query("SET threads=1")->HasError());
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{0, FILE_SIZE}});
}

TEST_CASE("Reads of files the cache does not handle are not split", "[external_file_cache]") {
	DuckDB db(nullptr);
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 4096;
	const idx_t FILE_SIZE = BLOCK_SIZE * 3 + 100;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE));

	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_local_unsplit_reads.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);

	// Local files are not cached by default, so their reads stay whole
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{0, FILE_SIZE}});
}

TEST_CASE("A handle can still read after its caching file system is destroyed", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t FILE_SIZE = 65536;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_handle_outlives_fs.bin", content);

	// Readers own their caching file system, but read-ahead tasks can keep the handle alive longer
	auto cfs = make_uniq<CachingFileSystem>(*recording_fs, db_instance);
	auto handle = cfs->OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	cfs.reset();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	db_instance.GetExternalFileCache().SetEnabled(false);
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
}

TEST_CASE("Compressed remote files are read in chunks of up to the file size", "[external_file_cache]") {
	DuckDB db(nullptr);
	Connection con(db);
	EFCTestFileGuard test_file("test_compressed_read_chunks.csv.gz", "");
	REQUIRE(!con.Query(StringUtil::Format("COPY (SELECT i, md5(i::VARCHAR) AS h FROM range(20000) t(i)) TO '%s' "
	                                      "(FORMAT csv, COMPRESSION gzip)",
	                                      test_file.GetPath()))
	             ->HasError());

	GZipFileSystem gzip_fs;
	SimpleTrackingFileSystem tracking_fs;
	auto open_compressed = [&](const string &path) {
		auto child = tracking_fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		return gzip_fs.OpenCompressedFile(*con.context, std::move(child), false);
	};
	auto read_all = [](FileHandle &handle) {
		string result;
		vector<char> buffer(65536);
		int64_t read;
		while ((read = handle.Read(buffer.data(), buffer.size())) > 0) {
			result.append(buffer.data(), NumericCast<idx_t>(read));
		}
		return result;
	};

	SECTION("local files keep the buffer size of the compression") {
		auto local = open_compressed(test_file.GetPath());
		REQUIRE(local->Cast<CompressedFile>().stream_data.in_buf_size == gzip_fs.InBufferSize());
	}
	SECTION("remote files smaller than a chunk are read whole") {
		auto local = open_compressed(test_file.GetPath());
		auto remote = open_compressed(CACHING_TEST_REMOTE_PREFIX + test_file.GetPath());
		REQUIRE(remote->GetFileSize() > gzip_fs.InBufferSize());
		REQUIRE(remote->Cast<CompressedFile>().stream_data.in_buf_size == remote->GetFileSize());
		REQUIRE(read_all(*remote) == read_all(*local));
	}
	SECTION("the buffers count toward the memory limit") {
		auto &buffer_manager = BufferManager::GetBufferManager(*db.instance);
		const auto used_before = buffer_manager.GetUsedMemory();
		auto remote = open_compressed(CACHING_TEST_REMOTE_PREFIX + test_file.GetPath());
		auto &stream_data = remote->Cast<CompressedFile>().stream_data;
		REQUIRE(buffer_manager.GetUsedMemory() >= used_before + stream_data.in_buf_size + stream_data.out_buf_size);
		remote.reset();
		REQUIRE(buffer_manager.GetUsedMemory() == used_before);
	}
}

TEST_CASE("A read spanning cached ranges only fetches the gaps between them", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	// larger than the default local block size, so the file is not fetched whole
	const idx_t FILE_SIZE = 20480;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_fill_gaps.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, 4096, 0) == content.substr(0, 4096));
	REQUIRE(ReadFull(*handle, 4096, 16384) == content.substr(16384, 4096));
	recording_fs->TakeReads();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{4096, 12288}});
	REQUIRE(CountCachedBlocks(cache) == 3);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);
}

TEST_CASE("Small cached ranges between gaps are fetched with the gaps", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 65536;
	const idx_t ABSORB_SIZE = BLOCK_SIZE / 8;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE));

	const idx_t FILE_SIZE = 100000;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_absorb.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, 4096, 8192) == content.substr(8192, 4096));
	REQUIRE(ReadFull(*handle, ABSORB_SIZE, 20480) == content.substr(20480, ABSORB_SIZE));
	recording_fs->TakeReads();

	// the 4096 byte range is fetched again with its gaps, the range of exactly ABSORB_SIZE is kept
	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	const idx_t after = 20480 + ABSORB_SIZE;
	REQUIRE(recording_fs->TakeReads() ==
	        vector<pair<idx_t, idx_t>> {
	            {0, 20480}, {after, BLOCK_SIZE}, {after + BLOCK_SIZE, FILE_SIZE - after - BLOCK_SIZE}});
	REQUIRE(CountCachedBlocks(cache) == 4);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads().empty());
}

TEST_CASE("Small cached ranges are kept when fetching them with the gaps saves no request", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 65536;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE));

	// the gaps around the cached range need one request each, merged they would still need two
	const idx_t FILE_SIZE = 8192 + 4096 + 61440;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_absorb_no_saving.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, 4096, 8192) == content.substr(8192, 4096));
	recording_fs->TakeReads();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{0, 8192}, {12288, 61440}});
	REQUIRE(CountCachedBlocks(cache) == 3);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);
}

TEST_CASE("Cached ranges of local files between gaps are kept at the default block size", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	// larger than the default local block size, so the file is not fetched whole
	const idx_t FILE_SIZE = 20480;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_keep_local.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, 4096, 4096) == content.substr(4096, 4096));
	REQUIRE(ReadFull(*handle, 4096, 12288) == content.substr(12288, 4096));
	recording_fs->TakeReads();

	REQUIRE(ReadFull(*handle, FILE_SIZE) == content);
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{0, 4096}, {8192, 4096}, {16384, 4096}});
	REQUIRE(CountCachedBlocks(cache) == 5);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);
}

TEST_CASE("A file no larger than the block size is fetched whole on the first read", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t FILE_SIZE = 3000;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_small_file.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto &cache = db_instance.GetExternalFileCache();

	REQUIRE(ReadFull(*handle, 100, 2800) == content.substr(2800, 100));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{0, FILE_SIZE}});
	REQUIRE(CountCachedBlocks(cache) == 1);
	REQUIRE(TotalCachedBytes(cache) == FILE_SIZE);

	// Any later read of the file is served from the cache
	REQUIRE(ReadFull(*handle, 500, 100) == content.substr(100, 500));
	REQUIRE(recording_fs->TakeReads().empty());
}

TEST_CASE("Reads of remote files are rounded out to the minimum blocks around them", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto remote_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t MAX_BLOCK_SIZE = 16384;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_remote_max_block_size=%llu", MAX_BLOCK_SIZE));
	const idx_t MIN_BLOCK_SIZE = 4096;
	const idx_t FILE_SIZE = 16 * MIN_BLOCK_SIZE + 100;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_min_block.bin", content);

	CachingFileSystem cfs(*remote_fs, db_instance);
	auto handle =
	    cfs.OpenFile(MakeTestOpenFileInfo(CACHING_TEST_REMOTE_PREFIX + test_file.GetPath()), ReaderSizedFlags());

	REQUIRE(ReadFull(*handle, 1, 5000) == content.substr(5000, 1));
	REQUIRE(remote_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{MIN_BLOCK_SIZE, MIN_BLOCK_SIZE}});

	REQUIRE(ReadFull(*handle, 10, 6000) == content.substr(6000, 10));
	REQUIRE(remote_fs->TakeReads().empty());

	REQUIRE(ReadFull(*handle, 100, 2 * MIN_BLOCK_SIZE - 50) == content.substr(2 * MIN_BLOCK_SIZE - 50, 100));
	REQUIRE(remote_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{2 * MIN_BLOCK_SIZE, MIN_BLOCK_SIZE}});

	const idx_t long_read = 5 * MIN_BLOCK_SIZE + 10;
	REQUIRE(ReadFull(*handle, MAX_BLOCK_SIZE + 1, long_read) == content.substr(long_read, MAX_BLOCK_SIZE + 1));
	REQUIRE(remote_fs->TakeReads() ==
	        vector<pair<idx_t, idx_t>> {{5 * MIN_BLOCK_SIZE, MAX_BLOCK_SIZE}, {9 * MIN_BLOCK_SIZE, MIN_BLOCK_SIZE}});

	REQUIRE(ReadFull(*handle, 10, FILE_SIZE - 10) == content.substr(FILE_SIZE - 10, 10));
	REQUIRE(remote_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{16 * MIN_BLOCK_SIZE, 100}});
}

TEST_CASE("Short reads of local files are not widened", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t FILE_SIZE = 65636;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_local_short.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());

	REQUIRE(ReadFull(*handle, 1, 5000) == content.substr(5000, 1));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{5000, 1}});
}

TEST_CASE("Reads sized by the cache fetch the cache blocks around them", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();
	auto &cache = db_instance.GetExternalFileCache();

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
	const idx_t FILE_SIZE = 3 * BLOCK_SIZE + 50;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_cache_sized.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);

	REQUIRE(ReadFull(*handle, 100, 0) == content.substr(0, 100));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{0, BLOCK_SIZE}});

	for (idx_t offset = 100; offset + 1000 <= BLOCK_SIZE; offset += 1000) {
		REQUIRE(ReadFull(*handle, 1000, offset) == content.substr(offset, 1000));
	}
	REQUIRE(recording_fs->TakeReads().empty());

	REQUIRE(ReadFull(*handle, 100, BLOCK_SIZE) == content.substr(BLOCK_SIZE, 100));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{BLOCK_SIZE, BLOCK_SIZE}});

	REQUIRE(ReadFull(*handle, 100, 2 * BLOCK_SIZE + 100) == content.substr(2 * BLOCK_SIZE + 100, 100));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{2 * BLOCK_SIZE, BLOCK_SIZE}});

	REQUIRE(ReadFull(*handle, 10, FILE_SIZE - 10) == content.substr(FILE_SIZE - 10, 10));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{3 * BLOCK_SIZE, 50}});
	REQUIRE(CountCachedBlocks(cache) == 4);
}

TEST_CASE("Reads past the end of the file fetch only the bytes in it", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();
	auto &cache = db_instance.GetExternalFileCache();

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
	const idx_t FILE_SIZE = 2 * BLOCK_SIZE + 50;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_past_end.bin", content);

	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);

	auto group = handle->Read(100, FILE_SIZE - 10);
	REQUIRE(group.GetHandles().size() == 1);
	REQUIRE(group.GetHandles()[0].length == 10);
	string tail(10, '\0');
	group.CopyTo(reinterpret_cast<data_ptr_t>(&tail[0]), 10);
	REQUIRE(tail == content.substr(FILE_SIZE - 10));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{2 * BLOCK_SIZE, 50}});

	REQUIRE(handle->Read(10, FILE_SIZE).GetHandles().empty());
	REQUIRE(recording_fs->TakeReads().empty());
	REQUIRE(CountCachedBlocks(cache) == 1);
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

	const idx_t block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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
				con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", bs));
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

	const auto block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const auto block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const auto content = MakeTestContent(cache.GetCacheMaxBlockSize(missing_a));
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

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

TEST_CASE("An evicted block held by an in-flight read is fetched once", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	Connection con(db);
	REQUIRE(!con.Query("SET threads=1")->HasError());
	REQUIRE(!con.Query("SET async_threads=0")->HasError());
	const idx_t BLOCK_SIZE = 2097152;
	const idx_t SMALL_READ = 4096;
	REQUIRE(
	    !con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE))->HasError());

	auto content = MakeTestContent(BLOCK_SIZE * 3);
	EFCTestFileGuard test_file("test_efc_held_evicted_block.bin", content);
	auto blocking_fs = make_uniq<LocationBlockingFileSystem>();
	CachingFileSystem cfs(*blocking_fs, *db.instance);
	auto handle_a = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	auto handle_b = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);

	REQUIRE(ReadFull(*handle_b, 100, BLOCK_SIZE) == content.substr(BLOCK_SIZE, 100));
	REQUIRE(blocking_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{BLOCK_SIZE, BLOCK_SIZE}});

	blocking_fs->BlockReadsAt(BLOCK_SIZE - SMALL_READ);
	string result_a;
	std::thread reader_a([&]() { result_a = ReadFull(*handle_a, SMALL_READ + 100, BLOCK_SIZE - SMALL_READ); });
	blocking_fs->WaitForBlockedRead();
	auto evict = con.Query("SET memory_limit='2MB'");
	auto restore = con.Query("SET memory_limit='1GB'");
	auto result_b = ReadFull(*handle_b, 100, BLOCK_SIZE);
	blocking_fs->ReleaseReads();
	reader_a.join();

	REQUIRE(!evict->HasError());
	REQUIRE(!restore->HasError());
	REQUIRE(result_a == content.substr(BLOCK_SIZE - SMALL_READ, SMALL_READ + 100));
	REQUIRE(result_b == content.substr(BLOCK_SIZE, 100));
	REQUIRE(blocking_fs->TakeReads() ==
	        vector<pair<idx_t, idx_t>> {{BLOCK_SIZE - SMALL_READ, SMALL_READ}, {BLOCK_SIZE, BLOCK_SIZE}});
}

TEST_CASE("Content response can prohibit cache reuse", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto policy_fs = make_uniq<CachePolicyFileSystem>();

	const idx_t block_size = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

	const idx_t BLOCK_SIZE = cache.GetCacheMaxBlockSize(TestDirectoryPath());
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

TEST_CASE("Dropping cache blocks preserves existing readers and starts a new generation", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &cache = db.instance->GetExternalFileCache();
	auto cached_file = cache.GetOrCreateCachedFile("drop-blocks-test");

	constexpr idx_t BLOCK_SIZE = 4096;
	constexpr idx_t LOCATION = 7 * BLOCK_SIZE;
	auto acquired = cache.AcquireBlocks(*cached_file, LOCATION, 2 * BLOCK_SIZE, BLOCK_SIZE);
	REQUIRE(acquired.size() == 2);
	{
		const annotated_lock_guard<annotated_mutex> guard(acquired[0]->mtx);
		acquired[0]->state = CacheBlockState::LOADED;
		acquired[0]->nr_bytes = BLOCK_SIZE;
	}
	const auto generation = cache.GetContentGeneration(*cached_file);

	cache.DropBlocks(*cached_file);
	REQUIRE(cache.GetContentGeneration(*cached_file) == generation + 1);

	// Existing readers retain the blocks they hold
	{
		const annotated_lock_guard<annotated_mutex> guard(acquired[0]->mtx);
		REQUIRE(acquired[0]->state == CacheBlockState::LOADED);
		REQUIRE(acquired[0]->nr_bytes == BLOCK_SIZE);
	}
	// Future readers receive fresh blocks
	auto replacements = cache.AcquireBlocks(*cached_file, LOCATION, 2 * BLOCK_SIZE, BLOCK_SIZE);
	REQUIRE(replacements.size() == 2);
	REQUIRE(replacements[0] != acquired[0]);
	REQUIRE(replacements[1] != acquired[1]);
}

TEST_CASE("Request sizing can make every read cover the aligned blocks around it", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 4096;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE));
	con.Query("SET external_file_cache_request_sizing='GRID'");

	const idx_t FILE_SIZE = 5 * BLOCK_SIZE;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_request_sizing_grid.bin", content);

	// a reader that sizes its own requests reads whole blocks
	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), ReaderSizedFlags());
	recording_fs->TakeReads();

	REQUIRE(ReadFull(*handle, 100, BLOCK_SIZE + 10) == content.substr(BLOCK_SIZE + 10, 100));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{BLOCK_SIZE, BLOCK_SIZE}});
}

TEST_CASE("Request sizing can make every read cover exactly the requested bytes", "[external_file_cache]") {
	DuckDB db = MakeCacheLocalFilesDB();
	auto &db_instance = *db.instance;
	auto recording_fs = make_uniq<EFCTrackingFileSystem>();

	const idx_t BLOCK_SIZE = 4096;
	Connection con(db);
	con.Query(StringUtil::Format("SET external_file_cache_local_max_block_size=%llu", BLOCK_SIZE));
	con.Query("SET external_file_cache_request_sizing='EXACT'");

	const idx_t FILE_SIZE = 5 * BLOCK_SIZE;
	auto content = MakeTestContent(FILE_SIZE);
	EFCTestFileGuard test_file("test_efc_request_sizing_exact.bin", content);

	// a reader sized by the cache reads exactly the requested bytes
	CachingFileSystem cfs(*recording_fs, db_instance);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	recording_fs->TakeReads();

	REQUIRE(ReadFull(*handle, 100, BLOCK_SIZE + 10) == content.substr(BLOCK_SIZE + 10, 100));
	REQUIRE(recording_fs->TakeReads() == vector<pair<idx_t, idx_t>> {{BLOCK_SIZE + 10, 100}});
}

} // namespace duckdb
