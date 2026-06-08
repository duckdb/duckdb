#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "test_helpers.hpp"

namespace duckdb {

namespace {

class ExternalCacheTestFileGuard {
public:
	ExternalCacheTestFileGuard(const string &filename, const string &content) : file_path(TestCreatePath(filename)) {
		WriteContent(content);
	}

	~ExternalCacheTestFileGuard() {
		auto local_fs = FileSystem::CreateLocal();
		local_fs->TryRemoveFile(file_path);
	}

	const string &GetPath() const {
		return file_path;
	}

	void WriteContent(const string &content) const {
		auto local_fs = FileSystem::CreateLocal();
		auto handle = local_fs->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		handle->Write(QueryContext(), const_cast<char *>(content.data()), content.size(), 0);
		handle->Sync();
	}

private:
	string file_path;
};

OpenFileInfo MakeTestOpenFileInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	return info;
}

string ReadFull(CachingFileHandle &handle, idx_t size, idx_t offset = 0) {
	data_ptr_t data;
	auto pin = handle.Read(data, size, offset);
	return string(reinterpret_cast<const char *>(data), size);
}

void EvictObjectCache(ObjectCache &object_cache) {
	const auto memory = object_cache.GetCurrentMemory();
	REQUIRE(memory > 0);
	REQUIRE(object_cache.EvictToReduceMemory(memory) > 0);
}

} // namespace

TEST_CASE("Disabled external file cache does not insert into ObjectCache", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();
	auto &object_cache = db_instance.GetObjectCache();

	const string content(16384, 'A');
	ExternalCacheTestFileGuard test_file("test_efc_disabled.bin", content);

	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem cfs(*local_fs, db_instance);

	cache.SetEnabled(false);
	REQUIRE_FALSE(cache.IsEnabled());
	REQUIRE(cache.GetCachedFileCount() == 0);
	REQUIRE(object_cache.GetCurrentMemory() == 0);

	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, content.size()) == content);
	}

	REQUIRE(cache.GetCachedFileCount() == 0);
	REQUIRE(cache.GetCachedFileInformation().empty());
	REQUIRE(object_cache.GetCurrentMemory() == 0);

	cache.SetEnabled(true);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, content.size()) == content);
	}
	REQUIRE(cache.GetCachedFileCount() == 1);
	REQUIRE(object_cache.GetCurrentMemory() > 0);
}

TEST_CASE("Re-enabled external file cache refreshes live handle metadata", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();

	const string content_a(64, 'A');
	const string content_b(128, 'B');
	ExternalCacheTestFileGuard test_file("test_efc_reenabled_live_handle_metadata.bin", content_a);

	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem cfs(*local_fs, db_instance);

	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(handle->GetFileSize() == content_a.size());
	REQUIRE(cache.GetCachedFileCount() == 1);

	cache.SetEnabled(false);
	REQUIRE(cache.GetCachedFileCount() == 0);
	test_file.WriteContent(content_b);

	cache.SetEnabled(true);
	REQUIRE(handle->GetFileSize() == content_b.size());
	REQUIRE(cache.GetCachedFileCount() == 1);
}

TEST_CASE("Disabling external file cache clears ObjectCache sentinels", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();
	auto &object_cache = db_instance.GetObjectCache();

	const string content(16384, 'A');
	ExternalCacheTestFileGuard test_file("test_efc_object_cache_disable.bin", content);

	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem cfs(*local_fs, db_instance);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, content.size()) == content);
	}

	REQUIRE(cache.GetCachedFileCount() == 1);
	REQUIRE(object_cache.GetCurrentMemory() > 0);

	cache.SetEnabled(false);
	REQUIRE(cache.GetCachedFileInformation().empty());
	REQUIRE(cache.GetCachedFileCount() == 0);
	REQUIRE(object_cache.GetCurrentMemory() == 0);

	cache.SetEnabled(true);
	auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadFull(*handle, content.size()) == content);
	REQUIRE(cache.GetCachedFileCount() == 1);
}

TEST_CASE("Failed CachingFileHandle construction leaves evictable cached file entries", "[external_file_cache]") {
	DuckDB db(":memory:");
	auto &db_instance = *db.instance;
	auto &cache = db_instance.GetExternalFileCache();
	auto &object_cache = db_instance.GetObjectCache();

	auto local_fs = FileSystem::CreateLocal();
	CachingFileSystem cfs(*local_fs, db_instance);

	const auto missing_a = TestCreatePath("test_efc_missing_a.bin");
	const auto missing_b = TestCreatePath("test_efc_missing_b.bin");
	local_fs->TryRemoveFile(missing_a);
	local_fs->TryRemoveFile(missing_b);

	REQUIRE_THROWS(cfs.OpenFile(MakeTestOpenFileInfo(missing_a), FileFlags::FILE_FLAGS_READ));
	REQUIRE_THROWS(cfs.OpenFile(MakeTestOpenFileInfo(missing_b), FileFlags::FILE_FLAGS_READ));

	REQUIRE(cache.GetCachedFileCount() == 2);

	const string content(16384, 'A');
	ExternalCacheTestFileGuard test_file("test_efc_missing_a.bin", content);
	{
		auto handle = cfs.OpenFile(MakeTestOpenFileInfo(test_file.GetPath()), FileFlags::FILE_FLAGS_READ);
		REQUIRE(ReadFull(*handle, content.size()) == content);
	}
	REQUIRE(cache.GetCachedFileCount() == 2);

	EvictObjectCache(object_cache);
	REQUIRE(cache.GetCachedFileCount() == 0);
}

} // namespace duckdb
