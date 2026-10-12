#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

//! Serves <prefix><name> from a local file, with a modification time that is set, not implemented or failing
class ModifiedTimeFileSystem : public LocalFileSystem {
public:
	ModifiedTimeFileSystem(string prefix_p, string local_prefix_p)
	    : prefix(std::move(prefix_p)), local_prefix(std::move(local_prefix_p)) {
	}

	string MapPath(const string &path) const {
		if (!StringUtil::StartsWith(path, prefix)) {
			return path;
		}
		return local_prefix + path.substr(prefix.size());
	}

	string GetName() const override {
		return "ModifiedTimeFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, prefix);
	}

	bool FileExists(const string &path, optional_ptr<FileOpener> opener) override {
		return LocalFileSystem::FileExists(MapPath(path), opener);
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override {
		vector<OpenFileInfo> result;
		if (LocalFileSystem::FileExists(MapPath(path), opener)) {
			result.emplace_back(path);
		}
		return result;
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		// the backing file is local, so a direct read of a remote file becomes a plain one
		FileOpenFlags local_flags(flags.GetFlagsInternal() & ~FileOpenFlags::FILE_FLAGS_DIRECT_IO, flags.Lock(),
		                          flags.Compression());
		local_flags.SetCachingMode(flags.GetCachingMode());
		local_flags.SetRequestSizing(flags.GetRequestSizing());
		return LocalFileSystem::OpenFile(MapPath(path), local_flags, opener);
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		if (error_type == ExceptionType::NOT_IMPLEMENTED) {
			return FileSystem::GetLastModifiedTime(handle);
		}
		if (error_type != ExceptionType::INVALID) {
			throw Exception(error_type, "failed to get the modification time");
		}
		return last_modified;
	}

	optional<timestamp_t> TryGetLastModifiedTime(FileHandle &handle) override {
		if (unknown_by_try_get) {
			return nullopt;
		}
		return LocalFileSystem::TryGetLastModifiedTime(handle);
	}

	//! The local getters go through Stats, so they read the local metadata directly
	int64_t GetFileSize(FileHandle &handle) override {
		return LocalFileSystem::Stats(handle).file_size;
	}

	FileType GetFileType(FileHandle &handle) override {
		return LocalFileSystem::Stats(handle).file_type;
	}

	string GetVersionTag(FileHandle &handle) override {
		return LocalFileSystem::Stats(handle).version_tag;
	}

	FileMetadata Stats(FileHandle &handle) override {
		return FileSystem::Stats(handle);
	}

	optional<FileMetadata> GetStatsIfExists(const OpenFileInfo &file, optional_ptr<FileOpener> opener) override {
		return FileSystem::GetStatsIfExists(file, opener);
	}

public:
	//! INVALID returns last_modified, NOT_IMPLEMENTED is not implemented, any other type is thrown
	ExceptionType error_type = ExceptionType::INVALID;
	timestamp_t last_modified;
	//! Whether TryGetLastModifiedTime reports an unknown time without calling GetLastModifiedTime
	bool unknown_by_try_get = false;

private:
	string prefix;
	string local_prefix;
};

} // namespace

TEST_CASE("read_blob returns NULL as the modification time of a file system that cannot tell it", "[api]") {
	// a local path is read through the metadata of the file, a remote one through the opened file
	auto prefix = GENERATE(string("no-modified-time://"), string("gs://no-modified-time/"));
	auto function = GENERATE(string("read_blob"), string("read_text"));
	auto error_type = GENERATE(ExceptionType::NOT_IMPLEMENTED, ExceptionType::INVALID, ExceptionType::IO);
	auto unknown_by_try_get = GENERATE(false, true);
	CAPTURE(prefix, function, Exception::ExceptionTypeToString(error_type), unknown_by_try_get);
	const auto modified_time = Timestamp::FromEpochSeconds(1000000000);

	DuckDB db(nullptr);
	Connection con(db);
	auto local_prefix = TestCreatePath("no_modified_time_");
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT 'hello' AS s) TO '%sfile.csv' (HEADER false)", local_prefix)));
	auto file_system = make_uniq<ModifiedTimeFileSystem>(prefix, local_prefix);
	file_system->error_type = error_type;
	file_system->last_modified = modified_time;
	file_system->unknown_by_try_get = unknown_by_try_get;
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(file_system));

	auto file = prefix + "file.csv";
	auto result = con.Query(StringUtil::Format("SELECT size, last_modified, content FROM %s('%s')", function, file));
	if (error_type == ExceptionType::IO && !unknown_by_try_get) {
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "failed to get the modification time"));
		return;
	}
	auto known_time = error_type == ExceptionType::INVALID && !unknown_by_try_get;
	auto expected_time = known_time ? Value::TIMESTAMPTZ(timestamp_tz_t(modified_time)) : Value();
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {expected_time}));
	REQUIRE(CHECK_COLUMN(result, 2, {function == "read_blob" ? Value::BLOB("hello\n") : Value("hello\n")}));

	result = con.Query(StringUtil::Format("SELECT size, last_modified IS NULL FROM %s('%s')", function, file));
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {!known_time}));

	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto flags = FileFlags::FILE_FLAGS_READ;
	flags.SetCachingMode(CachingMode::CACHE_REMOTE_ONLY);
	auto handle = fs.OpenFile(file, flags);
	REQUIRE(fs.TryGetLastModifiedTime(*handle) == (known_time ? optional<timestamp_t>(modified_time) : nullopt));
}
