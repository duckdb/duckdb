#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

//! Serves <prefix><name> from a local file, but cannot tell the modification time of a file
class NoModifiedTimeFileSystem : public LocalFileSystem {
public:
	NoModifiedTimeFileSystem(string prefix_p, string local_prefix_p)
	    : prefix(std::move(prefix_p)), local_prefix(std::move(local_prefix_p)) {
	}

	string MapPath(const string &path) const {
		if (!StringUtil::StartsWith(path, prefix)) {
			return path;
		}
		return local_prefix + path.substr(prefix.size());
	}

	string GetName() const override {
		return "NoModifiedTimeFileSystem";
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
		return LocalFileSystem::OpenFile(MapPath(path), local_flags, opener);
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return FileSystem::GetLastModifiedTime(handle);
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

private:
	string prefix;
	string local_prefix;
};

} // namespace

TEST_CASE("read_blob returns NULL as the modification time of a file system that cannot tell it", "[api]") {
	// a local path is read through the metadata of the file, a remote one through the opened file
	auto prefix = GENERATE(string("no-modified-time://"), string("gs://no-modified-time/"));

	DuckDB db(nullptr);
	Connection con(db);
	auto local_prefix = TestCreatePath("no_modified_time_");
	REQUIRE_NO_FAIL(
	    con.Query(StringUtil::Format("COPY (SELECT 'hello' AS s) TO '%sfile.csv' (HEADER false)", local_prefix)));
	FileSystem::GetFileSystem(*con.context)
	    .RegisterSubSystem(make_uniq<NoModifiedTimeFileSystem>(prefix, local_prefix));

	auto file = prefix + "file.csv";
	auto result = con.Query(StringUtil::Format("SELECT size, last_modified FROM read_blob('%s')", file));
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	result = con.Query(StringUtil::Format("SELECT content, last_modified IS NULL FROM read_blob('%s')", file));
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BLOB("hello\n")}));
	REQUIRE(CHECK_COLUMN(result, 1, {true}));
}
