#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

//! Serves off-disk://<name> from a local file as a file off local disk would be served, and records the reads
class OffDiskFileSystem : public LocalFileSystem {
public:
	//! Resolve the test directory on the test's own thread: the query reads on other threads
	OffDiskFileSystem() : local_prefix(TestCreatePath("off_disk_")) {
	}

	static const string &Prefix() {
		static const string prefix = "off-disk://";
		return prefix;
	}

	string MapPath(const string &path) const {
		if (!StringUtil::StartsWith(path, Prefix())) {
			return path;
		}
		return local_prefix + path.substr(Prefix().size());
	}

	string GetName() const override {
		return "OffDiskFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, Prefix());
	}

	bool IsLocalFileSystem() const override {
		return false;
	}

	bool OnDiskFile(FileHandle &handle) override {
		return on_disk;
	}

	bool FileExists(const string &path, optional_ptr<FileOpener> opener) override {
		return LocalFileSystem::FileExists(MapPath(path), opener);
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override {
		if (!CanHandleFile(path)) {
			return LocalFileSystem::Glob(path, opener);
		}
		vector<OpenFileInfo> result;
		if (LocalFileSystem::FileExists(MapPath(path), opener)) {
			result.emplace_back(path);
		}
		return result;
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		return LocalFileSystem::OpenFile(MapPath(path), flags, opener);
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		{
			annotated_lock_guard<annotated_mutex> guard(reads_lock);
			reads.emplace_back(location, NumericCast<idx_t>(nr_bytes));
		}
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	vector<pair<idx_t, idx_t>> TakeReads() {
		annotated_lock_guard<annotated_mutex> guard(reads_lock);
		auto result = std::move(reads);
		reads.clear();
		std::sort(result.begin(), result.end());
		return result;
	}

	//! Whether the files count as on local disk, where requests cost no round trip
	bool on_disk = false;

private:
	const string local_prefix;
	annotated_mutex reads_lock;
	vector<pair<idx_t, idx_t>> reads DUCKDB_GUARDED_BY(reads_lock);
};

} // namespace

TEST_CASE("A small row group of a file off disk that the cache does not serve is read in one request", "[parquet]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto off_disk_fs = make_uniq<OffDiskFileSystem>();
	auto &fs = *off_disk_fs;
	const auto file = OffDiskFileSystem::Prefix() + "wide.parquet";
	const auto local_path = fs.MapPath(file);
	// one row group of 20 columns of about 16 KB each: the three columns read lie far apart
	string columns;
	for (idx_t i = 0; i < 20; i++) {
		columns += StringUtil::Format("%shash(i + %llu) AS c%02llu", i ? ", " : "", i, i);
	}
	REQUIRE_NO_FAIL(con.Query(
	    StringUtil::Format("COPY (SELECT %s FROM range(2000) t(i)) TO '%s' (FORMAT parquet, COMPRESSION uncompressed)",
	                       columns, local_path)));
	auto &local_fs = TestLocalFileSystem();
	const auto file_size =
	    NumericCast<idx_t>(local_fs.GetFileSize(*local_fs.OpenFile(local_path, FileFlags::FILE_FLAGS_READ)));
	REQUIRE(file_size > 20 * 2000 * 8);
	REQUIRE(file_size < 2097152);
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(off_disk_fs));

	// the gaps between c00, c10 and c19 are about 144 KB, the one between c00 and c19 alone about 288 KB
	string sums = "SELECT sum(c00), sum(c10), sum(c19) FROM read_parquet('%s')";
	idx_t data_reads = 0;
	SECTION("the row group is read in one request") {
		data_reads = 1;
	}
	SECTION("columns further apart than the widened gap are read apart") {
		sums = "SELECT sum(c00), sum(c19) FROM read_parquet('%s')";
		data_reads = 2;
	}
	SECTION("a pinned gap is kept") {
		REQUIRE_NO_FAIL(con.Query("SET parquet_prefetch_column_gap = 16384"));
		data_reads = 3;
	}
	SECTION("a row group larger than a cache block is read column by column") {
		REQUIRE_NO_FAIL(con.Query("SET external_file_cache_remote_max_block_size = 65536"));
		data_reads = 3;
	}
	SECTION("a file on local disk is read column by column") {
		fs.on_disk = true;
		data_reads = 3;
	}
	auto expected = con.Query(StringUtil::Format(sums, local_path));
	REQUIRE(!expected->HasError());
	auto result = con.Query(StringUtil::Format(sums, file));
	REQUIRE(!result->HasError());
	REQUIRE(result->Equals(*expected));
	// the footer, read once to bind the scan, then the column chunks' reads
	auto reads = fs.TakeReads();
	REQUIRE(reads.size() == 1 + data_reads);
	REQUIRE(reads.back().first + reads.back().second == file_size);
}
