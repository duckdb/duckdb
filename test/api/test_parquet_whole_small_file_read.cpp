#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "test_helpers.hpp"

#include <cstring>

using namespace duckdb;

namespace {

//! Serves off-disk://<name> from a local file as a file off local disk would be served, and records the reads
class OffDiskFileSystem : public LocalFileSystem {
public:
	//! Resolve the test directory on the test's own thread: the query reads on other threads
	OffDiskFileSystem() : local_prefix(TestCreatePath("whole_file_")) {
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
			reads[handle.GetPath()].emplace_back(location, NumericCast<idx_t>(nr_bytes));
		}
		LocalFileSystem::Read(handle, buffer, nr_bytes, location);
	}

	//! The (offset, length) of every read since the last call, in file order, by the local path of the file
	map<string, vector<pair<idx_t, idx_t>>> TakeReads() {
		annotated_lock_guard<annotated_mutex> guard(reads_lock);
		auto result = std::move(reads);
		reads.clear();
		for (auto &entry : result) {
			std::sort(entry.second.begin(), entry.second.end());
		}
		return result;
	}

	//! Whether the files count as on local disk, where requests cost no round trip
	bool on_disk = false;

private:
	const string local_prefix;
	annotated_mutex reads_lock;
	map<string, vector<pair<idx_t, idx_t>>> reads DUCKDB_GUARDED_BY(reads_lock);
};

idx_t LocalFileSize(const string &path) {
	auto &local_fs = TestLocalFileSystem();
	return NumericCast<idx_t>(local_fs.GetFileSize(*local_fs.OpenFile(path, FileFlags::FILE_FLAGS_READ)));
}

//! How many bytes of the file [reads] cover, counting each byte once; [reads] is in file order
idx_t BytesCovered(const vector<pair<idx_t, idx_t>> &reads) {
	idx_t covered = 0;
	idx_t end = 0;
	for (auto &read : reads) {
		const auto read_end = read.first + read.second;
		if (read_end > end) {
			covered += read_end - MaxValue(read.first, end);
			end = read_end;
		}
	}
	return covered;
}

//! The length of the footer, as the last 8 bytes of the file give it
idx_t FooterLength(const string &path) {
	auto &local_fs = TestLocalFileSystem();
	auto handle = local_fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	char tail[8];
	local_fs.Read(*handle, tail, 8, LocalFileSize(path) - 8);
	uint32_t length;
	memcpy(&length, tail, sizeof(length));
	return length;
}

//! Write [rows] rows from row [first]: a sorted key i, a low-cardinality d (even values only) and [columns] random
//! columns c0, c1, ...
void WriteFile(Connection &con, const string &path, idx_t first, idx_t rows, idx_t columns,
               const string &options = "") {
	string select = "i, (i % 10) * 2 AS d";
	for (idx_t c = 0; c < columns; c++) {
		select += StringUtil::Format(", hash(i * %llu + %llu) AS c%llu", columns, c, c);
	}
	// one SELECT per 200 rows, so that a row group size of 200 gives several row groups
	vector<string> parts;
	for (idx_t start = first; start < first + rows; start += 200) {
		parts.push_back(StringUtil::Format("SELECT %s FROM range(%llu, %llu) t(i)", select, start,
		                                   MinValue<idx_t>(start + 200, first + rows)));
	}
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("COPY (%s) TO '%s' (FORMAT parquet, COMPRESSION uncompressed%s)",
	                                             StringUtil::Join(parts, " UNION ALL "), path, options)));
}

//! The schema of WriteFile's files, pinned as a reader that knows it passes it: the bind then opens no file
string PinnedSchema(idx_t columns) {
	string schema = "schema := map {'i': {name: 'i', type: 'BIGINT', default_value: NULL}, "
	                "'d': {name: 'd', type: 'BIGINT', default_value: NULL}";
	for (idx_t c = 0; c < columns; c++) {
		schema += StringUtil::Format(", 'c%llu': {name: 'c%llu', type: 'UBIGINT', default_value: NULL}", c, c);
	}
	return schema + "}";
}

//! [query] with %s for the source, run over the off-disk files and over the same files read locally
struct ScanCheck {
	string query;
	string source;
	string options;

	string Remote() const {
		return StringUtil::Format(query, source, options);
	}
	string Local(const OffDiskFileSystem &fs) const {
		return StringUtil::Replace(Remote(), OffDiskFileSystem::Prefix(), fs.MapPath(OffDiskFileSystem::Prefix()));
	}
};

//! Run [check] over the off-disk files, compare its answer with a local read, and return the reads it sent
map<string, vector<pair<idx_t, idx_t>>> RunCheck(Connection &con, OffDiskFileSystem &fs, const ScanCheck &check) {
	auto expected = con.Query(check.Local(fs));
	REQUIRE(!expected->HasError());
	fs.TakeReads();
	auto result = con.Query(check.Remote());
	REQUIRE(!result->HasError());
	REQUIRE(result->Equals(*expected));
	return fs.TakeReads();
}

} // namespace

TEST_CASE("A small file off disk that the cache does not serve is read in one request", "[parquet]") {
	DuckDB db(nullptr);
	Connection con(db);
	// several threads scan the row groups of a file, all through the one handle that holds it
	REQUIRE_NO_FAIL(con.Query("SET threads = 4"));
	auto off_disk_fs = make_uniq<OffDiskFileSystem>();
	auto &fs = *off_disk_fs;
	vector<string> files;
	for (idx_t f = 0; f < 3; f++) {
		files.push_back(StringUtil::Format("%ssmall%llu.parquet", OffDiskFileSystem::Prefix(), f));
		// three row groups of small pages, with a page index and bloom filters: more than the footer prefetch, at
		// most the bound. Each file holds rows of its own.
		WriteFile(con, fs.MapPath(files.back()), f * 600, 600, 6,
		          ", ROW_GROUP_SIZE 200, WRITE_PAGE_INDEX true, DATA_PAGE_SIZE_LIMIT 512");
		const auto size = LocalFileSize(fs.MapPath(files.back()));
		REQUIRE(size > 16384);
		REQUIRE(size <= CachingFileHandle::WHOLE_FILE_READ_MAXIMUM_SIZE);
	}
	// a file the footer prefetch reads whole already
	const auto tiny_file = OffDiskFileSystem::Prefix() + "tiny.parquet";
	WriteFile(con, fs.MapPath(tiny_file), 0, 100, 6);
	REQUIRE(LocalFileSize(fs.MapPath(tiny_file)) <= 16384);
	const auto &file = files[0];
	auto metadata = con.Query(StringUtil::Format(
	    "SELECT count(DISTINCT row_group_id), bool_or(path_in_schema = 'i' AND column_index_offset IS NOT NULL), "
	    "bool_or(path_in_schema = 'd' AND bloom_filter_offset IS NOT NULL) FROM parquet_metadata('%s')",
	    fs.MapPath(file)));
	REQUIRE(CHECK_COLUMN(metadata, 0, {3}));
	REQUIRE(CHECK_COLUMN(metadata, 1, {true}));
	REQUIRE(CHECK_COLUMN(metadata, 2, {true}));
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(off_disk_fs));

	ScanCheck check {"SELECT sum(c0), sum(c3), sum(c5) FROM read_parquet(%s%s)", "'" + file + "'",
	                 ", " + PinnedSchema(6)};
	vector<string> read_files {file};
	// whether the bind opens the first file for the schema, which then reads only the footer of it
	bool bind_opens_first_file = false;
	SECTION("a scan of the file") {
	}
	SECTION("a scan that reads no column") {
		check.query = "SELECT count(*) FROM read_parquet(%s%s)";
	}
	SECTION("a filter that bloom filters rule out") {
		check.query = "SELECT count(*), sum(c1) FROM read_parquet(%s%s) WHERE d = 5";
	}
	SECTION("a filter that statistics and the page index narrow") {
		check.query = "SELECT count(*), sum(c1) FROM read_parquet(%s%s) WHERE i BETWEEN 250 AND 260";
	}
	SECTION("a footer size given with the file") {
		check.source = StringUtil::Format("[{'filename': '%s', 'footer_size': %llu::UBIGINT}]", file,
		                                  FooterLength(fs.MapPath(file)));
	}
	SECTION("several files") {
		check.source = "['" + StringUtil::Join(files, "', '") + "']";
		read_files = files;
	}
	SECTION("several files, the first of which the bind opens for the schema") {
		check.source = "['" + StringUtil::Join(files, "', '") + "']";
		check.options = "";
		read_files = files;
		bind_opens_first_file = true;
	}
	SECTION("the page index function") {
		check.query = "SELECT count(*), sum(num_rows), sum(compressed_page_size) FROM parquet_page_index(%s)%s";
		check.options = "";
	}
	SECTION("the bloom filter probe function") {
		check.query = "SELECT count(*), sum(bloom_filter_excludes::INTEGER) FROM parquet_bloom_probe(%s, 'd', 5)%s";
		check.options = "";
	}
	SECTION("a file of at most 16 KiB") {
		check.source = "'" + tiny_file + "'";
		read_files = {tiny_file};
	}

	auto reads = RunCheck(con, fs, check);
	REQUIRE(reads.size() == read_files.size());
	for (idx_t f = 0; f < read_files.size(); f++) {
		const auto local_path = fs.MapPath(read_files[f]);
		const auto size = LocalFileSize(local_path);
		auto &file_reads = reads[local_path];
		if (f == 0 && bind_opens_first_file) {
			// the bind read the footer, and the scan reads the ranges it needs, as for a file larger than the bound
			REQUIRE(file_reads.size() > 1);
			for (auto &read : file_reads) {
				REQUIRE(read.second < size);
			}
		} else {
			// the footer's request read the whole file, and nothing else was requested
			REQUIRE(file_reads == vector<pair<idx_t, idx_t>> {{0, size}});
		}
	}
}

TEST_CASE("A small file is read in ranges when it is not read whole with its footer", "[parquet]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto off_disk_fs = make_uniq<OffDiskFileSystem>();
	auto &fs = *off_disk_fs;
	vector<string> files;
	for (idx_t f = 0; f < 2; f++) {
		files.push_back(StringUtil::Format("%ssmall%llu.parquet", OffDiskFileSystem::Prefix(), f));
		WriteFile(con, fs.MapPath(files.back()), f * 600, 600, 6);
		const auto size = LocalFileSize(fs.MapPath(files.back()));
		REQUIRE(size > 16384);
		REQUIRE(size <= CachingFileHandle::WHOLE_FILE_READ_MAXIMUM_SIZE);
	}
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(off_disk_fs));

	ScanCheck check {"SELECT sum(c0), sum(c3), sum(c5) FROM read_parquet(%s%s)", "'" + files[0] + "'",
	                 ", " + PinnedSchema(6)};
	vector<string> read_files {files[0]};
	SECTION("the footer is cached") {
		REQUIRE_NO_FAIL(con.Query("SET parquet_metadata_cache = true"));
		// the second scan uses the footer the first one cached, however recently the file was written
		REQUIRE_NO_FAIL(con.Query("SET validate_external_file_cache = 'NO_VALIDATION'"));
		REQUIRE_NO_FAIL(con.Query(check.Remote()));
	}
	SECTION("the file is larger than the bound") {
		check.source = "'" + OffDiskFileSystem::Prefix() + "large.parquet'";
		read_files = {OffDiskFileSystem::Prefix() + "large.parquet"};
		WriteFile(con, fs.MapPath(read_files[0]), 0, 4000, 6);
		REQUIRE(LocalFileSize(fs.MapPath(read_files[0])) > CachingFileHandle::WHOLE_FILE_READ_MAXIMUM_SIZE);
	}
	SECTION("the file is on local disk") {
		fs.on_disk = true;
	}
	SECTION("prefetching is disabled") {
		REQUIRE_NO_FAIL(con.Query("SET disable_parquet_prefetching = true"));
	}
	SECTION("the external file cache serves the file") {
		REQUIRE_NO_FAIL(con.Query("SET cache_local_files = true"));
	}
	SECTION("the bind opens the file for the schema") {
		check.options = "";
	}
	SECTION("the bind opens every file for union_by_name") {
		check.source = "['" + StringUtil::Join(files, "', '") + "']";
		check.options = ", union_by_name = true";
		read_files = files;
	}
	SECTION("a metadata function that needs only the footer") {
		check.query = "SELECT count(*), sum(num_values) FROM parquet_metadata(%s)%s";
		check.options = "";
	}

	auto reads = RunCheck(con, fs, check);
	REQUIRE(reads.size() == read_files.size());
	for (auto &read_file : read_files) {
		const auto local_path = fs.MapPath(read_file);
		const auto size = LocalFileSize(local_path);
		auto &file_reads = reads[local_path];
		REQUIRE(!file_reads.empty());
		for (auto &read : file_reads) {
			REQUIRE(read.second < size);
		}
		// nor was it read in pieces: together the reads leave part of the file unread, which a read of the whole file
		// that the cache splits into blocks would not
		REQUIRE(BytesCovered(file_reads) < size);
	}
}

TEST_CASE("Small files read whole under a small memory limit give the same answers", "[parquet]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto off_disk_fs = make_uniq<OffDiskFileSystem>();
	auto &fs = *off_disk_fs;
	vector<string> files;
	for (idx_t f = 0; f < 200; f++) {
		files.push_back(StringUtil::Format("%smany%llu.parquet", OffDiskFileSystem::Prefix(), f));
		WriteFile(con, fs.MapPath(files.back()), f * 600, 600, 6);
	}
	FileSystem::GetFileSystem(*con.context).RegisterSubSystem(std::move(off_disk_fs));
	// the files opened ahead of the scan would hold more than the limit if they could not be evicted: those the
	// buffer manager evicts are read in ranges instead
	REQUIRE_NO_FAIL(con.Query("SET memory_limit = '16MB'"));
	REQUIRE_NO_FAIL(con.Query("SET threads = 8"));
	ScanCheck check {"SELECT count(*), sum(c0), sum(c3), sum(c5) FROM read_parquet(%s%s)",
	                 "['" + StringUtil::Join(files, "', '") + "']", ", " + PinnedSchema(6)};
	auto reads = RunCheck(con, fs, check);
	REQUIRE(reads.size() == files.size());
}
