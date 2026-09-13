#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct PartitionCaseTestDirectory {
	explicit PartitionCaseTestDirectory(const string &suffix) : path(TestCreatePath(suffix)) {
		TestDeleteDirectory(path);
		fs.CreateDirectory(path);
	}
	~PartitionCaseTestDirectory() {
		TestDeleteDirectory(path);
	}

	string Child(const string &name) {
		return fs.JoinPath(path, name);
	}

	bool FoldsCase() {
		auto lower_probe = fs.JoinPath(path, "case_probe_dir");
		auto upper_probe = fs.JoinPath(path, "CASE_PROBE_DIR");
		fs.CreateDirectory(lower_probe);
		const bool folds = fs.DirectoryExists(upper_probe);
		fs.RemoveDirectory(lower_probe);
		return folds;
	}

	LocalFileSystem fs;
	string path;
};

class CaseFoldingFileSystem : public LocalFileSystem {
public:
	explicit CaseFoldingFileSystem(string root_p) : root(std::move(root_p)) {
	}

	string GetName() const override {
		return "CaseFoldingFileSystem";
	}

	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::OpenFile(Fold(path), flags, opener);
	}
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override {
		LocalFileSystem::MoveFile(Fold(source), Fold(target), opener);
	}
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::DirectoryExists(Fold(directory), opener);
	}
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		LocalFileSystem::CreateDirectory(Fold(directory), opener);
	}
	bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::CreateDirectoryExtended(Fold(directory), options, opener);
	}
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		LocalFileSystem::RemoveDirectory(Fold(directory), opener);
	}
	bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::RemoveDirectoryExtended(Fold(directory), options, opener);
	}
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		return LocalFileSystem::ListFiles(Fold(directory), callback, opener);
	}
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::FileExists(Fold(filename), opener);
	}
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		LocalFileSystem::RemoveFile(Fold(filename), opener);
	}
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::TryRemoveFile(Fold(filename), opener);
	}

private:
	static duckdb::vector<string> SplitPath(const string &suffix) {
		duckdb::vector<string> parts;
		string current;
		for (auto c : suffix) {
			if (c == '/' || c == '\\') {
				if (!current.empty()) {
					parts.push_back(current);
					current.clear();
				}
			} else {
				current += c;
			}
		}
		if (!current.empty()) {
			parts.push_back(current);
		}
		return parts;
	}

	string Fold(const string &path) {
		if (path.size() <= root.size() || path.compare(0, root.size(), root) != 0) {
			return path;
		}
		auto resolved = root;
		for (auto &part : SplitPath(path.substr(root.size()))) {
			auto exact = JoinPath(resolved, part);
			if (LocalFileSystem::DirectoryExists(exact) || LocalFileSystem::FileExists(exact)) {
				resolved = exact;
				continue;
			}
			string match;
			if (LocalFileSystem::DirectoryExists(resolved)) {
				LocalFileSystem::ListFiles(resolved, [&](const string &name, bool) {
					if (match.empty() && StringUtil::CIEquals(name, part)) {
						match = name;
					}
				});
			}
			resolved = match.empty() ? exact : JoinPath(resolved, match);
		}
		return resolved;
	}

	string root;
};

class ProbeFailingFileSystem : public LocalFileSystem {
public:
	string GetName() const override {
		return "ProbeFailingFileSystem";
	}

	bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener = nullptr) override {
		if (StringUtil::Contains(directory, "duckdb_case_probe_")) {
			throw IOException("probe directories are not supported by this file system");
		}
		return LocalFileSystem::CreateDirectoryExtended(directory, options, opener);
	}
};

duckdb::unique_ptr<DuckDB> ProbeFailingDatabase(DBConfig &config) {
	config.file_system = make_uniq<VirtualFileSystem>(make_uniq<ProbeFailingFileSystem>());
	return make_uniq<DuckDB>(nullptr, &config);
}

duckdb::unique_ptr<DuckDB> CaseFoldingDatabase(DBConfig &config, const string &root) {
	config.file_system = make_uniq<VirtualFileSystem>(make_uniq<CaseFoldingFileSystem>(root));
	return make_uniq<DuckDB>(nullptr, &config);
}

int64_t CountRows(Connection &con, const string &out) {
	auto result = con.Query("SELECT count(*) FROM read_parquet('" + out + "/**/*.parquet')");
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<int64_t>();
}

string PartitionCopy(const string &table, const string &out, const string &columns) {
	return "COPY " + table + " TO '" + out + "' (FORMAT parquet, PARTITION_BY (" + columns + "), OVERWRITE_OR_IGNORE)";
}

} // namespace

TEST_CASE("Partitioned COPY handles partition values that differ only in case", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_collision");
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('A', 2)) v(k, x)"));

	auto out = dir.Child("out");
	auto copy_sql = PartitionCopy("t", out, "k");
	auto result = con.Query(copy_sql);

	if (dir.FoldsCase()) {
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
		return;
	}

	REQUIRE_NO_FAIL(*result);
	REQUIRE(dir.fs.DirectoryExists(dir.fs.JoinPath(out, "k=a")));
	REQUIRE(dir.fs.DirectoryExists(dir.fs.JoinPath(out, "k=A")));
	REQUIRE(CountRows(con, out) == 2);

	REQUIRE_NO_FAIL(con.Query(copy_sql));
	REQUIRE(CountRows(con, out) == 2);

	REQUIRE_NO_FAIL(con.Query("COPY t TO '" + out + "' (FORMAT parquet, PARTITION_BY (k), OVERWRITE)"));
	REQUIRE(CountRows(con, out) == 2);
}

TEST_CASE("Partitioned COPY handles a partition directory written by an earlier COPY", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_earlier_copy");
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lower_only AS SELECT 'a' AS k, 1 AS x"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE upper_only AS SELECT 'A' AS k, 2 AS x"));

	auto out = dir.Child("out");
	REQUIRE_NO_FAIL(con.Query(PartitionCopy("lower_only", out, "k")));
	auto result = con.Query(PartitionCopy("upper_only", out, "k"));

	if (dir.FoldsCase()) {
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
		REQUIRE(CountRows(con, out) == 1);
		return;
	}

	REQUIRE_NO_FAIL(*result);
	REQUIRE(CountRows(con, out) == 2);
	auto labels = con.Query("SELECT count(*) FROM read_parquet('" + out +
	                        "/**/*.parquet', hive_partitioning = true) WHERE (k = 'a') = (x = 1)");
	REQUIRE_NO_FAIL(*labels);
	REQUIRE(labels->GetValue(0, 0).GetValue<int64_t>() == 2);
}

TEST_CASE("Partitioned COPY handles a pre-created partition directory that differs only in case",
          "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_precreated");
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE upper_only AS SELECT 'A' AS k, 2 AS x"));

	auto out = dir.Child("out");
	dir.fs.CreateDirectory(out);
	dir.fs.CreateDirectory(dir.fs.JoinPath(out, "k=a"));

	auto result = con.Query(PartitionCopy("upper_only", out, "k"));

	if (dir.FoldsCase()) {
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
		return;
	}

	REQUIRE_NO_FAIL(*result);
	REQUIRE(dir.fs.DirectoryExists(dir.fs.JoinPath(out, "k=A")));
	REQUIRE(CountRows(con, out) == 1);
}

TEST_CASE("Partitioned COPY handles a case collision in a second partition column", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_multi_column");
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT * FROM (VALUES ('p', 'a', 1), ('p', 'A', 2)) v(g, k, x)"));

	auto out = dir.Child("out");
	auto copy_sql = PartitionCopy("t", out, "g, k");
	auto result = con.Query(copy_sql);

	if (dir.FoldsCase()) {
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
		return;
	}

	REQUIRE_NO_FAIL(*result);
	REQUIRE(dir.fs.DirectoryExists(dir.fs.JoinPath(dir.fs.JoinPath(out, "g=p"), "k=a")));
	REQUIRE(dir.fs.DirectoryExists(dir.fs.JoinPath(dir.fs.JoinPath(out, "g=p"), "k=A")));
	REQUIRE(CountRows(con, out) == 2);

	REQUIRE_NO_FAIL(con.Query(copy_sql));
	REQUIRE(CountRows(con, out) == 2);
}

TEST_CASE("Partitioned COPY accepts partition values that do not collide", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_no_collision");
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE u AS SELECT * FROM (VALUES ('a', 1), ('b', 2), ('Zed', 3)) v(k, x)"));

	auto out = dir.Child("out");
	auto copy_sql = PartitionCopy("u", out, "k");
	REQUIRE_NO_FAIL(con.Query(copy_sql));
	REQUIRE(CountRows(con, out) == 3);

	REQUIRE_NO_FAIL(con.Query(copy_sql));
	REQUIRE(CountRows(con, out) == 3);

	auto result = con.Query("SELECT count(*) FROM read_parquet('" + out +
	                        "/**/*.parquet', hive_partitioning = true) WHERE k = 'Zed'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 1);
}

TEST_CASE("Partitioned COPY handles NULL partition values", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_null_values");
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT * FROM (VALUES ('a', NULL, 1), ('b', 'q', 2), "
	                          "(NULL, NULL, 3)) v(g, k, x)"));

	auto out = dir.Child("out");
	auto copy_sql = PartitionCopy("t", out, "g, k");
	REQUIRE_NO_FAIL(con.Query(copy_sql));
	REQUIRE(CountRows(con, out) == 3);

	REQUIRE_NO_FAIL(con.Query(copy_sql));
	REQUIRE(CountRows(con, out) == 3);
}

TEST_CASE("Partitioned COPY allows a case collision under a unique filename pattern", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_uuid");
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('A', 2)) v(k, x)"));

	auto out = dir.Child("out");
	REQUIRE_NO_FAIL(con.Query("COPY t TO '" + out + "' (FORMAT parquet, PARTITION_BY (k), FILENAME_PATTERN '{uuid}')"));
	REQUIRE(CountRows(con, out) == 2);

	REQUIRE_NO_FAIL(
	    con.Query("COPY t TO '" + out + "' (FORMAT parquet, PARTITION_BY (k), FILENAME_PATTERN '{uuid}', APPEND)"));
	REQUIRE(CountRows(con, out) == 4);

	auto labels =
	    con.Query("SELECT count(DISTINCT k) FROM read_parquet('" + out + "/**/*.parquet', hive_partitioning = true)");
	REQUIRE_NO_FAIL(*labels);
	REQUIRE(labels->GetValue(0, 0).GetValue<int64_t>() == (dir.FoldsCase() ? 1 : 2));
}

TEST_CASE("Partitioned COPY rejects a case collision on a case-folding file system", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_folding_fs");
	DBConfig config;
	auto db = CaseFoldingDatabase(config, dir.path);
	Connection con(*db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('A', 2)) v(k, x)"));

	auto out = dir.Child("out");
	auto result = con.Query(PartitionCopy("t", out, "k"));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
}

TEST_CASE("Partitioned COPY rejects an earlier partition directory on a case-folding file system",
          "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_folding_fs_earlier");
	DBConfig config;
	auto db = CaseFoldingDatabase(config, dir.path);
	Connection con(*db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lower_only AS SELECT 'a' AS k, 1 AS x"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE upper_only AS SELECT 'A' AS k, 2 AS x"));

	auto out = dir.Child("out");
	REQUIRE_NO_FAIL(con.Query(PartitionCopy("lower_only", out, "k")));

	auto result = con.Query(PartitionCopy("upper_only", out, "k"));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
}

TEST_CASE("Partitioned COPY rejects an OVERWRITE that changes a partition value's case on a case-folding file system",
          "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_folding_fs_overwrite");
	DBConfig config;
	auto db = CaseFoldingDatabase(config, dir.path);
	Connection con(*db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE upper_only AS SELECT 'B' AS k, 1 AS x"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lower_only AS SELECT 'b' AS k, 2 AS x"));

	auto out = dir.Child("out");
	REQUIRE_NO_FAIL(con.Query("COPY upper_only TO '" + out + "' (FORMAT parquet, PARTITION_BY (k), OVERWRITE)"));

	auto result = con.Query("COPY lower_only TO '" + out + "' (FORMAT parquet, PARTITION_BY (k), OVERWRITE)");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
}

TEST_CASE("Partitioned COPY detects a case collision among many concurrent partitions", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_folding_fs_concurrent");
	DBConfig config;
	auto db = CaseFoldingDatabase(config, dir.path);
	Connection con(*db);
	REQUIRE_NO_FAIL(con.Query("SET threads=8"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE seeds AS SELECT 'seed' || (i % 10) AS k, i AS x FROM range(200) r(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE pairs AS SELECT CASE WHEN (i // 200) % 2 = 0 THEN 'v' || (i % 200) "
	                          "ELSE 'V' || (i % 200) END AS k, i AS x FROM range(400) r(i)"));

	auto out = dir.Child("out");
	REQUIRE_NO_FAIL(con.Query(PartitionCopy("seeds", out, "k")));

	auto result = con.Query(PartitionCopy("(FROM seeds UNION ALL FROM pairs)", out, "k"));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
}

TEST_CASE("Partitioned COPY keeps the collision check when the case probe fails", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_probe_failure");
	DBConfig config;
	auto db = ProbeFailingDatabase(config);
	Connection con(*db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('A', 2)) v(k, x)"));

	auto out = dir.Child("out");

	if (dir.FoldsCase()) {
		auto result = con.Query(PartitionCopy("t", out, "k"));
		REQUIRE(result->HasError());
		REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
		return;
	}

	dir.fs.CreateDirectory(out);
	dir.fs.CreateDirectory(dir.fs.JoinPath(out, "k=a"));
	dir.fs.CreateDirectory(dir.fs.JoinPath(out, "k=A"));

	REQUIRE_NO_FAIL(con.Query(PartitionCopy("t", out, "k")));
	REQUIRE(CountRows(con, out) == 2);
}
