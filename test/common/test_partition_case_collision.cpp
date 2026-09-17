#include "catch.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

const char *const CASE_PAIR = "(SELECT * FROM (VALUES ('a', 1), ('A', 2)) v(k, x))";

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

	//! Whether the real file system under this directory folds case (macOS/Windows, but not Linux)
	bool FoldsCase() {
		auto probe = fs.JoinPath(path, "case_probe_dir");
		fs.CreateDirectory(probe);
		const bool folds = fs.DirectoryExists(fs.JoinPath(path, "CASE_PROBE_DIR"));
		fs.RemoveDirectory(probe);
		return folds;
	}

	LocalFileSystem fs;
	string path;
};

//! Resolves each path component to an existing entry that differs only in case, so that the guard is
//! exercised on case-sensitive platforms as well
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
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::FileExists(Fold(filename), opener);
	}
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::DirectoryExists(Fold(directory), opener);
	}
	bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::CreateDirectoryExtended(Fold(directory), options, opener);
	}
	bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::RemoveDirectoryExtended(Fold(directory), options, opener);
	}
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		return LocalFileSystem::ListFiles(Fold(directory), callback, opener);
	}

private:
	string Fold(const string &path) {
		if (path.size() <= root.size() || path.compare(0, root.size(), root) != 0) {
			return path;
		}
		auto resolved = root;
		for (auto &part : StringUtil::Split(StringUtil::Replace(path.substr(root.size()), "\\", "/"), '/')) {
			if (part.empty()) {
				continue;
			}
			auto exact = JoinPath(resolved, part);
			string match;
			if (!LocalFileSystem::DirectoryExists(exact) && !LocalFileSystem::FileExists(exact)) {
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

//! Makes the case probe throw, so the collision check has to fall back to its default
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

template <class FILE_SYSTEM, class... ARGS>
duckdb::unique_ptr<DuckDB> DatabaseOn(DBConfig &config, ARGS &&... args) {
	config.file_system = make_uniq<VirtualFileSystem>(make_uniq<FILE_SYSTEM>(std::forward<ARGS>(args)...));
	return make_uniq<DuckDB>(nullptr, &config);
}

string PartitionCopy(const string &source, const string &out, const string &columns) {
	return "COPY " + source + " TO '" + out + "' (FORMAT parquet, PARTITION_BY (" + columns + "), OVERWRITE_OR_IGNORE)";
}

void RequireCaseCollision(duckdb::unique_ptr<MaterializedQueryResult> result) {
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "only in case"));
}

int64_t CountRows(Connection &con, const string &out) {
	auto result = con.Query("SELECT count(*) FROM read_parquet('" + out + "/**/*.parquet')");
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<int64_t>();
}

} // namespace

TEST_CASE("Partitioned COPY rejects partition values that differ only in case", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_collision");
	DBConfig config;
	auto db = DatabaseOn<CaseFoldingFileSystem>(config, dir.path);
	Connection con(*db);

	// two values within one COPY that resolve to the same directory
	RequireCaseCollision(con.Query(PartitionCopy(CASE_PAIR, dir.Child("out"), "k")));

	// a directory left behind by an earlier COPY, at either partition level
	auto nested = dir.Child("nested");
	REQUIRE_NO_FAIL(con.Query(PartitionCopy("(SELECT 'p' AS g, 'a' AS k, 1 AS x)", nested, "g, k")));
	RequireCaseCollision(con.Query(PartitionCopy("(SELECT 'P' AS g, 'a' AS k, 2 AS x)", nested, "g, k")));
	RequireCaseCollision(con.Query(PartitionCopy("(SELECT 'p' AS g, 'A' AS k, 3 AS x)", nested, "g, k")));
}

TEST_CASE("Partitioned COPY keeps writing partitions that do not collide", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_no_collision");
	DBConfig config;
	auto db = DatabaseOn<CaseFoldingFileSystem>(config, dir.path);
	Connection con(*db);

	auto out = dir.Child("out");
	auto sql = PartitionCopy("(SELECT * FROM (VALUES ('a', 1), ('Zed', 2)) v(k, x))", out, "k");
	REQUIRE_NO_FAIL(con.Query(sql));
	REQUIRE_NO_FAIL(con.Query(sql));
	REQUIRE(CountRows(con, out) == 2);
}

TEST_CASE("Partitioned COPY allows a case collision under a unique filename pattern", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_uuid");
	DBConfig config;
	auto db = DatabaseOn<CaseFoldingFileSystem>(config, dir.path);
	Connection con(*db);

	auto out = dir.Child("out");
	REQUIRE_NO_FAIL(con.Query("COPY " + string(CASE_PAIR) + " TO '" + out +
	                          "' (FORMAT parquet, PARTITION_BY (k), FILENAME_PATTERN '{uuid}')"));
	REQUIRE(CountRows(con, out) == 2);
}

TEST_CASE("Partitioned COPY keeps the collision check when the case probe fails", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_probe_failure");
	DBConfig config;
	auto db = DatabaseOn<ProbeFailingFileSystem>(config);
	Connection con(*db);

	auto out = dir.Child("out");
	auto sql = PartitionCopy(CASE_PAIR, out, "k");
	auto result = con.Query(sql);
	if (dir.FoldsCase()) {
		RequireCaseCollision(std::move(result));
		return;
	}
	// case-sensitive host: both directories exist, so the still-enabled check must not fire on a re-run
	REQUIRE_NO_FAIL(*result);
	REQUIRE_NO_FAIL(con.Query(sql));
	REQUIRE(CountRows(con, out) == 2);
}

TEST_CASE("Partitioned COPY handles case-only partition values on the host file system", "[partition_case_collision]") {
	PartitionCaseTestDirectory dir("partition_case_host_fs");
	DuckDB db(nullptr);
	Connection con(db);

	auto out = dir.Child("out");
	auto result = con.Query(PartitionCopy(CASE_PAIR, out, "k"));
	if (dir.FoldsCase()) {
		RequireCaseCollision(std::move(result));
		return;
	}
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CountRows(con, out) == 2);
}
