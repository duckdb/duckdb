#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"

#ifndef _WIN32
#include <unistd.h>
#endif

using namespace duckdb;

TEST_CASE("Test temporary file owners", "[api]") {
	SECTION("a name round-trips through its owner") {
		set<string> prefixes;
		for (int64_t pid = 1; pid < 64; pid++) {
			for (idx_t instance = 0; instance < 64; instance++) {
				TemporaryFileOwner written {pid, instance};
				auto prefix = TemporaryFilePrefix(written);
				prefixes.insert(prefix);

				TemporaryFileOwner parsed;
				REQUIRE(TryParseTemporaryFileOwner(prefix + "storage_DEFAULT-0.tmp", parsed));
				REQUIRE(parsed == written);
				REQUIRE(TryParseTemporaryFileOwner(prefix + "block-7.block", parsed));
				REQUIRE(parsed == written);
				// the marker is swept like any other file of a dead owner, so it must parse too
				REQUIRE(TryParseTemporaryFileOwner(TemporaryOwnerMarkerName(written), parsed));
				REQUIRE(parsed == written);
			}
		}
		// no two owners share a prefix, so no instance can reach another's files
		REQUIRE(prefixes.size() == 63 * 64);
	}

	SECTION("names that do not carry an owner are not claimed") {
		// a version that named its files differently may still be running and using them
		TemporaryFileOwner parsed;
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_5ee2fed7-f0e1-4e7f-9399-d813d74cee53_storage_DEFAULT-0.tmp",
		                                    parsed));
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_storage_DEFAULT-0.tmp", parsed));
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_block-1.block", parsed));
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_-1_0_storage_DEFAULT-0.tmp", parsed));
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_1_ _storage_DEFAULT-0.tmp", parsed));
		REQUIRE(!TryParseTemporaryFileOwner("something_else.tmp", parsed));
	}
}

#ifndef _WIN32
namespace {

//! Makes closing the first owner markers fail
class FailingMarkerCloseFileSystem : public LocalFileSystem {
public:
	atomic<idx_t> marker_opens {0};

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		if (!IsMarker(path) || marker_opens++ >= 3) {
			return LocalFileSystem::OpenFile(path, flags, opener);
		}
		unique_ptr<FileHandle> handle;
		auto open_file = [&]() {
			handle = LocalFileSystem::OpenFile(path, flags, opener);
		};
		marker_fd = TestOpenDescriptor(path, open_file);
		if (marker_fd == -1) {
			throw InternalException("Could not find the descriptor of \"%s\"", path);
		}
		return handle;
	}

	FileType GetFileType(FileHandle &handle) override {
		auto type = LocalFileSystem::GetFileType(handle);
		if (marker_fd != -1 && IsMarker(handle.GetPath())) {
			// the root reads the type after opening, closing the descriptor now makes the close of the handle fail
			close(marker_fd);
			marker_fd = -1;
		}
		return type;
	}

private:
	static bool IsMarker(const string &path) {
		return StringUtil::EndsWith(path, "owner");
	}

	int marker_fd = -1;
};

} // namespace

TEST_CASE("A failed close of a temporary directory marker is reported", "[api]") {
	auto fs = make_uniq<FailingMarkerCloseFileSystem>();
	auto &marker_fs = *fs;
	DBConfig config;
	config.file_system = make_uniq<VirtualFileSystem>(std::move(fs));
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto temp_directory = TestCreatePath("failed_marker_close.tmp");
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET temp_directory='" + temp_directory + "'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMP TABLE t AS SELECT i, repeat('y', 80000) AS s FROM range(400) r(i)"));
	auto result = con.Query("SET memory_limit='10MB'");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "Could not close file"));
	REQUIRE(marker_fs.marker_opens == 1);

	// the marker whose close failed is removed
	vector<string> files;
	marker_fs.ListFiles(
	    temp_directory, [&](const string &name, bool) { files.push_back(name); }, nullptr);
	REQUIRE(files.empty());
}
#endif
