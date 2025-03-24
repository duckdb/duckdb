#include "catch.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// This test isn't really necessary, but CachingFileSystem is only used in extensions (for now),
// so this test ensures that it is built properly (Windows CI was giving issues).
TEST_CASE("Test external file cache", "[external_file_cache][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto fs = CachingFileSystem::Get(*con.context);

	const string file_path = "data/parquet-testing/simple.parquet";
	auto fh = fs.OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
	REQUIRE(fh->GetPath() == file_path);
	REQUIRE(fh->GetFileSize() > 0);
	REQUIRE(fh->GetLastModifiedTime() != 0);
	REQUIRE(fh->CanSeek());
	REQUIRE(fh->OnDiskFile());
}
