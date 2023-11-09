#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/local_file_system.hpp"

TEST_CASE("filesystems", "[fs]") {
	duckdb::LocalFileSystem fs;

#ifndef _WIN32
	REQUIRE(fs.IsPathAbsolute("/home/me"));
	REQUIRE(!fs.IsPathAbsolute("./me"));
	REQUIRE(!fs.IsPathAbsolute("me"));
#else
	REQUIRE(fs.IsPathAbsolute("\\\\network_drive\\filename.csv"))
	REQUIRE(fs.IsPathAbsolute("C:\\folder\\filename.csv"))
	REQUIRE(fs.IsPathAbsolute("C:/folder\\filename.csv"))
#endif
}
