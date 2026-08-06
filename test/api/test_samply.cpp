#include "catch.hpp"
#include "duckdb/main/profiler/samply.hpp"
#include "test_helpers.hpp"
#include "utf8proc_wrapper.hpp"

#include <fstream>

using namespace duckdb;

TEST_CASE("Samply markers are written in marker-file format", "[api][samply]") {
	REQUIRE(SamplyQueryMarkerName("  SELECT\n  42;\t") == "Query: SELECT 42;");
	REQUIRE(SamplyQueryMarkerName(" \n\t ") == "Query: <empty>");

	auto long_query = StringUtil::Repeat("x", 501);
	REQUIRE(SamplyQueryMarkerName(long_query) == "Query: " + StringUtil::Repeat("x", 499) + "…");
	auto unicode_query_name = SamplyQueryMarkerName(StringUtil::Repeat("é", 450));
	REQUIRE(Utf8Proc::IsValid(unicode_query_name.c_str(), unicode_query_name.size()));
	REQUIRE(StringUtil::EndsWith(unicode_query_name, "…"));

	auto directory = TestCreatePath("samply_markers");
	TestCreateDirectory(directory);

	SamplyMarkerWriter writer(directory.c_str());
	REQUIRE(writer.WriteMarker(10, 20, SamplyQueryMarkerName("SELECT 42;").c_str()));
	REQUIRE(writer.WriteMarker(30, 40, "Pipeline 2: TABLE_SCAN[source] → HASH_JOIN[build]"));

	string path = writer.GetPath();
	REQUIRE(StringUtil::Contains(path, "/marker-"));
	REQUIRE(StringUtil::EndsWith(path, ".txt"));

	std::ifstream marker_file(path);
	string contents((std::istreambuf_iterator<char>(marker_file)), std::istreambuf_iterator<char>());
	REQUIRE(contents == "10 20 Query: SELECT 42;\n30 40 Pipeline 2: TABLE_SCAN[source] → HASH_JOIN[build]\n");
}
