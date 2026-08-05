#include "catch.hpp"
#include "duckdb/main/profiler/samply.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;

TEST_CASE("Samply markers are written in marker-file format", "[api][samply]") {
	auto directory = TestCreatePath("samply_markers");
	TestCreateDirectory(directory);

	SamplyMarkerWriter writer(directory.c_str());
	REQUIRE(writer.WriteMarker(10, 20, "query.total_time"));
	REQUIRE(writer.WriteMarker(30, 40, "Pipeline 2: TABLE_SCAN[source] → HASH_JOIN[build]"));

	string path = writer.GetPath();
	REQUIRE(StringUtil::Contains(path, "/marker-"));
	REQUIRE(StringUtil::EndsWith(path, ".txt"));

	std::ifstream marker_file(path);
	string contents((std::istreambuf_iterator<char>(marker_file)), std::istreambuf_iterator<char>());
	REQUIRE(contents == "10 20 query.total_time\n30 40 Pipeline 2: TABLE_SCAN[source] → HASH_JOIN[build]\n");
}
