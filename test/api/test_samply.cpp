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

TEST_CASE("Samply counters are buffered and flushed in counter-file format", "[api][samply]") {
	auto directory = TestCreatePath("samply_counters");
	TestCreateDirectory(directory);
	string path;
	{
		SamplyCounterWriter writer(directory.c_str());
		REQUIRE(writer.WriteClock(100, 1000));
		REQUIRE(writer.WriteClock(200, 2000));
		REQUIRE(writer.WriteSample(110, "rss", 4096));
		REQUIRE(writer.WriteSample(120, "network-rx", 50));
		REQUIRE(writer.WriteSample(120, "network-tx", 25));
		path = writer.GetPath();

		std::ifstream before_flush(path);
		string buffered_contents((std::istreambuf_iterator<char>(before_flush)), std::istreambuf_iterator<char>());
		REQUIRE(buffered_contents.empty());
	}

	std::ifstream counter_file(path);
	string contents((std::istreambuf_iterator<char>(counter_file)), std::istreambuf_iterator<char>());
	REQUIRE(contents == "clock 100 1000\n110 rss 4096\n120 network-rx 50\n120 network-tx 25\n");
}

#if defined(__linux__) || defined(__APPLE__)
TEST_CASE("Samply resources receive a final sample when deactivated", "[api][samply]") {
	auto directory = TestCreatePath("samply_final_resources");
	TestCreateDirectory(directory);
	auto tracks = static_cast<uint8_t>(SamplyTrack::MEMORY) | static_cast<uint8_t>(SamplyTrack::NETWORK);
	auto subscription = StartSamplyResourceSamplingForTesting(tracks, directory.c_str());
	REQUIRE(subscription);
	subscription.reset();

	auto file_system = FileSystem::CreateLocal();
	string counter_path;
	file_system->ListFiles(directory, [&](const string &path, bool is_directory) {
		if (!is_directory && StringUtil::StartsWith(path, "counter-") && StringUtil::EndsWith(path, ".txt")) {
			counter_path = file_system->JoinPath(directory, path);
		}
	});
	REQUIRE(!counter_path.empty());

	std::ifstream counter_file(counter_path);
	string contents((std::istreambuf_iterator<char>(counter_file)), std::istreambuf_iterator<char>());
	auto lines = StringUtil::Split(contents, '\n');
	REQUIRE(lines.size() == 4);
	REQUIRE(StringUtil::StartsWith(lines[0], "clock "));
	REQUIRE(StringUtil::Contains(lines[1], " rss "));
	REQUIRE(StringUtil::Contains(lines[2], " network-rx "));
	REQUIRE(StringUtil::Contains(lines[3], " network-tx "));
}
#endif
