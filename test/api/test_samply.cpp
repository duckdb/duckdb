#include "catch.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/profiler/samply.hpp"
#include "test_helpers.hpp"

#include <fstream>

using namespace duckdb;

TEST_CASE("Samply query profiles are written in query sidecar format", "[api][samply]") {
	auto directory = TestCreatePath("samply_query_profiles");
	TestCreateDirectory(directory);
	string path;
	{
		SamplyQueryWriter writer(directory.c_str());
		REQUIRE(writer.WriteProfile(1000, 250, "{\"query\":{\"sql\":\"SELECT 42\"}}"));
		path = writer.GetPath();
	}

	REQUIRE(StringUtil::Contains(path, "/query-"));
	REQUIRE(StringUtil::EndsWith(path, ".jsonl"));
	std::ifstream query_file(path);
	string contents((std::istreambuf_iterator<char>(query_file)), std::istreambuf_iterator<char>());
	REQUIRE(
	    contents ==
	    "{\"version\":1,\"start_unix_ns\":1000,\"duration_ns\":250,\"profile\":{\"query\":{\"sql\":\"SELECT 42\"}}}\n");
}

TEST_CASE("Samply counters are buffered and flushed in counter-file format", "[api][samply]") {
	auto directory = TestCreatePath("samply_counters");
	TestCreateDirectory(directory);
	string path;
	{
		SamplyCounterWriter writer(directory.c_str());
		REQUIRE(writer.WriteClock(100, 1000));
		REQUIRE(writer.WriteClock(200, 2000));
		REQUIRE(writer.WriteSample(110, "tracked-memory", 4096));
		REQUIRE(writer.WriteSample(120, "http-download", 50));
		REQUIRE(writer.WriteSample(120, "http-upload", 25));
		path = writer.GetPath();

		std::ifstream before_flush(path);
		string buffered_contents((std::istreambuf_iterator<char>(before_flush)), std::istreambuf_iterator<char>());
		REQUIRE(buffered_contents.empty());
	}

	std::ifstream counter_file(path);
	string contents((std::istreambuf_iterator<char>(counter_file)), std::istreambuf_iterator<char>());
	REQUIRE(contents == "clock 100 1000\n110 tracked-memory 4096\n120 http-download 50\n120 http-upload 25\n");
}

TEST_CASE("DuckDB allocator reports tracked memory", "[api][samply]") {
	Allocator allocator;
	auto baseline = Allocator::GetTrackedMemory();
	auto pointer = allocator.AllocateData(64);
	REQUIRE(Allocator::GetTrackedMemory() == baseline + 64);
	pointer = allocator.ReallocateData(pointer, 64, 96);
	REQUIRE(Allocator::GetTrackedMemory() == baseline + 96);
	allocator.FreeData(pointer, 96);
	REQUIRE(Allocator::GetTrackedMemory() == baseline);
}

TEST_CASE("Samply HTTP attempts are written in HTTP sidecar format", "[api][samply]") {
	auto directory = TestCreatePath("samply_http");
	TestCreateDirectory(directory);
	string path;
	{
		SamplyHTTPWriter writer(directory.c_str());
		REQUIRE(writer.WriteAttempt(1000000000, 25000000, "GET", "https://example.com/file.parquet", 206, 4096, 5000000,
		                            "bytes=100-4195", "bytes 100-4195/10000"));
		path = writer.GetPath();
	}

	REQUIRE(StringUtil::Contains(path, "/http-"));
	REQUIRE(StringUtil::EndsWith(path, ".txt"));
	std::ifstream http_file(path);
	string contents((std::istreambuf_iterator<char>(http_file)), std::istreambuf_iterator<char>());
	StringUtil::RTrim(contents);
	auto fields = StringUtil::Split(contents, '\t');
	REQUIRE(fields.size() == 10);
	REQUIRE(fields[0] == "1");
	REQUIRE(fields[1] == "1000000000");
	REQUIRE(fields[2] == "25000000");
	REQUIRE(fields[3] == "206");
	REQUIRE(fields[4] == "4096");
	REQUIRE(fields[5] == "5000000");
	REQUIRE(fields[6] == "GET");
	REQUIRE(fields[7] == Blob::ToBase64("https://example.com/file.parquet"));
	REQUIRE(fields[8] == Blob::ToBase64("bytes=100-4195"));
	REQUIRE(fields[9] == Blob::ToBase64("bytes 100-4195/10000"));
}

#if defined(__linux__) || defined(__APPLE__)
TEST_CASE("Samply HTTP tracking follows subscription lifetime", "[api][samply]") {
	REQUIRE_FALSE(SamplyHTTPTrackEnabled());
	auto subscription = StartSamplyResourceSampling(static_cast<uint8_t>(SamplyTrack::HTTP));
	REQUIRE(subscription);
	REQUIRE(SamplyHTTPTrackEnabled());
	subscription.reset();
	REQUIRE_FALSE(SamplyHTTPTrackEnabled());
}

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
	REQUIRE(StringUtil::Contains(lines[1], " tracked-memory "));
	REQUIRE(StringUtil::Contains(lines[2], " http-download "));
	REQUIRE(StringUtil::Contains(lines[3], " http-upload "));
}
#endif
