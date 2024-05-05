#include "catch.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/json/json_value.hpp"
#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test that JsonValue parsing works", "[json]") {
	const char *str = R"(
		{
			"key1": 42,
			"key2": "hello",
			"key3": [1, 2, 3],
			"key4": {
				"key5": 43
			},
			"key6": null,
			"key7": true,
			"key8": false
		}
	)";

	auto json = JsonValue::Parse(str);
	REQUIRE(json.GetType() == JsonKind::OBJECT);

	REQUIRE(json["key1"].GetType() == JsonKind::NUMBER);
	REQUIRE(json["key1"].AsNumber() == 42);

	REQUIRE(json["key2"].GetType() == JsonKind::STRING);
	REQUIRE(json["key2"].AsString() == "hello");

	REQUIRE(json["key3"].GetType() == JsonKind::ARRAY);
	REQUIRE(json["key3"][0].AsNumber() == 1);
	REQUIRE(json["key3"][1].AsNumber() == 2);
	REQUIRE(json["key3"][2].AsNumber() == 3);

	REQUIRE(json["key4"].GetType() == JsonKind::OBJECT);
	REQUIRE(json["key4"]["key5"].AsNumber() == 43);

	REQUIRE(json["key6"].GetType() == JsonKind::NULLVALUE);

	REQUIRE(json["key7"].GetType() == JsonKind::BOOLEAN);
	REQUIRE(json["key7"].AsBool() == true);

	REQUIRE(json["key8"].GetType() == JsonKind::BOOLEAN);
	REQUIRE(json["key8"].AsBool() == false);
}

TEST_CASE("Test that JsonValue parsing works on data files", "[json]") {
	// Load all files in the data/json/ directory and try to parse them
	auto fs = FileSystem::CreateLocal();

	std::vector<char> buffer;
	bool found_anything = false;
	auto test_path = "data/json";
	INFO("Listing files in " << test_path);
	fs->ListFiles(test_path, [&](const string &filename, bool is_dir) {
		if (is_dir) {
			return;
		}
		auto path = fs->JoinPath(test_path, filename);
		if (!StringUtil::EndsWith(path, ".json")) {
			return;
		}
		found_anything = true;

		INFO("Parsing file " << path)

		auto file = fs->OpenFile(path, FileFlags::FILE_FLAGS_READ);
		auto file_size = file->GetFileSize();
		buffer.resize(file_size);
		file->Read(buffer.data(), file_size);

		// Now turn the buffer into a string
		string str(buffer.begin(), buffer.begin() + file_size);
		REQUIRE(!str.empty());

		// Now parse the JSON, this should not throw
		REQUIRE_NOTHROW(JsonValue::Parse(str));
	});
	REQUIRE(found_anything);
}
