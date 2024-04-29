#include "catch.hpp"
#include "duckdb/common/json/json_value.hpp"

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
			}
		}
	)";

	auto json = JsonValue::Parse(str);
	REQUIRE(json.GetType() == JsonKind::OBJECT);

	REQUIRE(json["key1"].GetType() == JsonKind::NUMBER);
	REQUIRE(json["key1"].GetNumber() == 42);

	REQUIRE(json["key2"].GetType() == JsonKind::STRING);
	REQUIRE(json["key2"].GetString() == "hello");

	REQUIRE(json["key3"].GetType() == JsonKind::ARRAY);
	REQUIRE(json["key3"][0].GetNumber() == 1);
	REQUIRE(json["key3"][1].GetNumber() == 2);
	REQUIRE(json["key3"][2].GetNumber() == 3);

	REQUIRE(json["key4"].GetType() == JsonKind::OBJECT);
	REQUIRE(json["key4"]["key5"].GetNumber() == 43);
}
