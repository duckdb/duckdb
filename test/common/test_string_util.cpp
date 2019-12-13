#include "duckdb/common/string_util.hpp"

#include "catch.hpp"

#include <vector>
#include <string>

using namespace duckdb;

TEST_CASE("Test join vector items", "[string_util]") {
	SECTION("Three string items") {
		std::vector<std::string> str_items = {"abc", "def", "ghi"};
		std::string result = StringUtil::Join(str_items, ",");
		REQUIRE(result == "abc,def,ghi");
	}

	SECTION("One string item") {
		std::vector<std::string> str_items = {"abc"};
		std::string result = StringUtil::Join(str_items, ",");
		REQUIRE(result == "abc");
	}

	SECTION("No string items") {
		std::vector<std::string> str_items;
		std::string result = StringUtil::Join(str_items, ",");
		REQUIRE(result == "");
	}

	SECTION("Three int items") {
		std::vector<int> int_items = {1, 2, 3};
		std::string result =
		    StringUtil::Join(int_items, int_items.size(), ", ", [](const int &item) { return std::to_string(item); });
		REQUIRE(result == "1, 2, 3");
	}

	SECTION("One int item") {
		std::vector<int> int_items = {1};
		std::string result =
		    StringUtil::Join(int_items, int_items.size(), ", ", [](const int &item) { return std::to_string(item); });
		REQUIRE(result == "1");
	}

	SECTION("No int items") {
		std::vector<int> int_items;
		std::string result =
		    StringUtil::Join(int_items, int_items.size(), ", ", [](const int &item) { return std::to_string(item); });
		REQUIRE(result == "");
	}
}
