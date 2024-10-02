#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "catch.hpp"

#include "duckdb/common/vector.hpp"
#include <string>
#include <cstring>

using namespace duckdb;

TEST_CASE("Test strcmp() to ensure platform sanity", "[comparison]") {
	int res;
	res = strcmp("ZZZ", "ZZZ");
	REQUIRE(res == 0);

	res = strcmp("ZZZ", "HXR");
	REQUIRE(res > 0);

	res = strcmp("ZZZ", "NUT");
	REQUIRE(res > 0);

	res = strcmp("HXR", "ZZZ");
	REQUIRE(res < 0);

	res = strcmp("HXR", "HXR");
	REQUIRE(res == 0);

	res = strcmp("HXR", "NUT");
	REQUIRE(res < 0);

	res = strcmp("NUT", "ZZZ");
	REQUIRE(res < 0);

	res = strcmp("NUT", "HXR");
	REQUIRE(res > 0);

	res = strcmp("NUT", "NUT");
	REQUIRE(res == 0);

	Value zzz("ZZZ");
	Value hxr("HXR");
	Value nut("NUT");

	REQUIRE_FALSE(zzz > zzz);
	REQUIRE(zzz > hxr);
	REQUIRE(zzz > nut);

	REQUIRE(zzz >= zzz);
	REQUIRE(zzz >= hxr);
	REQUIRE(zzz >= nut);

	REQUIRE(zzz <= zzz);
	REQUIRE_FALSE(zzz <= hxr);
	REQUIRE_FALSE(zzz <= nut);

	REQUIRE(zzz == zzz);
	REQUIRE_FALSE(zzz == hxr);
	REQUIRE_FALSE(zzz == nut);

	REQUIRE_FALSE(zzz != zzz);
	REQUIRE(zzz != hxr);
	REQUIRE(zzz != nut);

	REQUIRE_FALSE(hxr > zzz);
	REQUIRE_FALSE(hxr > hxr);
	REQUIRE_FALSE(hxr > nut);

	REQUIRE_FALSE(hxr >= zzz);
	REQUIRE(hxr >= hxr);
	REQUIRE_FALSE(hxr >= nut);

	REQUIRE(hxr <= zzz);
	REQUIRE(hxr <= hxr);
	REQUIRE(hxr <= nut);

	REQUIRE_FALSE(hxr == zzz);
	REQUIRE(hxr == hxr);
	REQUIRE_FALSE(hxr == nut);

	REQUIRE(hxr != zzz);
	REQUIRE_FALSE(hxr != hxr);
	REQUIRE(hxr != nut);

	REQUIRE_FALSE(nut > zzz);
	REQUIRE(nut > hxr);
	REQUIRE_FALSE(nut > nut);

	REQUIRE_FALSE(nut >= zzz);
	REQUIRE(nut >= hxr);
	REQUIRE(nut >= nut);

	REQUIRE(nut <= zzz);
	REQUIRE_FALSE(nut <= hxr);
	REQUIRE(nut <= nut);

	REQUIRE_FALSE(nut == zzz);
	REQUIRE_FALSE(nut == hxr);
	REQUIRE(nut == nut);

	REQUIRE(nut != zzz);
	REQUIRE(nut != hxr);
	REQUIRE_FALSE(nut != nut);
}

TEST_CASE("Test join vector items", "[string_util]") {
	SECTION("Three string items") {
		duckdb::vector<std::string> str_items = {"abc", "def", "ghi"};
		std::string result = StringUtil::Join(str_items, ",");
		REQUIRE(result == "abc,def,ghi");
	}

	SECTION("One string item") {
		duckdb::vector<std::string> str_items = {"abc"};
		std::string result = StringUtil::Join(str_items, ",");
		REQUIRE(result == "abc");
	}

	SECTION("No string items") {
		duckdb::vector<std::string> str_items;
		std::string result = StringUtil::Join(str_items, ",");
		REQUIRE(result == "");
	}

	SECTION("Three int items") {
		duckdb::vector<int> int_items = {1, 2, 3};
		std::string result =
		    StringUtil::Join(int_items, int_items.size(), ", ", [](const int &item) { return to_string(item); });
		REQUIRE(result == "1, 2, 3");
	}

	SECTION("One int item") {
		duckdb::vector<int> int_items = {1};
		std::string result =
		    StringUtil::Join(int_items, int_items.size(), ", ", [](const int &item) { return to_string(item); });
		REQUIRE(result == "1");
	}

	SECTION("No int items") {
		duckdb::vector<int> int_items;
		std::string result =
		    StringUtil::Join(int_items, int_items.size(), ", ", [](const int &item) { return to_string(item); });
		REQUIRE(result == "");
	}
}

TEST_CASE("Test split quoted strings", "[string_util]") {
	SECTION("Empty string") {
		REQUIRE(StringUtil::SplitWithQuote("") == duckdb::vector<string> {});
	}

	SECTION("Empty string with space") {
		REQUIRE(StringUtil::SplitWithQuote(" ") == duckdb::vector<string> {});
	}

	SECTION("One item") {
		REQUIRE(StringUtil::SplitWithQuote("x") == duckdb::vector<string> {"x"});
	}

	SECTION("One item with space") {
		REQUIRE(StringUtil::SplitWithQuote(" x ") == duckdb::vector<string> {"x"});
	}

	SECTION("One item with quote") {
		REQUIRE(StringUtil::SplitWithQuote("\"x\"") == duckdb::vector<string> {"x"});
	}

	SECTION("One empty item with quote") {
		REQUIRE(StringUtil::SplitWithQuote("\"\"") == duckdb::vector<string> {""});
	}

	SECTION("One empty item, followed by non-empty one - Or vise versa") {
		REQUIRE(StringUtil::SplitWithQuote("\"\",hello") == duckdb::vector<string> {"", "hello"});
		REQUIRE(StringUtil::SplitWithQuote(",\"hello\"") == duckdb::vector<string> {"", "hello"});
		REQUIRE(StringUtil::SplitWithQuote(",hello") == duckdb::vector<string> {"", "hello"});
		REQUIRE(StringUtil::SplitWithQuote("\"\",\"hello\"") == duckdb::vector<string> {"", "hello"});

		REQUIRE(StringUtil::SplitWithQuote("\"hello\",") == duckdb::vector<string> {"hello", ""});
		REQUIRE(StringUtil::SplitWithQuote("hello,\"\"") == duckdb::vector<string> {"hello", ""});
		REQUIRE(StringUtil::SplitWithQuote("hello,") == duckdb::vector<string> {"hello", ""});
		REQUIRE(StringUtil::SplitWithQuote("\"hello\",\"\"") == duckdb::vector<string> {"hello", ""});
	}

	SECTION("One quoted item with spaces") {
		REQUIRE(StringUtil::SplitWithQuote(" \" x y \" ") == duckdb::vector<string> {" x y "});
	}

	SECTION("One quoted item with a delimiter") {
		REQUIRE(StringUtil::SplitWithQuote("\"x,y\"") == duckdb::vector<string> {"x,y"});
	}

	SECTION("Three items") {
		REQUIRE(StringUtil::SplitWithQuote("x,y,z") == duckdb::vector<string> {"x", "y", "z"});
	}

	SECTION("Three items, with and without quote") {
		REQUIRE(StringUtil::SplitWithQuote("x,\"y\",z") == duckdb::vector<string> {"x", "y", "z"});
	}

	SECTION("Even more items, with and without quote") {
		REQUIRE(StringUtil::SplitWithQuote("a,b,c,d,e,f,g") ==
		        duckdb::vector<string> {"a", "b", "c", "d", "e", "f", "g"});
	}

	SECTION("Three empty items") {
		REQUIRE(StringUtil::SplitWithQuote(",,") == duckdb::vector<string> {"", "", ""});
	}

	SECTION("Three empty quoted items") {
		REQUIRE(StringUtil::SplitWithQuote("\"\",\"\",\"\"") == duckdb::vector<string> {"", "", ""});
	}

	SECTION("Unclosed quote") {
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("\""), ParserException);
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("\"x"), ParserException);
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("\"x "), ParserException);
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("\","), ParserException);
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("\"x,"), ParserException);
	}

	SECTION("Unexpected quote") {
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("abc\"def"), ParserException);
	}

	SECTION("Missing delimiter") {
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("\"x\"\"y\""), ParserException);
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("\"x\" \"y\""), ParserException);
		REQUIRE_THROWS_AS(StringUtil::SplitWithQuote("x y"), ParserException);
	}
}

TEST_CASE("Test path utilities", "[string_util]") {
	SECTION("File name") {
		REQUIRE("bin" == StringUtil::GetFileName("/usr/bin/"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("foo.txt"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("tmp/foo.txt"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("tmp\\foo.txt"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("/tmp/foo.txt"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("\\tmp\\foo.txt"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("foo.txt/."));
		REQUIRE("foo.txt" == StringUtil::GetFileName("foo.txt/./"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("foo.txt/.//"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("foo.txt\\."));
		REQUIRE("foo.txt" == StringUtil::GetFileName("foo.txt\\.\\"));
		REQUIRE("foo.txt" == StringUtil::GetFileName("foo.txt\\.\\\\"));
		REQUIRE(".." == StringUtil::GetFileName(".."));
		REQUIRE("" == StringUtil::GetFileName("/"));
	}

	SECTION("File extension") {
		REQUIRE("cpp" == StringUtil::GetFileExtension("test.cpp"));
		REQUIRE("gz" == StringUtil::GetFileExtension("test.cpp.gz"));
		REQUIRE("" == StringUtil::GetFileExtension("test"));
		REQUIRE("" == StringUtil::GetFileExtension(".gitignore"));
	}

	SECTION("File stem (base name)") {
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp"));
		REQUIRE("test.cpp" == StringUtil::GetFileStem("test.cpp.gz"));
		REQUIRE("test" == StringUtil::GetFileStem("test"));
		REQUIRE(".gitignore" == StringUtil::GetFileStem(".gitignore"));

		REQUIRE("test" == StringUtil::GetFileStem("test.cpp/"));
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp/."));
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp/./"));
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp/.//"));
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp\\"));
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp\\."));
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp\\.\\"));
		REQUIRE("test" == StringUtil::GetFileStem("test.cpp\\.\\\\"));
		REQUIRE(".." == StringUtil::GetFileStem(".."));
		REQUIRE("" == StringUtil::GetFileStem("/"));
		REQUIRE("test" == StringUtil::GetFileStem("tmp/test.txt"));
		REQUIRE("test" == StringUtil::GetFileStem("tmp\\test.txt"));
		REQUIRE("test" == StringUtil::GetFileStem("/tmp/test.txt"));
		REQUIRE("test" == StringUtil::GetFileStem("\\tmp\\test.txt"));
	}

	SECTION("File path") {
		REQUIRE("/usr/local/bin" == StringUtil::GetFilePath("/usr/local/bin/test.cpp"));
		REQUIRE("\\usr\\local\\bin" == StringUtil::GetFilePath("\\usr\\local\\bin\\test.cpp"));
		REQUIRE("tmp" == StringUtil::GetFilePath("tmp/test.txt"));
		REQUIRE("tmp" == StringUtil::GetFilePath("tmp\\test.txt"));
		REQUIRE("/tmp" == StringUtil::GetFilePath("/tmp/test.txt"));
		REQUIRE("\\tmp" == StringUtil::GetFilePath("\\tmp\\test.txt"));
		REQUIRE("/tmp" == StringUtil::GetFilePath("/tmp/test.txt/"));
		REQUIRE("\\tmp" == StringUtil::GetFilePath("\\tmp\\test.txt\\"));
		REQUIRE("/tmp" == StringUtil::GetFilePath("/tmp//test.txt"));
		REQUIRE("\\tmp" == StringUtil::GetFilePath("\\tmp\\\\test.txt"));
	}
}
