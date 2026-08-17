#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"

using namespace duckdb;

TEST_CASE("Test temporary file slot codes", "[api]") {
	SECTION("the first slots are a single character") {
		// the point of the scheme: a directory listing stays readable
		REQUIRE(TemporarySlotCode(0) == "0");
		REQUIRE(TemporarySlotCode(9) == "9");
		REQUIRE(TemporarySlotCode(10) == "a");
		REQUIRE(TemporarySlotCode(31) == "z");
		REQUIRE(TemporarySlotCode(32) == "10");
	}

	SECTION("the alphabet is one case and excludes the characters that are read wrong") {
		set<char> used;
		for (idx_t i = 0; i < 32; i++) {
			auto code = TemporarySlotCode(i);
			REQUIRE(code.size() == 1);
			used.insert(code[0]);
		}
		REQUIRE(used.size() == 32);
		for (const auto excluded : {'i', 'l', 'o', 'u'}) {
			REQUIRE(used.find(excluded) == used.end());
		}
		// a mixed-case alphabet would alias two slots to one file on a case-insensitive filesystem
		for (const auto c : used) {
			REQUIRE(!(c >= 'A' && c <= 'Z'));
		}
	}

	SECTION("codes are distinct and round-trip through a file name") {
		set<string> codes;
		for (idx_t i = 0; i < 4096; i++) {
			auto code = TemporarySlotCode(i);
			codes.insert(code);
			string parsed;
			REQUIRE(TryParseTemporaryFileOwner(TemporaryFilePrefix(code) + "storage_DEFAULT-0.tmp", parsed));
			REQUIRE(parsed == code);
			REQUIRE(TryParseTemporaryFileOwner(TemporaryLockFileName(code), parsed));
			REQUIRE(parsed == code);
		}
		REQUIRE(codes.size() == 4096);
	}

	SECTION("names from a version that did not use slots are not claimed") {
		// a UUID carries '-', which is not in the alphabet, so these parse as unowned and are left
		// alone rather than reaped out from under a running instance of that version
		string parsed;
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_5ee2fed7-f0e1-4e7f-9399-d813d74cee53_storage_DEFAULT-0.tmp",
		                                    parsed));
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_storage_DEFAULT-0.tmp", parsed));
		REQUIRE(!TryParseTemporaryFileOwner("duckdb_temp_block-1.block", parsed));
		REQUIRE(!TryParseTemporaryFileOwner("something_else.tmp", parsed));
	}
}
