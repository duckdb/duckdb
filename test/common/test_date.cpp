#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Time parsing works", "[date][.]") {
	REQUIRE(Time::ToString(Time::FromString("14:42:04")) == "14:42:04");

	for (int hour = 0; hour < 24; hour++) {
		for (int minute = 0; minute < 60; minute++) {
			for (int second = 0; second < 60; second++) {
				if (Time::IsValidTime(hour, minute, second)) {
					REQUIRE(Time::ToString(Time::FromString(Time::Format(hour, minute, second))) ==
					        Time::Format(hour, minute, second));
				}
			}
		}
	}

	int hour = 14;
	int min = 42;
	int sec = 11;

	for (int ms = 0; ms < 1000; ms++) {
		REQUIRE(Time::ToString(Time::FromString(Time::Format(hour, min, sec, ms))) == Time::Format(hour, min, sec, ms));
	}

	// some corner cases without trailing 0
	REQUIRE(Time::ToString(Time::FromString("14:42:04.0")) == "14:42:04");
	REQUIRE(Time::ToString(Time::FromString("14:42:04.00")) == "14:42:04");
	REQUIRE(Time::ToString(Time::FromString("14:42:04.0000")) == "14:42:04"); // questionable

	REQUIRE(Time::ToString(Time::FromString("14:42:04.200")) == "14:42:04.200");
	REQUIRE(Time::ToString(Time::FromString("14:42:04.030")) == "14:42:04.030");
}
