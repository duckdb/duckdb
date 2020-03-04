#include "catch.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Date parsing works", "[date]") {
	REQUIRE(Date::ToString(Date::FromString("1992-01-01")) == "1992-01-01");
	REQUIRE(Date::ToString(Date::FromString(Date::Format(1992, 1, 1))) == Date::Format(1992, 1, 1));
	REQUIRE(Date::ToString(Date::FromString(Date::Format(1992, 10, 10))) == Date::Format(1992, 10, 10));
	REQUIRE(Date::ToString(Date::FromString(Date::Format(1992, 9, 20))) == Date::Format(1992, 9, 20));
	REQUIRE(Date::ToString(Date::FromString(Date::Format(1992, 12, 31))) == Date::Format(1992, 12, 31));

	REQUIRE(Date::FromString("1992-09-20") == Date::FromDate(1992, 9, 20));
	REQUIRE(Date::ToString(Date::FromString("1992-09-20")) == "1992-09-20");
	REQUIRE(Date::Format(1992, 9, 20) == "1992-09-20");

	REQUIRE(Date::IsLeapYear(1992));
	REQUIRE(Date::IsLeapYear(1996));
	REQUIRE(Date::IsLeapYear(2000));
	REQUIRE(!Date::IsLeapYear(3));
	REQUIRE(!Date::IsLeapYear(2100));
	REQUIRE(!Date::IsLeapYear(1993));

	REQUIRE(Date::Format(30, 1, 1) == "0030-01-01");
	REQUIRE(Date::Format(30000, 1, 1) == "30000-01-01");
	REQUIRE(Date::Format(-1000, 1, 1) == "1000-01-01 (BC)");

	REQUIRE(!Date::IsValidDay(1, 2, 29));

	for (int year = 50; year < 4000; year += 50) {
		for (int month = 1; month <= 12; month++) {
			for (int day = 1; day <= 31; day++) {
				if (Date::IsValidDay(year, month, day)) {
					REQUIRE(Date::ToString(Date::FromString(Date::Format(year, month, day))) ==
					        Date::Format(year, month, day));
				}
			}
		}
	}
	// we accept a few different separators
	REQUIRE(Date::FromString("1992/09/20") == Date::FromDate(1992, 9, 20));
	REQUIRE(Date::FromString("1992 09 20") == Date::FromDate(1992, 9, 20));
	REQUIRE(Date::FromString("1992\\09\\20") == Date::FromDate(1992, 9, 20));

	// invalid formats
	REQUIRE_THROWS(Date::FromString("100000"));
	REQUIRE_THROWS(Date::FromString("1992-10/10"));
	REQUIRE_THROWS(Date::FromString("1992a10a10"));
	REQUIRE_THROWS(Date::FromString("1992/10-10"));
	REQUIRE_THROWS(Date::FromString("hello"));
	REQUIRE_THROWS(Date::FromString("aa-10-10"));
	REQUIRE_THROWS(Date::FromString("1992-aa-10"));
	REQUIRE_THROWS(Date::FromString("1992-10-aa"));
	REQUIRE_THROWS(Date::FromString(""));
	REQUIRE_THROWS(Date::FromString("-"));
	REQUIRE_THROWS(Date::FromString("-/10/10"));
	REQUIRE_THROWS(Date::FromString("-a"));
	REQUIRE_THROWS(Date::FromString("1992-"));
	REQUIRE_THROWS(Date::FromString("1992-10"));
	REQUIRE_THROWS(Date::FromString("1992-10-"));
	// date out of range
	REQUIRE_THROWS(Date::FromString("10000000000-01-01"));
	REQUIRE_THROWS(Date::FromString("-10000000000-01-01"));
	REQUIRE_THROWS(Date::FromString("1992-30-30"));
	REQUIRE_THROWS(Date::FromString("1992-10-50"));
	REQUIRE_THROWS(Date::FromString("1992-10-100"));
}

TEST_CASE("Time parsing works", "[date]") {
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
