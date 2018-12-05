#include "catch.hpp"
#include "common/types/date.hpp"

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
}
