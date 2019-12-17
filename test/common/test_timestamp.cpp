#include "catch.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

using namespace duckdb;
using namespace std;

static void VerifyTimestamp(date_t date, dtime_t time, int64_t epoch) {
	// create the timestamp from the string
	timestamp_t stamp = Timestamp::FromString(Date::ToString(date) + " " + Time::ToString(time));
	// verify that we can get the date and time back
	REQUIRE(Timestamp::GetDate(stamp) == date);
	REQUIRE(Timestamp::GetTime(stamp) == time);
	// verify that the individual extract functions work
	int32_t hour, min, sec, msec;
	Time::Convert(time, hour, min, sec, msec);
	REQUIRE(Timestamp::GetHours(stamp) == hour);
	REQUIRE(Timestamp::GetMinutes(stamp) == min);
	REQUIRE(Timestamp::GetSeconds(stamp) == sec);

	// verify that the epoch is correct
	REQUIRE(epoch == (Date::Epoch(date) + time / 1000));
	REQUIRE(Timestamp::GetEpoch(stamp) == epoch);
}

TEST_CASE("Verify that timestamp functions work", "[timestamp]") {
	VerifyTimestamp(Date::FromDate(2019, 8, 26), Time::FromTime(8, 52, 6), 1566809526);
	VerifyTimestamp(Date::FromDate(1970, 1, 1), Time::FromTime(0, 0, 0), 0);
	VerifyTimestamp(Date::FromDate(2000, 10, 10), Time::FromTime(10, 10, 10), 971172610);
}
