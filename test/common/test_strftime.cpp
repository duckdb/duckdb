#include "catch.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test that strftime format works", "[strftime]") {
	auto string = StrfTimeFormat::Format(Timestamp::FromDatetime(Date::FromDate(1992, 1, 1), dtime_t(0)), "%Y%m%d");
	REQUIRE(string == "19920101");
}
