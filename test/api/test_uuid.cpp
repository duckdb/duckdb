#include "test_helpers.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "catch.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test UUID API", "[api]") {
	REQUIRE(UUID::ToString(UUID::FromUHugeint(uhugeint_t(0))) == "00000000-0000-0000-0000-000000000000");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(uhugeint_t(1))) == "00000000-0000-0000-0000-000000000001");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(NumericLimits<uhugeint_t>::Maximum())) ==
	        "ffffffff-ffff-ffff-ffff-ffffffffffff");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(NumericLimits<uhugeint_t>::Maximum() - 1)) ==
	        "ffffffff-ffff-ffff-ffff-fffffffffffe");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(NumericLimits<uhugeint_t>::Maximum() / 2)) ==
	        "7fffffff-ffff-ffff-ffff-ffffffffffff");
	REQUIRE(UUID::ToString(UUID::FromUHugeint((NumericLimits<uhugeint_t>::Maximum() / 2) + 1)) ==
	        "80000000-0000-0000-0000-000000000000");
}
