#include "test_helpers.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "catch.hpp"

// work around MinGW defining UUID
typedef duckdb::UUID UUID;
typedef duckdb::uhugeint_t uhugeint_t;

TEST_CASE("Test UUID API", "[api]") {
	REQUIRE(UUID::ToString(UUID::FromUHugeint(uhugeint_t(0))) == "00000000-0000-0000-0000-000000000000");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(uhugeint_t(1))) == "00000000-0000-0000-0000-000000000001");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(duckdb::NumericLimits<uhugeint_t>::Maximum())) ==
	        "ffffffff-ffff-ffff-ffff-ffffffffffff");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(duckdb::NumericLimits<uhugeint_t>::Maximum() - 1)) ==
	        "ffffffff-ffff-ffff-ffff-fffffffffffe");
	REQUIRE(UUID::ToString(UUID::FromUHugeint(duckdb::NumericLimits<uhugeint_t>::Maximum() / 2)) ==
	        "7fffffff-ffff-ffff-ffff-ffffffffffff");
	REQUIRE(UUID::ToString(UUID::FromUHugeint((duckdb::NumericLimits<uhugeint_t>::Maximum() / 2) + 1)) ==
	        "80000000-0000-0000-0000-000000000000");

	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("00000000-0000-0000-0000-000000000000")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x0000000000000000 && input.lower == 0x0000000000000000;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("00000000-0000-0000-0000-000000000001")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x0000000000000000 && input.lower == 0x0000000000000001;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("ffffffff-ffff-ffff-ffff-ffffffffffff")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0xffffffffffffffff && input.lower == 0xffffffffffffffff;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("ffffffff-ffff-ffff-ffff-fffffffffffe")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0xffffffffffffffff && input.lower == 0xfffffffffffffffe;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("7fffffff-ffff-ffff-ffff-ffffffffffff")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x7fffffffffffffff && input.lower == 0xffffffffffffffff;
	             }));
	REQUIRE_THAT(UUID::ToUHugeint(UUID::FromString("80000000-0000-0000-0000-000000000000")),
	             Catch::Predicate<uhugeint_t>([&](const uhugeint_t &input) {
		             return input.upper == 0x8000000000000000 && input.lower == 0x0000000000000000;
	             }));
}
