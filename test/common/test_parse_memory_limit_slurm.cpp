#include "catch.hpp"
#include "duckdb/main/config.hpp"

using namespace duckdb;

TEST_CASE("Test ParseMemoryLimitSlurm", "[config]") {
	static constexpr idx_t UNLIMITED = static_cast<idx_t>(NumericLimits<int64_t>::Maximum());

	// empty or unparseable input is invalid
	REQUIRE(!DBConfig::ParseMemoryLimitSlurm("").IsValid());
	REQUIRE(!DBConfig::ParseMemoryLimitSlurm("abc").IsValid());
	REQUIRE(!DBConfig::ParseMemoryLimitSlurm("nan").IsValid());

	// plain numbers default to MB, suffixes K/M/G/T scale by powers of 1000
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("1000").GetIndex() == 1000ULL * 1000 * 1000);
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("10K").GetIndex() == 10ULL * 1000);
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("2g").GetIndex() == 2ULL * 1000 * 1000 * 1000);
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("1.5G").GetIndex() == 1500ULL * 1000 * 1000);
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("1T").GetIndex() == 1000ULL * 1000 * 1000 * 1000);

	// negative means unlimited
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("-1").GetIndex() == UNLIMITED);

	// values whose scaled result is not representable in idx_t are clamped to unlimited
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("inf").GetIndex() == UNLIMITED);
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("1000000000T").GetIndex() == UNLIMITED);
	// double(idx_max) rounds up to 2^64 - this value sits exactly on the clamp boundary
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("18446744073709552K").GetIndex() == UNLIMITED);
	// one representable double below the boundary stays an exact value
	REQUIRE(DBConfig::ParseMemoryLimitSlurm("18446744073709548K").GetIndex() == 18446744073709547520ULL);
}
