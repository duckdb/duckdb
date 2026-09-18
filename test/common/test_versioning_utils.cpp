#include "duckdb/main/extension.hpp"
#include "catch.hpp"

using namespace duckdb;

TEST_CASE("Test release version detection", "[extension]") {
	REQUIRE(VersioningUtils::IsReleaseVersion("v2.0.0"));
	REQUIRE(VersioningUtils::IsReleaseVersion("v2.0.0-alpha35155"));
	REQUIRE(VersioningUtils::IsReleaseVersion("v2.0.0-rc1"));
	REQUIRE_FALSE(VersioningUtils::IsReleaseVersion("v2.0.0-dev123"));
}
