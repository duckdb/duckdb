#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/process_util.hpp"

using namespace duckdb;

TEST_CASE("Test ProcessUtil", "[api]") {
	SECTION("the process running this test is running") {
		auto pid = ProcessUtil::CurrentProcessId();
		REQUIRE(pid > 0);
		REQUIRE(ProcessUtil::ProcessIsRunning(pid));
	}

	SECTION("an id that cannot belong to a process is reported as running") {
		// callers reap what they can prove is gone, so anything unanswerable has to count as live
		REQUIRE(ProcessUtil::ProcessIsRunning(0));
		REQUIRE(ProcessUtil::ProcessIsRunning(-1));
	}

	SECTION("describing a process never throws") {
		// best effort: platforms that cannot say return an empty string rather than failing
		LocalFileSystem fs;
		REQUIRE_NOTHROW(ProcessUtil::GetProcessDescription(fs, ProcessUtil::CurrentProcessId()));
		REQUIRE_NOTHROW(ProcessUtil::GetProcessDescription(fs, 0));
	}
}
