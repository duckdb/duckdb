#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/process_util.hpp"
#include "duckdb/main/os_util.hpp"

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
		REQUIRE_NOTHROW(ProcessUtil::GetProcessDescription(ProcessUtil::CurrentProcessId()));
		REQUIRE_NOTHROW(ProcessUtil::GetProcessDescription(0));
	}
}

TEST_CASE("Test OSUtil system files", "[api]") {
	string contents;
	SECTION("arguments that do not belong to the file are refused") {
		REQUIRE(!OSUtil::TryReadSystemFile(SystemFile::PROCESS_CGROUPS, "1", contents));
		REQUIRE(!OSUtil::TryReadSystemFile(SystemFile::PROCESS_CMDLINE, "", contents));
		REQUIRE(!OSUtil::TryReadSystemFile(SystemFile::PROCESS_CMDLINE, "self/../../etc/passwd", contents));
		REQUIRE(!OSUtil::TryReadSystemFile(SystemFile::PROCESS_CMDLINE, "-1", contents));
		REQUIRE(!OSUtil::TryReadSystemFile(SystemFile::CGROUP_V2_CPU_MAX, "/../../../etc", contents));
		REQUIRE(!OSUtil::TryReadSystemFile(SystemFile::CGROUP_V2_CPU_MAX, "relative", contents));
	}
#ifdef __linux__
	SECTION("a known file can be read") {
		REQUIRE(OSUtil::TryReadSystemFile(SystemFile::PROCESS_CGROUPS, string(), contents));
		REQUIRE(OSUtil::TryReadSystemFile(SystemFile::PROCESS_CMDLINE, to_string(ProcessUtil::CurrentProcessId()),
		                                  contents));
		REQUIRE(!contents.empty());
	}
#endif
}
