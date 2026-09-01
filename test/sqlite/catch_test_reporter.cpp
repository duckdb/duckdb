#include "catch_test_reporter.hpp"

#include "catch.hpp"

namespace duckdb {

void CatchTestReporter::Fail(const string &message, const string &file, idx_t line) {
	// FAIL/FAIL_LINE throw Catch's own TestFailureException, so this never returns.
	if (file.empty()) {
		FAIL(message);
	}
	FAIL_LINE(file, NumericCast<int>(line), message);
}

void CatchTestReporter::Skip(const string &reason) {
	TestReporter::Skip(reason);
	Catch::getResultCapture().skipTestDuringRun(reason);
}

void CatchTestReporter::Assertion() {
	TestReporter::Assertion();
	// keep Catch's assertion tally moving
	REQUIRE(true);
}

string CatchTestReporter::CurrentTestName() {
	return Catch::getResultCapture().getCurrentTestName();
}

} // namespace duckdb
