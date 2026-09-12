#include "catch_test_reporter.hpp"

#include "catch.hpp"
#include "test_failure_record.hpp"

namespace duckdb {

void CatchTestReporter::Fail(const string &message, const string &file, idx_t line) {
	// Failures routed through the sqllogictest logger already carry a far richer record; this covers the
	// rest (parse errors, load and cleanup failures), which know the script location but nothing else.
	if (TestFailureRecorder::Enabled() && !TestFailureRecorder::HasRecord()) {
		TestFailureRecord record;
		record.kind = TestFailureKind::OTHER;
		record.message = message;
		record.file = file;
		record.line = line;
		TestFailureRecorder::Record(std::move(record));
	}
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
