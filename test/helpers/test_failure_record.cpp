#include "test_failure_record.hpp"

namespace duckdb {

const char *TestFailureKindToString(TestFailureKind kind) {
	switch (kind) {
	case TestFailureKind::UNEXPECTED_FAILURE:
		return "unexpected_failure";
	case TestFailureKind::UNEXPECTED_STATEMENT:
		return "unexpected_statement";
	case TestFailureKind::WRONG_ROW_COUNT:
		return "wrong_row_count";
	case TestFailureKind::WRONG_COLUMN_COUNT:
		return "wrong_column_count";
	case TestFailureKind::WRONG_COLUMN_COUNT_CORRECT_RESULT:
		return "wrong_column_count_correct_result";
	case TestFailureKind::WRONG_RESULT_HASH:
		return "wrong_result_hash";
	case TestFailureKind::VALUE_MISMATCH:
		return "value_mismatch";
	case TestFailureKind::EXPECTED_ERROR_MISMATCH:
		return "expected_error_mismatch";
	case TestFailureKind::INTERNAL_EXCEPTION:
		return "internal_exception";
	case TestFailureKind::NOT_CLEANLY_DIVISIBLE:
		return "not_cleanly_divisible";
	case TestFailureKind::SPLIT_MISMATCH:
		return "split_mismatch";
	case TestFailureKind::LOAD_DATABASE_FAIL:
		return "load_database_fail";
	case TestFailureKind::OTHER:
		return "other";
	case TestFailureKind::NONE:
	default:
		return "none";
	}
}

TestFailureRecorder &TestFailureRecorder::Instance() {
	static TestFailureRecorder instance;
	return instance;
}

void TestFailureRecorder::Record(TestFailureRecord new_record) {
	auto &recorder = Instance();
	if (!recorder.enabled) {
		return;
	}
	lock_guard<mutex> guard(recorder.record_lock);
	recorder.record = std::move(new_record);
}

TestFailureRecord TestFailureRecorder::Take() {
	auto &recorder = Instance();
	lock_guard<mutex> guard(recorder.record_lock);
	TestFailureRecord result = std::move(recorder.record);
	recorder.record = TestFailureRecord();
	return result;
}

bool TestFailureRecorder::HasRecord() {
	auto &recorder = Instance();
	lock_guard<mutex> guard(recorder.record_lock);
	return recorder.record.IsSet();
}

void TestFailureRecorder::Clear() {
	auto &recorder = Instance();
	lock_guard<mutex> guard(recorder.record_lock);
	recorder.record = TestFailureRecord();
}

void TestFailureRecorder::SetEnabled(bool enabled) {
	Instance().enabled = enabled;
}

bool TestFailureRecorder::Enabled() {
	return Instance().enabled;
}

} // namespace duckdb
