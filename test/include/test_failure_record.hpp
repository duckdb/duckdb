//===----------------------------------------------------------------------===//
//                         DuckDB
//
// test_failure_record.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! The semantic kind of a sqllogictest failure. Mirrors the SQLLogicTestLogger method that reports it,
//! so a consumer can branch on the kind instead of pattern-matching the human-readable message.
enum class TestFailureKind : uint8_t {
	NONE,
	UNEXPECTED_FAILURE,
	UNEXPECTED_STATEMENT,
	WRONG_ROW_COUNT,
	WRONG_COLUMN_COUNT,
	WRONG_COLUMN_COUNT_CORRECT_RESULT,
	WRONG_RESULT_HASH,
	VALUE_MISMATCH,
	EXPECTED_ERROR_MISMATCH,
	INTERNAL_EXCEPTION,
	NOT_CLEANLY_DIVISIBLE,
	SPLIT_MISMATCH,
	LOAD_DATABASE_FAIL,
	OTHER
};

const char *TestFailureKindToString(TestFailureKind kind);

//! A structured description of the failure that ended a test. Populated by SQLLogicTestLogger alongside
//! the human-readable output, and rendered by the JSON reporter; fields not meaningful for a given kind
//! stay empty.
struct TestFailureRecord {
	TestFailureKind kind = TestFailureKind::NONE;
	//! The human-readable headline (the same text the default output prints)
	string message;
	string file;
	idx_t line = 0;
	string query;

	//! Row-oriented values, one string per cell, laid out row-major over `columns` columns
	vector<string> expected;
	vector<string> actual;
	bool has_expected = false;
	bool has_actual = false;
	//! True when the corresponding vector was capped, so a consumer does not read it as the full result
	bool expected_truncated = false;
	bool actual_truncated = false;
	idx_t expected_rows = 0;
	idx_t actual_rows = 0;
	idx_t columns = 0;

	//! Row indices (into the untruncated result) whose expected and actual values differ. Only populated
	//! when both sides have the same shape, which is what makes a row-wise comparison meaningful.
	vector<idx_t> mismatch_rows;

	string expected_hash;
	string actual_hash;
	//! A concrete remedy when the logger can derive one (e.g. the corrected `query III` header)
	string suggested_fix;
	string error_message;

	bool IsSet() const {
		return kind != TestFailureKind::NONE;
	}
};

//! Process-wide slot holding the failure that ended the test currently running. A sqllogictest aborts at
//! its first failure, so one slot per run is enough; Clear() runs at test start and Take() at test end.
class TestFailureRecorder {
public:
	//! Overwrites any record already held: the first failure aborts the test, so a later record can only
	//! come from a subsequent test whose start cleared this anyway.
	static void Record(TestFailureRecord record);
	//! Move the current record out, leaving the slot empty
	static TestFailureRecord Take();
	static bool HasRecord();
	static void Clear();
	//! Records are only built when a consumer will read them - the default output path skips the work.
	static void SetEnabled(bool enabled);
	static bool Enabled();

private:
	static TestFailureRecorder &Instance();

private:
	mutex record_lock;
	TestFailureRecord record;
	bool enabled = false;
};

//! Cap on how many cells of each result are copied into a record. Bounds a runaway result to a
//! predictable payload size while still showing enough to identify the failure.
static constexpr idx_t TEST_FAILURE_MAX_VALUES = 100;

} // namespace duckdb
