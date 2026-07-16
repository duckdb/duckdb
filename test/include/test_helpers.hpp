//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// test_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef _MSC_VER
// these break enum.hpp otherwise
#undef DELETE
#undef DEFAULT
#undef EXISTS
#undef IN
// this breaks file_system.cpp otherwise
#undef CreateDirectory
#undef RemoveDirectory
#endif

#include "compare_result.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types.hpp"
#include "test_config.hpp"
#include <sstream>
#include <iostream>

namespace duckdb {

void RegisterSqllogictests();
void RegisterSqllogictestStdin();
bool SummarizeFailures();

//! Test identity: the full test name sanitized to one filesystem/shell-safe path component (every char
//! outside [A-Za-z0-9_-] -> '_', including '.'). The body suffix is kept, so siblings differing only by
//! suffix (foo.test vs foo.test_slow) stay distinct (foo_test vs foo_test_slow). Shared by the TEST_ID
//! env var and the temp-dir leaf so they agree.
string TestNameToId(const string &name);

//! Delete a database file (+ its .wal). A no-op under --database-destroy off (retain).
void DeleteDatabase(string path);
void TestDeleteDirectory(string path);
void TestCreateDirectory(string path);
string TestJoinPath(string path1, string path2);
void TestDeleteFile(string path);
void TestChangeDirectory(string path);

void SetDeleteTestPath(bool delete_path);
bool DeleteTestPath();
void ClearTestDirectory();
string TestGetCurrentDirectory();
string TestDirectoryPath();
string TestCreatePath(string suffix);

//! The HOME sandbox dir ("<run-root>-home"): a sibling of the run root, never inside any {TEST_DIR}.
//! main points HOME/USERPROFILE here once per invocation (isolates ~/.duckdb without landing inside a
//! test-whitelisted dir). Materialized by PrepareTempDir, reclaimed by DestroyTempDir.
string GetTempDirHome();

void SetEmitTestEvents(bool emit);
bool EmitTestEventsEnabled();

unique_ptr<DBConfig> GetTestConfig();
bool TestIsInternalError(unordered_set<string> &internal_error_messages, const string &error);

// -----------------------------------------------------------------------------
// --temp-dir-* family: TEMP_DIR = $BASE / [$RUN_ID] / [$TEST_ID]
//
// Two independent, toggleable nesting levels plus create/destroy dispositions
// inherited down every level. See test/py/DISPOSITIONS.md.
//
//   $BASE    -- root; default "duckdb_unittest_tempdir" or --temp-dir-base <p>
//   $RUN_ID  -- per-invocation isolation ($TS--$RANDTAG when auto, or a caller id)
//   $TEST_ID -- per-test isolation (full test name, sanitized); resolved at test-run time
//
//! Create disposition: what to do about existence.
enum class TempDirCreate : uint8_t {
	NEVER,     //! must pre-exist (error if absent)
	ON_ABSENT, //! mkdir-p, never clobber (default)
	ALWAYS     //! force-fresh (delete + recreate)
};
//! Destroy disposition: what to do about the dir at end-of-invocation / end-of-test.
enum class TempDirDestroy : uint8_t {
	NEVER,      //! retain
	ON_SUCCESS, //! remove on pass, retain on fail (default)
	ALWAYS      //! remove regardless
};
//! --database-destroy: disposition of loaded database files (DeleteDatabase). Independent of the
//! temp-dir dispositions -- a DB's keep/destroy is not per-se a temp-dir property. OFF restores the
//! old --test-temp-dir behavior where DeleteDatabase was a no-op (so retained temp dirs keep their
//! DB files). ON_SUCCESS mirrors --temp-dir-destroy so a failed test's DB survives for inspection.
enum class DatabaseDestroy : uint8_t {
	ON,        //! always delete DB files
	OFF,       //! never delete -- retain DB files
	ON_SUCCESS //! delete on pass, retain on fail (default)
};

//! Overrides the default base ("duckdb_unittest_tempdir"). Env TEMP_DIR_BASE.
void SetTempDirBase(const string &base);
//! Returns the resolved base ("duckdb_unittest_tempdir" by default).
string GetTempDirBase();
//! Returns the resolved RUN_ID value ("" when RUN_ID is off).
string GetTempDirRunId();
//! --temp-dir-run-id {off|auto|<id>}: off => no RUN_ID level; auto => generated
//! $TS--$RANDTAG (once per invocation); any other value is used literally as the RUN_ID.
void SetRunId(const string &id);
bool SetTempDirRunIdInPath(const string &mode);
//! --temp-dir-test-id {on|off}. Returns false on an unrecognized value.
bool SetTempDirTestId(const string &mode);
//! String setters used by the CLI; return false on an unrecognized value.
bool SetTempDirCreate(const string &mode);
bool SetTempDirDestroy(const string &mode);
//! --database-destroy {on|off|on-success}. Returns false on an unrecognized value.
bool SetDatabaseDestroy(const string &mode);
//! Whether the post-test DB cleanup should delete, given this test's pass/fail (on-success aware).
bool DatabaseDestroyFires(bool success);
//! Materializes $BASE + $RUN_ID once, at startup (main). Fills error + returns false on failure.
bool PrepareTempDir(string &error);
//! Executes the run-id-level destroy disposition (recursive, bottom-up "destroy only what I created").
void DestroyTempDir(bool success);
//! Executes the TEST_ID destroy disposition using THIS test's pass/fail (called at test end).
void DestroyTestTempDir(bool success);
// -----------------------------------------------------------------------------

void SetEmitOnSkip(bool emit);
bool EmitOnSkipEnabled();
void SetDebugInitialize(int value);
void AddRequire(string require);
bool IsRequired(string require);

bool NO_FAIL(QueryResult &result);
bool NO_FAIL(duckdb::unique_ptr<QueryResult> result);

#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))
#define REQUIRE_FAIL(result)    REQUIRE((result)->HasError())

#define COMPARE_CSV(result, csv, header)                                                                               \
	{                                                                                                                  \
		auto res = compare_csv(*result, csv, header);                                                                  \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

#define COMPARE_CSV_COLLECTION(collection, csv, header)                                                                \
	{                                                                                                                  \
		auto res = compare_csv_collection(collection, csv, header);                                                    \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

} // namespace duckdb
