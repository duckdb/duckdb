// #define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "compare_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/csv_scanner/string_value_scanner.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "test_config.hpp"
#include "pid.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <fstream>

#define TESTING_DIRECTORY_NAME "duckdb_unittest_tempdir"

namespace duckdb {
static case_insensitive_set_t required_requires;
static bool delete_test_path = true;
static bool emit_on_skip = false; // --emit-on-skip opt-in: emit a [SKIP_TEST] marker per skipped test

// --temp-dir-* family state: TEMP_DIR = $BASE/[RUN_ID]/[TEST_ID] (see DISPOSITIONS.md).
static string temp_dir_base = TESTING_DIRECTORY_NAME; // $BASE; default duckdb_unittest_tempdir
static bool temp_dir_run_id_in_path = true;           // --temp-dir-run-id {on|off}: RUN_ID as a path level
static string temp_dir_run_id;                        // RUN_ID value (--run-id, or generated when absent)
static bool temp_dir_test_id = true;
static TempDirCreate temp_dir_create = TempDirCreate::ON_ABSENT;
static TempDirDestroy temp_dir_destroy = TempDirDestroy::ON_SUCCESS;
// --database-destroy: independent of the temp-dir dispositions (see test_helpers.hpp).
static DatabaseDestroy database_destroy = DatabaseDestroy::ON_SUCCESS;
// Levels THIS invocation created (outermost..leaf), split by lifecycle so each
// destroy step reclaims only what its own step made.
static vector<string> temp_dir_run_created_levels;  // $BASE..$RUN_ID (main / Prepare|DestroyTempDir)
static vector<string> temp_dir_test_created_levels; // $TEST_ID (per-test path)
static string temp_dir_active_test_leaf;            // currently-materialized TEST_ID dir ("" when none)

bool NO_FAIL(QueryResult &result) {
	if (result.HasError()) {
		fprintf(stderr, "Query failed with message: %s\n", result.GetError().c_str());
	}
	return !result.HasError();
}

bool NO_FAIL(duckdb::unique_ptr<QueryResult> result) {
	return NO_FAIL(*result);
}

void TestDeleteDirectory(string path) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	try {
		if (fs->DirectoryExists(path)) {
			fs->RemoveDirectory(path);
		}
	} catch (...) {
	}
}

void TestDeleteFile(string path) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	try {
		fs->TryRemoveFile(path);
	} catch (...) {
	}
}

void TestChangeDirectory(string path) {
	// set the base path for the tests
	FileSystem::SetWorkingDirectory(path);
}

string TestGetCurrentDirectory() {
	return FileSystem::GetWorkingDirectory();
}

void DeleteDatabase(string path) {
	if (database_destroy == DatabaseDestroy::OFF) {
		return; // retain: never touch DB files
	}
	TestDeleteFile(path);
	TestDeleteFile(path + ".wal");
}

void TestCreateDirectory(string path) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	fs->CreateDirectory(path);
}

string TestJoinPath(string path1, string path2) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return fs->JoinPath(path1, path2);
}

void SetEmitOnSkip(bool emit) {
	emit_on_skip = emit;
}

bool EmitOnSkipEnabled() {
	return emit_on_skip;
}

void AddRequire(string require) {
	required_requires.insert(require);
}

bool IsRequired(string require) {
	return required_requires.count(require);
}

// -----------------------------------------------------------------------------
// --temp-dir-* family
//

static string ResolveRunId(); // resolved RUN_ID value (always set; generated even when off); defined below

void SetTempDirBase(const string &base) {
	temp_dir_base = base;
}

string GetTempDirBase() {
	return temp_dir_base;
}

string GetTempDirRunId() {
	return ResolveRunId();
}

void SetRunId(const string &id) {
	// The run identity → RUN_ID env (and the path segment when --temp-dir-run-id on). "auto"
	// (or absent) generates one on first resolve; any other value is a caller-supplied fixed id
	// (pytest passes one shared id so a run's many unittest batches co-locate).
	temp_dir_run_id = (id == "auto") ? "" : id;
}

bool SetTempDirRunIdInPath(const string &mode) {
	if (mode == "on") {
		temp_dir_run_id_in_path = true;
	} else if (mode == "off") {
		temp_dir_run_id_in_path = false;
	} else {
		return false;
	}
	return true;
}

bool SetTempDirTestId(const string &mode) {
	if (mode == "on") {
		temp_dir_test_id = true;
	} else if (mode == "off") {
		temp_dir_test_id = false;
	} else {
		return false;
	}
	return true;
}

bool SetTempDirCreate(const string &mode) {
	if (mode == "never") {
		temp_dir_create = TempDirCreate::NEVER;
	} else if (mode == "on-absent") {
		temp_dir_create = TempDirCreate::ON_ABSENT;
	} else if (mode == "always") {
		temp_dir_create = TempDirCreate::ALWAYS;
	} else {
		return false;
	}
	return true;
}

bool SetTempDirDestroy(const string &mode) {
	if (mode == "never") {
		temp_dir_destroy = TempDirDestroy::NEVER;
	} else if (mode == "on-success") {
		temp_dir_destroy = TempDirDestroy::ON_SUCCESS;
	} else if (mode == "always") {
		temp_dir_destroy = TempDirDestroy::ALWAYS;
	} else {
		return false;
	}
	return true;
}

bool SetDatabaseDestroy(const string &mode) {
	if (mode == "on") {
		database_destroy = DatabaseDestroy::ON;
	} else if (mode == "off") {
		database_destroy = DatabaseDestroy::OFF;
	} else if (mode == "on-success") {
		database_destroy = DatabaseDestroy::ON_SUCCESS;
	} else {
		return false;
	}
	return true;
}

bool DatabaseDestroyFires(bool success) {
	switch (database_destroy) {
	case DatabaseDestroy::ON:
		return true;
	case DatabaseDestroy::ON_SUCCESS:
		return success;
	case DatabaseDestroy::OFF:
	default:
		return false;
	}
}

// Join a leaf onto a parent. Remote parents are appended as pure strings (no VFS/mkdir);
// local parents use the platform path join. An empty leaf yields the parent unchanged.
static string JoinTempLevel(const string &parent, const string &leaf) {
	if (leaf.empty()) {
		return parent;
	}
	if (FileSystem::IsRemoteFile(parent)) {
		if (parent.empty()) {
			return leaf;
		}
		return (parent.back() == '/') ? parent + leaf : parent + "/" + leaf;
	}
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return fs->JoinPath(parent, leaf);
}

// RUN_ID auto value: sortable timestamp + a memorable mnemonic, e.g.
// 2026-07-01T14-30-22Z--crimson-torus-07 (UTC). Mirrors the pytest driver's run-id shape
// (test/py/driver/mnemonic.py: <ISO-basic>--<word>-<word>-<NN>) but uses DISTINCT word
// banks — colors + shapes here vs the driver's adjectives + animals — so the run id's
// vocabulary reveals its source: a color-shape run id was minted by the unittest binary, an
// adjective-animal one by pytest.
static const char *const TEMP_DIR_COLORS[] = {"crimson", "scarlet", "azure",   "cobalt", "teal",    "olive",
                                              "maroon",  "indigo",  "violet",  "coral",  "ochre",   "umber",
                                              "jade",    "ivory",   "slate",   "sienna", "magenta", "cyan",
                                              "khaki",   "russet",  "saffron", "mauve",  "bronze",  "copper"};
static const char *const TEMP_DIR_SHAPES[] = {"cube",    "prism",   "sphere",  "cone",  "torus",   "helix",
                                              "wedge",   "disc",    "ring",    "arch",  "spiral",  "prong",
                                              "vane",    "cusp",    "node",    "facet", "obelisk", "pylon",
                                              "lattice", "rhombus", "spindle", "dome",  "ingot",   "girder"};

static string GenerateAutoRunId() {
	std::time_t now = std::time(nullptr);
	char ts[24] = {0};
	std::strftime(ts, sizeof(ts), "%Y-%m-%dT%H-%M-%SZ", std::gmtime(&now));
	// hugeint_t carries independent entropy in lower/upper; index each bank + the 2 digits.
	auto uuid = UUID::GenerateRandomUUID();
	auto n_colors = sizeof(TEMP_DIR_COLORS) / sizeof(TEMP_DIR_COLORS[0]);
	auto n_shapes = sizeof(TEMP_DIR_SHAPES) / sizeof(TEMP_DIR_SHAPES[0]);
	const char *color = TEMP_DIR_COLORS[uuid.lower % n_colors];
	const char *shape = TEMP_DIR_SHAPES[static_cast<uint64_t>(uuid.upper) % n_shapes];
	auto nn = static_cast<int>((uuid.lower >> 32) % 100);
	string digits = (nn < 10 ? "0" : "") + std::to_string(nn);
	return string(ts) + "--" + color + "-" + shape + "-" + digits;
}

// Resolved RUN_ID value ("" when off). AUTO generates once and caches for the invocation.
static string ResolveRunId() {
	// The RUN_ID value is always resolved — the caller's --run-id, or a generated one when none
	// was given — so the RUN_ID env var is populated even when run-id is not a TEMP_DIR path
	// segment. Path inclusion is gated separately in ResolveRunIdRoot().
	if (temp_dir_run_id.empty()) {
		temp_dir_run_id = GenerateAutoRunId();
	}
	return temp_dir_run_id;
}

// $BASE/[RUN_ID] -- the run-id root; stable across the invocation. String-only (no IO).
static string ResolveRunIdRoot() {
	// run-id is a TEMP_DIR path segment only when --temp-dir-run-id on; the value still exists
	// as the RUN_ID env var either way.
	if (!temp_dir_run_id_in_path) {
		return temp_dir_base;
	}
	return JoinTempLevel(temp_dir_base, ResolveRunId());
}

// The HOME sandbox: a SIBLING of the run-id root ("<run-root>-home"), never *inside* it. HOME must
// not fall within any {TEST_DIR} a test whitelists via allowed_directories -- else ~/.duckdb
// (extensions, secrets) becomes an allowed path and permission tests (e.g. INSTALL-is-denied) break.
// A sibling holds in both regimes: default {TEST_DIR}=$BASE/$RUN_ID/$TEST_ID (sibling under $RUN_ID),
// managed {TEST_DIR}=$BASE (sibling of $BASE). Materialized by PrepareTempDir, reclaimed by DestroyTempDir.
static string ResolveHomeDir() {
	return ResolveRunIdRoot() + "-home";
}

string GetTempDirHome() {
	return ResolveHomeDir();
}

// Test identity: sanitize the FULL test name to a single filesystem- and shell-safe path component.
// Shared by the TEST_ID env var and the temp-dir leaf (ResolveTestId) so they agree.
string TestNameToId(const string &name) {
	// Keep the whole name, including any body suffix (.test / .test_slow / .test_coverage): dropping it
	// would collide siblings that differ only by suffix (foo.test vs foo.test_slow) onto one TEST_ID and
	// one temp dir. Map every char outside [A-Za-z0-9_-] to '_' -- notably '.' -> '_', so the suffix
	// survives as a distinct, path-safe segment (foo_test vs foo_test_slow). SQL names are path-like
	// (only '/' and '.' need mapping); C++ Catch2 case names are free-form ("Test replaying mismatching
	// WAL files"), so spaces/quotes/parens/etc. must go too -- else the leaf is unusable as an unquoted
	// shell path in tests that shell out via system() (e.g. `cp $TEST_DIR/a $TEST_DIR/b`).
	string id;
	id.reserve(name.size());
	for (char c : name) {
		bool safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
		id += safe ? c : '_';
	}
	return id;
}

// TEST_ID for the currently-executing test ("" when TEST_ID off or no test is active,
// e.g. at startup -- the test name isn't known until a test runs).
static string ResolveTestId() {
	if (!temp_dir_test_id) {
		return "";
	}
	string name;
	try {
		name = Catch::getResultCapture().getCurrentTestName();
	} catch (...) {
		name = "";
	}
	if (name.empty()) {
		return "";
	}
	return TestNameToId(name);
}

// Full resolved path $BASE/[RUN_ID]/[TEST_ID]. String-only (no IO).
static string ResolveTempDirPath() {
	return JoinTempLevel(ResolveRunIdRoot(), ResolveTestId());
}

static bool DirectoryIsEmpty(FileSystem &fs, const string &path) {
	bool empty = true;
	fs.ListFiles(path, [&](const string &, bool) { empty = false; });
	return empty;
}

// mkdir-p `path`, recording into `created` (outermost..leaf) which levels did not
// previously exist so the matching destroy step removes only what it created. The walk
// stops at the first pre-existing ancestor, so shared/parent levels created by an earlier
// lifecycle step (or a prior run) are never recorded here.
static void RecordAndCreateLevels(FileSystem &fs, const string &path, vector<string> &created) {
	vector<string> to_create; // innermost first
	string p = path;
	while (!p.empty() && !fs.DirectoryExists(p)) {
		to_create.push_back(p);
		auto parent = StringUtil::GetFilePath(p);
		if (parent.empty() || parent == p) {
			break;
		}
		p = parent;
	}
	fs.CreateDirectoriesRecursive(path);
	std::reverse(to_create.begin(), to_create.end()); // outermost..leaf
	created = to_create;
}

// Recursive bottom-up reclaim: remove `leaf` (and its contents), then walk up `created`
// removing each ancestor iff it is empty and this step created it; stop at the first
// non-empty / not-this-step-created level. Shared by both destroy lifecycle steps.
static void ReclaimLevels(FileSystem &fs, const string &leaf, const vector<string> &created) {
	if (fs.DirectoryExists(leaf)) {
		try {
			fs.RemoveDirectory(leaf); // recursive
		} catch (...) {
		}
	}
	for (idx_t idx = created.size(); idx-- > 0;) {
		const auto &level = created[idx];
		if (level == leaf) {
			continue; // already removed above
		}
		if (!fs.DirectoryExists(level)) {
			continue;
		}
		if (!DirectoryIsEmpty(fs, level)) {
			break; // shared with someone else's content -> stop
		}
		try {
			fs.RemoveDirectory(level);
		} catch (...) {
			break;
		}
	}
}

static bool DestroyFires(TempDirDestroy disposition, bool success) {
	switch (disposition) {
	case TempDirDestroy::ON_SUCCESS:
		return success;
	case TempDirDestroy::ALWAYS:
		return true;
	case TempDirDestroy::NEVER:
	default:
		return false;
	}
}

bool PrepareTempDir(string &error) {
	if (temp_dir_base.empty()) {
		error = "the --temp-dir-* family requires a non-empty base";
		return false;
	}
	// Remote clamp: a remote base forces create=NEVER + destroy=NEVER; nothing to
	// materialize here (object stores create-on-write; the test owns materialization).
	if (FileSystem::IsRemoteFile(temp_dir_base)) {
		return true;
	}
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	string root = ResolveRunIdRoot(); // $BASE/[RUN_ID]
	switch (temp_dir_create) {
	case TempDirCreate::NEVER:
		if (!fs->DirectoryExists(root)) {
			error = "temp dir does not exist and --temp-dir-create=never: " + root;
			return false;
		}
		break;
	case TempDirCreate::ON_ABSENT:
		RecordAndCreateLevels(*fs, root, temp_dir_run_created_levels);
		break;
	case TempDirCreate::ALWAYS:
		if (fs->DirectoryExists(root)) {
			fs->RemoveDirectory(root); // recursive
		}
		RecordAndCreateLevels(*fs, root, temp_dir_run_created_levels);
		break;
	}
	// HOME sandbox (sibling of the run root). Always materialized -- HOME must exist regardless of the
	// temp-dir create disposition (some code errors if home_directory is set but absent).
	string home = ResolveHomeDir();
	if (!fs->DirectoryExists(home)) {
		fs->CreateDirectoriesRecursive(home);
	}
	return true;
}

void DestroyTempDir(bool success) {
	// Remote clamp: destroy is forced to NEVER for remote bases.
	if (FileSystem::IsRemoteFile(temp_dir_base)) {
		return;
	}
	if (!DestroyFires(temp_dir_destroy, success)) {
		return;
	}
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	// Remove the HOME sandbox sibling first, so the $BASE-empty check in the ancestor walk below can
	// still reclaim the base when the run owns it.
	string home = ResolveHomeDir();
	if (fs->DirectoryExists(home)) {
		try {
			fs->RemoveDirectory(home); // recursive
		} catch (...) {
		}
	}
	// Remove the run-id root recursively (subsumes any leftover per-test dirs), then reclaim
	// this-run-created ancestors up to (but not past) a pre-existing $BASE.
	ReclaimLevels(*fs, ResolveRunIdRoot(), temp_dir_run_created_levels);
}

void DestroyTestTempDir(bool success) {
	// TEST_ID destroy: fired at test end using THIS test's pass/fail. Reclaims only the
	// $TEST_ID level this test's path created -- $RUN_ID/$BASE pre-existed (created by
	// PrepareTempDir at startup), so ReclaimLevels stops there.
	if (!FileSystem::IsRemoteFile(temp_dir_base)) {
		string leaf = temp_dir_active_test_leaf;
		if (!leaf.empty() && DestroyFires(temp_dir_destroy, success)) {
			duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
			ReclaimLevels(*fs, leaf, temp_dir_test_created_levels);
		}
	}
	temp_dir_active_test_leaf.clear();
	temp_dir_test_created_levels.clear();
}
// -----------------------------------------------------------------------------

string TestDirectoryPath() {
	string path = ResolveTempDirPath(); // $BASE/[RUN_ID]/[TEST_ID]
	// Remote base: pure string, no mkdir (the test owns materialization).
	if (FileSystem::IsRemoteFile(temp_dir_base)) {
		return path;
	}
	// $TEST_ID is resolved and materialized here, lazily, the first time a given test's
	// dir is requested (the test name isn't known until a test runs; $BASE/$RUN_ID were
	// already materialized by PrepareTempDir at startup). We record only the levels this
	// per-test path creates so DestroyTestTempDir reclaims exactly those.
	string root = ResolveRunIdRoot();
	if (path != root && path != temp_dir_active_test_leaf) {
		temp_dir_active_test_leaf = path;
		temp_dir_test_created_levels.clear();
		if (temp_dir_create != TempDirCreate::NEVER) {
			duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
			if (!fs->DirectoryExists(path)) {
				RecordAndCreateLevels(*fs, path, temp_dir_test_created_levels);
			}
		}
	}
	return path;
}

void SetDeleteTestPath(bool delete_path) {
	delete_test_path = delete_path;
}

bool DeleteTestPath() {
	return delete_test_path;
}

static bool emit_test_events = false;

void SetEmitTestEvents(bool emit) {
	emit_test_events = emit;
}

bool EmitTestEventsEnabled() {
	return emit_test_events;
}

void ClearTestDirectory() {
	if (!DeleteTestPath()) {
		return;
	}
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto test_dir = TestDirectoryPath();
	// try to clear any files we created in the test directory
	fs->ListFiles(test_dir, [&](const string &file, bool is_dir) {
		auto full_path = fs->JoinPath(test_dir, file);
		try {
			if (is_dir) {
				fs->RemoveDirectory(full_path);
			} else {
				fs->RemoveFile(full_path);
			}
		} catch (...) {
			// skip
		}
	});
}

string TestCreatePath(string suffix) {
	return TestJoinPath(TestDirectoryPath(), suffix);
}

bool TestIsInternalError(unordered_set<string> &internal_error_messages, const string &error) {
	for (auto &error_message : internal_error_messages) {
		if (StringUtil::Contains(error, error_message)) {
			return true;
		}
	}
	return false;
}

unique_ptr<DBConfig> GetTestConfig() {
	auto &test_config = TestConfiguration::Get();

	auto result = make_uniq<DBConfig>();
#ifndef DUCKDB_ALTERNATIVE_VERIFY
	result->options.checkpoint_wal_size = test_config.GetCheckpointWALSize();
	result->options.checkpoint_on_shutdown = test_config.GetCheckpointOnShutdown();
#else
	result->options.checkpoint_on_shutdown = false;
#endif
	result->options.abort_on_wal_failure = true;
#ifdef DUCKDB_RUN_SLOW_VERIFIERS
	// This mode isn't slow, but we want test coverage both when it's enabled
	// and when it's not, so we enable only when DUCKDB_RUN_SLOW_VERIFIERS is set.
	result->options.trim_free_blocks = true;
#endif
	result->SetOptionByName("allow_unsigned_extensions", true);
	auto storage_version = test_config.GetStorageVersion();
	if (!storage_version.empty()) {
		result->options.storage_compatibility = StorageCompatibility::FromString(storage_version);
	}

	auto max_threads = test_config.GetMaxThreads();
	if (max_threads.IsValid()) {
		result->options.maximum_threads = max_threads.GetIndex();
	}

	auto block_alloc_size = test_config.GetBlockAllocSize();
	if (block_alloc_size.IsValid()) {
		Storage::VerifyBlockAllocSize(block_alloc_size.GetIndex());
		result->SetOptionByName("default_block_size", Value::UBIGINT(block_alloc_size.GetIndex()));
	}

	result->options.debug_initialize = test_config.GetDebugInitialize();
	result->SetOptionByName("debug_verify_vector", EnumUtil::ToString(test_config.GetVectorVerification()));
	return result;
}

bool CHECK_COLUMN(QueryResult &result_, size_t column_number, vector<duckdb::Value> values) {
	if (result_.type == QueryResultType::STREAM_RESULT) {
		fprintf(stderr, "Unexpected stream query result in CHECK_COLUMN\n");
		return false;
	}
	auto &result = (MaterializedQueryResult &)result_;
	if (result.HasError()) {
		fprintf(stderr, "Query failed with message: %s\n", result.GetError().c_str());
		return false;
	}
	if (result.names.size() != result.types.size()) {
		// column names do not match
		result.Print();
		return false;
	}
	if (values.empty()) {
		if (result.RowCount() != 0) {
			result.Print();
			return false;
		} else {
			return true;
		}
	}
	if (result.RowCount() == 0) {
		result.Print();
		return false;
	}
	if (column_number >= result.types.size()) {
		result.Print();
		return false;
	}
	for (idx_t row_idx = 0; row_idx < values.size(); row_idx++) {
		auto value = result.GetValue(column_number, row_idx);
		// NULL <> NULL, hence special handling
		if (value.IsNull() && values[row_idx].IsNull()) {
			continue;
		}

		if (!Value::DefaultValuesAreEqual(value, values[row_idx])) {
			// FAIL("Incorrect result! Got " + vector.GetValue(j).ToString()
			// +
			//      " but expected " + values[i + j].ToString());
			result.Print();
			return false;
		}
	}
	return true;
}

bool CHECK_COLUMN(duckdb::unique_ptr<duckdb::QueryResult> &result, size_t column_number, vector<duckdb::Value> values) {
	if (result->type == QueryResultType::STREAM_RESULT) {
		auto &stream = (StreamQueryResult &)*result;
		result = stream.Materialize();
	}
	return CHECK_COLUMN(*result, column_number, values);
}

bool CHECK_COLUMN(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result, size_t column_number,
                  vector<duckdb::Value> values) {
	return CHECK_COLUMN((QueryResult &)*result, column_number, values);
}

string compare_csv(duckdb::QueryResult &result, string csv, bool header) {
	D_ASSERT(result.type == QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = (MaterializedQueryResult &)result;
	if (materialized.HasError()) {
		fprintf(stderr, "Query failed with message: %s\n", materialized.GetError().c_str());
		return materialized.GetError();
	}
	string error;
	if (!compare_result(csv, materialized.Collection(), materialized.types, header, error)) {
		return error;
	}
	return "";
}

string compare_csv_collection(duckdb::ColumnDataCollection &collection, string csv, bool header) {
	string error;
	if (!compare_result(csv, collection, collection.Types(), header, error)) {
		return error;
	}
	return "";
}

string show_diff(DataChunk &left, DataChunk &right) {
	if (left.ColumnCount() != right.ColumnCount()) {
		return StringUtil::Format("Different column counts: %d vs %d", (int)left.ColumnCount(),
		                          (int)right.ColumnCount());
	}
	if (left.size() != right.size()) {
		return StringUtil::Format("Different sizes: %zu vs %zu", left.size(), right.size());
	}
	string difference;
	for (size_t i = 0; i < left.ColumnCount(); i++) {
		bool has_differences = false;
		auto &left_vector = left.data[i];
		auto &right_vector = right.data[i];
		string left_column = StringUtil::Format("Result\n------\n%s [", left_vector.GetType().ToString().c_str());
		string right_column = StringUtil::Format("Expect\n------\n%s [", right_vector.GetType().ToString().c_str());
		if (left_vector.GetType() == right_vector.GetType()) {
			for (size_t j = 0; j < left.size(); j++) {
				auto left_value = left_vector.GetValue(j);
				auto right_value = right_vector.GetValue(j);
				if (!Value::DefaultValuesAreEqual(left_value, right_value)) {
					left_column += left_value.ToString() + ",";
					right_column += right_value.ToString() + ",";
					has_differences = true;
				} else {
					left_column += "_,";
					right_column += "_,";
				}
			}
		} else {
			left_column += "...";
			right_column += "...";
		}
		left_column += "]\n";
		right_column += "]\n";
		if (has_differences) {
			difference += StringUtil::Format("Difference in column %d:\n", i);
			difference += left_column + "\n" + right_column + "\n";
		}
	}
	return difference;
}

//! Compares the result of a pipe-delimited CSV with the given DataChunk
//! Returns true if they are equal, and stores an error_message otherwise
bool compare_result(string csv, ColumnDataCollection &collection, vector<LogicalType> sql_types, bool has_header,
                    string &error_message) {
	D_ASSERT(collection.Count() == 0 || collection.Types().size() == sql_types.size());

	// create the csv on disk
	auto csv_path = TestCreatePath("__test_csv_path.csv");
	std::ofstream f(csv_path);
	f << csv;
	f.close();

	// set up the CSV reader
	CSVReaderOptions options;
	options.auto_detect = false;
	options.dialect_options.state_machine_options.delimiter = {"|"};
	options.dialect_options.header = has_header;
	options.dialect_options.state_machine_options.quote = '\"';
	options.dialect_options.state_machine_options.escape = '\"';
	options.file_path = csv_path;
	options.dialect_options.num_cols = sql_types.size();
	// set up the intermediate result chunk
	DataChunk parsed_result;
	parsed_result.Initialize(Allocator::DefaultAllocator(), sql_types);

	DuckDB db;
	Connection con(db);
	MultiFileOptions file_options;
	auto scanner_ptr = StringValueScanner::GetCSVScanner(*con.context, options, file_options);
	auto &scanner = *scanner_ptr;
	ColumnDataCollection csv_data_collection(*con.context, sql_types);
	while (!scanner.FinishedIterator()) {
		// parse a chunk from the CSV file
		try {
			parsed_result.Reset();
			scanner.Flush(parsed_result);
		} catch (std::exception &ex) {
			error_message = "Could not parse CSV: " + string(ex.what());
			return false;
		}
		if (parsed_result.size() == 0) {
			break;
		}
		csv_data_collection.Append(parsed_result);
	}
	string error;
	if (!ColumnDataCollection::ResultEquals(collection, csv_data_collection, error_message)) {
		return false;
	}
	return true;
}

} // namespace duckdb
