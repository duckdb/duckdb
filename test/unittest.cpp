#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include <stdlib.h>
#include <fstream>
#include <unordered_set>

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "sqlite/catch_test_reporter.hpp"
#include "sqlite/sqllogic_test_logger.hpp"
#include "sqlite/sqllogic_test_runner.hpp"
#include "test_helpers.hpp"
#include "test_config.hpp"

using namespace duckdb;

namespace {

struct TempDirReclaimer {
	~TempDirReclaimer() {
		DestroyTempDir(success);
	}

	bool success = false;
};

} // namespace

static bool IsSQLLogicTestFile(const string &path) {
	return StringUtil::EndsWith(path, ".test") || StringUtil::EndsWith(path, ".test_slow") ||
	       StringUtil::EndsWith(path, ".test_coverage");
}

static bool TryReadExactSQLLogicTestFilter(const vector<string> &input_files, vector<string> &test_paths,
                                           string &error) {
	if (input_files.empty()) {
		return false;
	}
	auto fs = FileSystem::CreateLocal();
	unordered_set<string> seen_paths;
	for (auto &input_file : input_files) {
		std::ifstream file(input_file.c_str());
		if (!file.is_open()) {
			return false;
		}
		string line;
		while (std::getline(file, line)) {
			StringUtil::Trim(line);
			if (line.empty() || line[0] == '#') {
				continue;
			}
			if (!IsSQLLogicTestFile(line)) {
				return false;
			}
			if (!fs->FileExists(line)) {
				error = "Unable to find sqllogictest file from -f/--input-file: " + line;
				return true;
			}
			if (seen_paths.insert(line).second) {
				test_paths.push_back(line);
			}
		}
	}
	return !test_paths.empty();
}

int main(int argc_in, char *argv[]) {
	string test_directory = DUCKDB_ROOT_DIRECTORY;

	// route the sqllogictest runner's verdicts into the Catch session
	static CatchTestReporter catch_reporter;
	TestReporter::Set(catch_reporter);

	auto &test_config = TestConfiguration::Get();
	try {
		// Applies every DUCKDB_TEST_<NAME> fallback, so a bad env value surfaces here.
		test_config.Initialize();
	} catch (std::exception &ex) {
		fprintf(stderr, "%s\n", ex.what());
		return 1;
	}
	bool keep_home = false;
	bool use_stdin = false;
	vector<string> input_files;
	unordered_set<idx_t> input_file_arg_indices;

	// The --temp-dir-* family, --run-id and --env-passthrough live in TestConfiguration's option table,
	// so ParseArgument handles them below and Initialize() already applied their env fallbacks. What
	// remains here is what that table cannot express.
	idx_t argc = NumericCast<idx_t>(argc_in);
	int new_argc = 0;
	auto new_argv = duckdb::unique_ptr<char *[]>(new char *[argc]);
	for (idx_t i = 0; i < argc; i++) {
		string argument(argv[i]);
		if (argument == "--test-dir") {
			test_directory = string(argv[++i]);
		} else if (argument == "--require") {
			AddRequire(string(argv[++i]));
		} else if (argument == "--emit-on-skip") {
			SetEmitOnSkip(true);
		} else if (argument == "--keep-home") {
			keep_home = true;
		} else if (argument == "--stdin") {
			use_stdin = true;
		} else if (argument == "--emit-test-events") {
			SetEmitTestEvents(true);
		} else {
			try {
				if (!test_config.ParseArgument(argument, argc, argv, i)) {
					if ((argument == "-f" || argument == "--input-file") && i + 1 < argc) {
						input_files.push_back(argv[i + 1]);
						input_file_arg_indices.insert(new_argc);
						input_file_arg_indices.insert(new_argc + 1);
					}
					new_argv[new_argc] = argv[i];
					new_argc++;
				}
			} catch (std::exception &ex) {
				fprintf(stderr, "%s\n", ex.what());
				return 1;
			}
		}
	}

	// Before any temp-dir prep or test runs: a named-but-absent var kills the whole invocation, unlike
	// require-env's per-test skip.
	string env_error;
	if (!ValidateEnvPassthrough(env_error) || !test_config.ValidateTestEnv(env_error)) {
		fprintf(stderr, "%s\n", env_error.c_str());
		return 1;
	}

	test_config.ChangeWorkingDirectory(test_directory);

	vector<string> exact_sqllogic_tests;
	string exact_sqllogic_error;
	bool exact_sqllogic_filter =
	    TryReadExactSQLLogicTestFilter(input_files, exact_sqllogic_tests, exact_sqllogic_error);
	if (!exact_sqllogic_error.empty()) {
		fprintf(stderr, "%s\n", exact_sqllogic_error.c_str());
		return 1;
	}
	string exact_sqllogic_test_filter = "*";
	if (exact_sqllogic_filter) {
		int filtered_argc = 0;
		for (int i = 0; i < new_argc; i++) {
			if (input_file_arg_indices.find(i) != input_file_arg_indices.end()) {
				continue;
			}
			new_argv[filtered_argc++] = new_argv[i];
		}
		new_argv[filtered_argc++] = const_cast<char *>(exact_sqllogic_test_filter.c_str());
		new_argc = filtered_argc;
	}

	// Resolve + provision $BASE/[RUN_ID] (the TEST_ID level is materialized later, on the
	// per-test path, once a test name is known).
	string prep_error;
	if (!PrepareTempDir(prep_error)) {
		fprintf(stderr, "Failed to prepare temp directory: %s\n", prep_error.c_str());
		return 1;
	}
	TempDirReclaimer temp_dir_reclaimer;

	// Capture env now that all --temp-dir-* context (base/run-id/create) is final; must run
	// after PrepareTempDir so TEMP_DIR reflects the materialized run root.
	test_config.UpdateEnvironment();

	string data_dir_error;
	if (!test_config.ValidateDataDirs(data_dir_error)) {
		fprintf(stderr, "%s\n", data_dir_error.c_str());
		return 1;
	}

	// Set ONCE per invocation, never per-test, so ~/.duckdb (extensions, secrets) is isolated without
	// landing inside a {TEST_DIR} a test whitelists. Absolute because a relative home would shift under
	// any test chdir.
	string home_dir = TestMakeAbsolute(GetTempDirHome(), TestGetCurrentDirectory());

	if (!keep_home) {
#ifdef DUCKDB_WINDOWS
		if (_putenv_s("USERPROFILE", home_dir.c_str()) != 0) {
			fprintf(stderr, "Failed to set USERPROFILE environment variable\n");
			return 1;
		}
#else
		if (setenv("HOME", home_dir.c_str(), 1) != 0) {
			fprintf(stderr, "Failed to set HOME environment variable\n");
			return 1;
		}
#endif
	}

	if (use_stdin || exact_sqllogic_filter || test_config.GetSkipCompiledTests()) {
		Catch::getMutableRegistryHub().clearTests();
	}
	if (use_stdin) {
		RegisterSqllogictestStdin();
	} else if (exact_sqllogic_filter) {
		RegisterSqllogictests(exact_sqllogic_tests);
	} else {
		RegisterSqllogictests();
	}
	int result = Catch::Session().run(new_argc, new_argv.get());

	std::string failures_summary = FailureSummary::GetFailureSummary();
	if (!failures_summary.empty()) {
		auto description = test_config.GetDescription();
		if (!description.empty()) {
			std::cerr << "\n====================================================" << std::endl;
			std::cerr << "====================  TEST INFO  ===================" << std::endl;
			std::cerr << "====================================================\n" << std::endl;
			std::cerr << description << std::endl;
		}
		std::cerr << "\n====================================================" << std::endl;
		std::cerr << "================  FAILURES SUMMARY  ================" << std::endl;
		std::cerr << "====================================================\n" << std::endl;
		std::cerr << failures_summary;
	}
	std::string skip_reason_summary = SQLLogicTestRunner::GetSkipReasonSummary();
	if (!skip_reason_summary.empty()) {
		std::cerr << "\n"
		          << "Skipped tests for the following reasons:" << std::endl;
		std::cerr << skip_reason_summary;
	}

	// Execute the run-id-level destroy disposition ($BASE/[RUN_ID]); pass/fail-aware, recursive.
	temp_dir_reclaimer.success = result == 0;

	return result;
}
