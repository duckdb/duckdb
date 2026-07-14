#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include <stdlib.h>

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "sqlite/sqllogic_test_logger.hpp"
#include "sqlite/sqllogic_test_runner.hpp"
#include "test_helpers.hpp"
#include "test_config.hpp"

using namespace duckdb;

int main(int argc_in, char *argv[]) {
	string test_directory = DUCKDB_ROOT_DIRECTORY;

	auto &test_config = TestConfiguration::Get();
	test_config.Initialize();
	bool keep_home = false;
	bool use_stdin = false;

	idx_t argc = NumericCast<idx_t>(argc_in);
	int new_argc = 0;
	auto new_argv = duckdb::unique_ptr<char *[]>(new char *[argc]);
	for (idx_t i = 0; i < argc; i++) {
		string argument(argv[i]);
		if (argument == "--test-dir") {
			test_directory = string(argv[++i]);
		} else if (argument == "--temp-dir-base") {
			SetTempDirBase(string(argv[++i]));
		} else if (argument == "--run-id") {
			SetRunId(string(argv[++i]));
		} else if (argument == "--temp-dir-run-id") {
			if (!SetTempDirRunIdInPath(string(argv[++i]))) {
				fprintf(stderr, "--temp-dir-run-id expects one of: on, off\n");
				return 1;
			}
		} else if (argument == "--temp-dir-test-id") {
			if (!SetTempDirTestId(string(argv[++i]))) {
				fprintf(stderr, "--temp-dir-test-id expects one of: on, off\n");
				return 1;
			}
		} else if (argument == "--temp-dir-create") {
			if (!SetTempDirCreate(string(argv[++i]))) {
				fprintf(stderr, "--temp-dir-create expects one of: never, on-absent, always\n");
				return 1;
			}
		} else if (argument == "--temp-dir-destroy") {
			if (!SetTempDirDestroy(string(argv[++i]))) {
				fprintf(stderr, "--temp-dir-destroy expects one of: never, on-success, always\n");
				return 1;
			}
		} else if (argument == "--database-destroy") {
			if (!SetDatabaseDestroy(string(argv[++i]))) {
				fprintf(stderr, "--database-destroy expects one of: on, off, on-success\n");
				return 1;
			}
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
					new_argv[new_argc] = argv[i];
					new_argc++;
				}
			} catch (std::exception &ex) {
				fprintf(stderr, "%s\n", ex.what());
				return 1;
			}
		}
	}
	test_config.ChangeWorkingDirectory(test_directory);

	// Resolve + provision $BASE/[RUN_ID] per the create disposition (the TEST_ID level is
	// materialized later, on the per-test path, once a test name is known).
	string prep_error;
	if (!PrepareTempDir(prep_error)) {
		fprintf(stderr, "Failed to prepare temp directory: %s\n", prep_error.c_str());
		return 1;
	}
	// Capture env now that all --temp-dir-* context (base/run-id/create) is final; must run
	// after PrepareTempDir so TEMP_DIR reflects the materialized run root.
	test_config.UpdateEnvironment();

	// HOME points at the dedicated sandbox (a sibling of the run root), absolute and set ONCE for the
	// whole invocation -- no per-test override. This isolates ~/.duckdb (extensions, secrets) without
	// ever landing inside a {TEST_DIR} that a test whitelists via allowed_directories. Absolute because a
	// relative home is meaningless and would shift under any test chdir.
	string home_dir = GetTempDirHome();
	{
		auto local_fs = FileSystem::CreateLocal();
		if (!local_fs->IsPathAbsolute(home_dir)) {
			home_dir = local_fs->JoinPath(TestGetCurrentDirectory(), home_dir);
		}
	}

	// A remote base cannot be a home dir; skip the override there.
	bool remote_base = FileSystem::IsRemoteFile(GetTempDirBase());
	if (!keep_home && !remote_base) {
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

	if (use_stdin || test_config.GetSkipCompiledTests()) {
		Catch::getMutableRegistryHub().clearTests();
	}
	if (use_stdin) {
		RegisterSqllogictestStdin();
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
	DestroyTempDir(result == 0);

	return result;
}
