#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include <stdlib.h>

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "sqlite/sqllogic_test_logger.hpp"
#include "test_helpers.hpp"
#include "test_config.hpp"

using namespace duckdb;

#ifdef DUCKDB_FUZZER
namespace duckdb {
int RunAFLFuzzerLoop(int argc, char *argv[]);
}
#endif

static int InitializeEnvironment(int argc_in, char *argv[], int &new_argc, duckdb::unique_ptr<char *[]> &new_argv,
                                 string &test_directory, string &dir) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	test_directory = DUCKDB_ROOT_DIRECTORY;

	auto &test_config = TestConfiguration::Get();
	test_config.Initialize();

	idx_t argc = NumericCast<idx_t>(argc_in);
	new_argc = 0;
	new_argv = duckdb::unique_ptr<char *[]>(new char *[argc]);
	for (idx_t i = 0; i < argc; i++) {
		string argument(argv[i]);
		if (argument == "--test-dir") {
			test_directory = string(argv[++i]);
		} else if (argument == "--test-temp-dir") {
			SetDeleteTestPath(false);
			auto test_dir = string(argv[++i]);
			if (fs->DirectoryExists(test_dir)) {
				fprintf(stderr, "--test-temp-dir cannot point to a directory that already exists (%s)\n",
				        test_dir.c_str());
				return 1;
			}
			SetTestDirectory(test_dir);
		} else if (argument == "--require") {
			AddRequire(string(argv[++i]));
		} else if (!test_config.ParseArgument(argument, argc, argv, i)) {
			new_argv[new_argc] = argv[i];
			new_argc++;
		}
	}
	test_config.ChangeWorkingDirectory(test_directory);

	// delete the testing directory if it exists
	dir = TestCreatePath("");
#ifndef DUCKDB_FUZZER
	try {
		TestDeleteDirectory(dir);
		// create the empty testing directory
		TestCreateDirectory(dir);
	} catch (std::exception &ex) {
		fprintf(stderr, "Failed to create testing directory \"%s\": %s\n", dir.c_str(), ex.what());
		return 1;
	}
#endif

	// Override the home dir so the .duckdb dir is isolated per test process.
#ifdef DUCKDB_WINDOWS
	if (_putenv_s("USERPROFILE", dir.c_str()) != 0) {
		fprintf(stderr, "Failed to set USERPROFILE environment variable\n");
		return 1;
	}
#else
	if (setenv("HOME", dir.c_str(), 1) != 0) {
		fprintf(stderr, "Failed to set HOME environment variable\n");
		return 1;
	}
#endif
	return 0;
}

int main(int argc_in, char *argv[]) {
	int new_argc = 0;
	duckdb::unique_ptr<char *[]> new_argv;
	string test_directory;
	string dir;
	auto init_result = InitializeEnvironment(argc_in, argv, new_argc, new_argv, test_directory, dir);
	if (init_result != 0) {
		return init_result;
	}

#ifdef DUCKDB_FUZZER
	return duckdb::RunAFLFuzzerLoop(argc_in, argv);
#else
	auto &test_config = TestConfiguration::Get();

	if (test_config.GetSkipCompiledTests()) {
		Catch::getMutableRegistryHub().clearTests();
	}
	RegisterSqllogictests();
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

	if (DeleteTestPath()) {
		TestDeleteDirectory(dir);
	}

	return result;
#endif
}
