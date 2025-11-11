#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "sqlite/sqllogic_test_logger.hpp"
#include "test_helpers.hpp"
#include "test_config.hpp"

using namespace duckdb;

int main(int argc_in, char *argv[]) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	string test_directory = DUCKDB_ROOT_DIRECTORY;

	auto &test_config = TestConfiguration::Get();
	test_config.Initialize();

	idx_t argc = NumericCast<idx_t>(argc_in);
	int new_argc = 0;
	auto new_argv = duckdb::unique_ptr<char *[]>(new char *[argc]);
	for (idx_t i = 0; i < argc; i++) {
		string argument(argv[i]);
		if (argument == "--test-dir") {
			test_directory = string(argv[++i]);
		} else if (argument == "--test-temp-dir") {
			// NOTE: making this fully compatible with TEMP_DIR and TEMP_DIR_BASE is tricky; instead get
			// 99% there by having the TEMP_DIR* variables source this temp-dir if specified, prevent this
			// option from accepting remote dirs
			SetDeleteTestPath(false);
			auto test_dir = string(argv[++i]);
			if (fs->IsRemoteFile(test_dir)) {
				std::cerr << "usage error: --test-temp-dir accepts only local dirs\n\n"
				          << "\tto test with a remote directory, use --temp-dir-base;\n"
				          << "\tfor now --test-temp-dir is kept as-is for compatibility" << std::endl;
				return 1;
			}
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
	test_config.Finalize();

	// delete the testing directory if it exists
	auto dir = TestCreatePath("");
	try {
		TestDeleteDirectory(dir);
		// create the empty testing directory
		TestCreateDirectory(dir);
	} catch (std::exception &ex) {
		fprintf(stderr, "Failed to create testing directory \"%s\": %s\n", dir.c_str(), ex.what());
		return 1;
	}

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
}
