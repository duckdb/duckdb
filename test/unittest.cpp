
#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "common/file_system.hpp"

using namespace duckdb;

#define TESTING_DIRECTORY_NAME "duckdb_unittest_tempdir"

int main(int argc, char *argv[]) {

	// delete the testing directory if it exists
	if (DirectoryExists(TESTING_DIRECTORY_NAME)) {
		RemoveDirectory(TESTING_DIRECTORY_NAME);
	}
	// create the empty testing directory
	CreateDirectory(TESTING_DIRECTORY_NAME);

	// save current working directory
	auto current_dir = GetWorkingDirectory();

	// set working directory to the testing directory
	SetWorkingDirectory(TESTING_DIRECTORY_NAME);

	int result = Catch::Session().run(argc, argv);

	//! Resets the current working directory
	SetWorkingDirectory(current_dir);

	// delete the testing directory after running the tests
	RemoveDirectory(TESTING_DIRECTORY_NAME);

	return result;
}
