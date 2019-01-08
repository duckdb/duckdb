#include "common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

int main(int argc, char *argv[]) {
	// delete the testing directory if it exists
	if (DirectoryExists(TESTING_DIRECTORY_NAME)) {
		RemoveDirectory(TESTING_DIRECTORY_NAME);
	}
	// create the empty testing directory
	CreateDirectory(TESTING_DIRECTORY_NAME);

	int result = Catch::Session().run(argc, argv);

	// delete the testing directory after running the tests
	RemoveDirectory(TESTING_DIRECTORY_NAME);

	return result;
}
