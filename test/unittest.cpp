#include "common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

int main(int argc, char *argv[]) {
	// delete the testing directory if it exists
	if (FileSystem::DirectoryExists(TESTING_DIRECTORY_NAME)) {
		FileSystem::RemoveDirectory(TESTING_DIRECTORY_NAME);
	}
	// create the empty testing directory
	FileSystem::CreateDirectory(TESTING_DIRECTORY_NAME);

	int result = Catch::Session().run(argc, argv);

	// delete the testing directory after running the tests
	FileSystem::RemoveDirectory(TESTING_DIRECTORY_NAME);

	return result;
}
