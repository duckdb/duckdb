#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

int main(int argc, char *argv[]) {
	// delete the testing directory if it exists
	auto dir = TestCreatePath("");
	try {
		TestDeleteDirectory(dir);
		// create the empty testing directory
		TestCreateDirectory(dir);
	} catch (Exception &ex) {
		fprintf(stderr, "Failed to create testing directory \"%s\": %s", dir.c_str(), ex.what());
		return 1;
	}

	int result = Catch::Session().run(argc, argv);

	TestDeleteDirectory(dir);

	return result;
}
