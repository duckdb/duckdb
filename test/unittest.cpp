#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace duckdb {
static bool test_force_storage = false;

bool TestForceStorage() {
	return test_force_storage;
}

} // namespace duckdb

int main(int argc, char *argv[]) {
	TestChangeDirectory(DUCKDB_ROOT_DIRECTORY);
	// delete the testing directory if it exists
	auto dir = TestCreatePath("");
	try {
		TestDeleteDirectory(dir);
		// create the empty testing directory
		TestCreateDirectory(dir);
	} catch (std::exception &ex) {
		fprintf(stderr, "Failed to create testing directory \"%s\": %s", dir.c_str(), ex.what());
		return 1;
	}

	int new_argc = 0;
	auto new_argv = unique_ptr<char *[]>(new char *[argc]);
	for (int i = 0; i < argc; i++) {
		if (string(argv[i]) == "--force-storage") {
			test_force_storage = true;
		} else {
			new_argv[new_argc] = argv[i];
			new_argc++;
		}
	}

	int result = Catch::Session().run(new_argc, new_argv.get());

	TestDeleteDirectory(dir);

	return result;
}
