#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace duckdb {
static bool test_force_storage = false;
static bool test_force_reload = false;

bool TestForceStorage() {
	return test_force_storage;
}

bool TestForceReload() {
	return test_force_reload;
}

} // namespace duckdb

int main(int argc, char *argv[]) {
	string test_directory = DUCKDB_ROOT_DIRECTORY;

	int new_argc = 0;
	auto new_argv = unique_ptr<char *[]>(new char *[argc]);
	for (int i = 0; i < argc; i++) {
		if (string(argv[i]) == "--force-storage") {
			test_force_storage = true;
		} else if (string(argv[i]) == "--force-reload") {
			test_force_reload = true;
		} else if (string(argv[i]) == "--test-dir") {
			test_directory = string(argv[++i]);
		} else {
			new_argv[new_argc] = argv[i];
			new_argc++;
		}
	}

	TestChangeDirectory(test_directory);
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

	RegisterSqllogictests();

	int result = Catch::Session().run(new_argc, new_argv.get());

	TestDeleteDirectory(dir);

	return result;
}
