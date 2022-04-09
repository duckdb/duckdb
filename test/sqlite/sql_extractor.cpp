#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/file_system.hpp"
#include "sqllogic_test_runner.hpp"

namespace duckdb {

bool TestForceStorage() {
	return false;
}

} // namespace duckdb

int main(int argc, char *argv[]) {
	if (argc != 2) {
		fprintf(stderr, "Usage: sql_extractor /path/to/test.text\n");
		return 1;
	}
	duckdb::SQLLogicTestRunner runner(":memory:");
	runner.output_sql = true;
	runner.ExecuteFile(argv[1]);
	return 0;
}
