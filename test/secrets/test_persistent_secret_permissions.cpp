#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret_storage.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/extension_util.hpp"

#include <sys/stat.h>

#ifndef _WIN32
#include <fcntl.h>
#endif

using namespace duckdb;
using namespace std;

#ifndef _WIN32
static void assert_correct_permission(string file) {
	struct stat st;
	auto res = lstat(file.c_str(), &st);
	REQUIRE(res == 0);

	// Only permissions should be User Read+Write
	REQUIRE(st.st_mode & (S_IRUSR | S_IWUSR));
	// The rest should be 0
	REQUIRE(!(st.st_mode & (S_IXUSR | S_IRGRP | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH)));
}

TEST_CASE("Test file permissions on linux/macos", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	// Set custom secret path to prevent interference with other tests
	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=true;"));
	auto secret_dir = TestCreatePath("test_persistent_secret_permissions");
	REQUIRE_NO_FAIL(con.Query("set secret_directory='" + secret_dir + "'"));

	printf("thing: %s\n", secret_dir.c_str());
	REQUIRE_NO_FAIL(con.Query("CREATE PERSISTENT SECRET oh_so_secret (TYPE S3)"));

	assert_correct_permission(secret_dir + "/" + "oh_so_secret.duckdb_secret");
}
#endif
