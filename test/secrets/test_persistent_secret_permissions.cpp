#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret_storage.hpp"
#include "test_helpers.hpp"

#include <sys/stat.h>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
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
	REQUIRE(!(st.st_mode & (S_IXUSR | S_IRWXG | S_IRWXO)));
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

	REQUIRE_NO_FAIL(con.Query("CREATE PERSISTENT SECRET oh_so_secret (TYPE S3)"));

	assert_correct_permission(secret_dir + "/" + "oh_so_secret.duckdb_secret");
}

static void assert_duckdb_will_reject_persistent_secret() {
	DuckDB db(nullptr);
	Connection con(db);

	// Set custom secret path to prevent interference with other tests
	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=true;"));
	auto secret_dir = TestCreatePath("test_persistent_secret_permissions");
	REQUIRE_NO_FAIL(con.Query("set secret_directory='" + secret_dir + "'"));

	auto res = con.Query("FROM duckdb_secrets()");
	REQUIRE(res->HasError());
	REQUIRE(StringUtil::Contains(res->GetError(),
	                             "has incorrect permissions! Please set correct permissions or remove file"));
}

TEST_CASE("Test that DuckDB rejects secrets with incorrect permissions on linux/macos", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	// Set custom secret path to prevent interference with other tests
	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=true;"));
	auto secret_dir = TestCreatePath("test_persistent_secret_permissions");
	REQUIRE_NO_FAIL(con.Query("set secret_directory='" + secret_dir + "'"));

	REQUIRE_NO_FAIL(con.Query("CREATE PERSISTENT SECRET also_very_secret (TYPE S3)"));

	string secret_path = secret_dir + "/" + "also_very_secret.duckdb_secret";

	mode_t incorrect_permissions[] {S_IRUSR | S_IWUSR | S_IRGRP,  // user rw + group read
	                                S_IRUSR | S_IWUSR | S_IWGRP,  // user rw + group write
	                                S_IRUSR | S_IWUSR | S_IXGRP,  // user rw + group execute
	                                S_IRUSR | S_IWUSR | S_IROTH,  // user rw + other read
	                                S_IRUSR | S_IWUSR | S_IWOTH,  // user rw + other write
	                                S_IRUSR | S_IWUSR | S_IXOTH}; // user rw + other execute

	// Now confirm that for all possible incorrect permissions, we throw
	for (auto perm : incorrect_permissions) {
		chmod(secret_path.c_str(), perm);
		assert_duckdb_will_reject_persistent_secret();
	}

	// Setting back to correct permission should allow us to read it again
	chmod(secret_path.c_str(), S_IRUSR | S_IWUSR);

	// Should be gud now
	DuckDB db2(nullptr);
	Connection con2(db2);
	REQUIRE_NO_FAIL(con2.Query("set allow_persistent_secrets=true;"));
	REQUIRE_NO_FAIL(con2.Query("set secret_directory='" + secret_dir + "'"));
	REQUIRE_NO_FAIL(con2.Query("FROM duckdb_secrets()"));
}
#endif
