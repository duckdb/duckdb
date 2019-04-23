#include "catch.hpp"
#include "common/file_system.hpp"
#include "duckdb.hpp"
#include "main/appender.hpp"
#include "test_helpers.hpp"

#include <sys/mman.h>
#include <unistd.h>

using namespace duckdb;
using namespace std;

TEST_CASE("Test transactional integrity when facing process aborts", "[persistence]") {
	// shared memory to keep track of insertions
	size_t *count = (size_t *)mmap(NULL, sizeof(size_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);

	string db_folder_parent = JoinPath(TESTING_DIRECTORY_NAME, "llstorage");
	if (DirectoryExists(db_folder_parent)) {
		RemoveDirectory(db_folder_parent);
	}
	CreateDirectory(db_folder_parent);

	string db_folder = JoinPath(db_folder_parent, "dbfolder");
	{
		DuckDB db(db_folder);
		Connection con(db);
		con.Query("CREATE TABLE a (i INTEGER)");
	}

	// fork away a child to be mercilessy shot in a bit
	pid_t pid = fork();

	if (pid == 0) { // child process
		DuckDB db(db_folder);
		Connection con(db);
		while (true) {
			con.Query("INSERT INTO a VALUES(42)");
			(*count)++;
		}
	} else if (pid > 0) { // parent process
		// wait until child has inserted at least 1000 rows
		while (*count < 1000) {
			usleep(100);
		}
		if (kill(pid, SIGKILL) != 0) {
			FAIL();
		}

		DuckDB db(db_folder);
		Connection con(db);
		auto res = con.Query("SELECT COUNT(*) FROM a");
		// there may be an off-by-one if we kill exactly between query and count increment
		REQUIRE(abs((int64_t)(res->GetValue(0, 0).GetNumericValue() - *count)) < 2);
	} else {
		FAIL();
	}
}
