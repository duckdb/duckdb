#include "catch.hpp"
#include "common/file_system.hpp"
#include "duckdb.hpp"
#include "main/appender.hpp"
#include "test_helpers.hpp"

#include <signal.h>
#include <sys/mman.h>
#include <unistd.h>

using namespace duckdb;
using namespace std;

#define BOOL_COUNT 3

TEST_CASE("Test write lock with multiple processes", "[persistence]") {
	// shared memory to keep track of which processes are done
	bool *finished = (bool *)mmap(NULL, sizeof(bool) * BOOL_COUNT, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
	memset(finished, 0, BOOL_COUNT * sizeof(bool));

	string dbdir = TestCreatePath("writelocktest");
	DeleteDatabase(dbdir);
	// test write lock
	// fork away a child
	pid_t pid = fork();
	if (pid == 0) {
		// child process
		// open db for writing
		DuckDB db(dbdir);
		Connection con(db);
		// opened db for writing
		finished[0] = true;
		// insert some values
		con.Query("CREATE TABLE a(i INTEGER)");
		con.Query("INSERT INTO a VALUES(42)");
		// wait for parent to finish
		while (!finished[1]);
		finished[2] = true;
		// exit
		exit(0);
	} else if (pid > 0) {
		unique_ptr<DuckDB> db;
		// parent process
		// wait for child to open db for writing
		while(!finished[0]);
		// try to open db for writing, this should fail
		REQUIRE_THROWS(db = make_unique<DuckDB>(dbdir));
		// now kill the child
		finished[1] = true;
	}
}

TEST_CASE("Test read lock with multiple processes", "[persistence]") {
	// shared memory to keep track of which processes are done
	bool *finished = (bool *)mmap(NULL, sizeof(bool) * BOOL_COUNT, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
	memset(finished, 0, BOOL_COUNT * sizeof(bool));

	string dbdir = TestCreatePath("readlocktest");
	DeleteDatabase(dbdir);

	// create the database
	{
		DuckDB db(dbdir);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42)"));
	}
	// test read lock
	pid_t pid = fork();
	DBConfig config;
	config.access_mode = AccessMode::READ_ONLY;
	if (pid == 0) {
		// child process
		// open db for reading
		DuckDB db(dbdir, &config);
		Connection con(db);
		// opened db for reading
		finished[0] = true;
		// query some values
		con.Query("SELECT * FROM a");
		con.Query("SELECT i+2 FROM a");
		// wait for parent to finish
		while (!finished[1]);
		finished[2] = true;
		// exit
		exit(0);
	} else if (pid > 0) {
		unique_ptr<DuckDB> db;
		// parent process
		// wait for child to open db for writing
		while(!finished[0]);
		// try to open db for writing, this should fail
		REQUIRE_THROWS(db = make_unique<DuckDB>(dbdir));
		// but opening db for reading should work
		REQUIRE_NOTHROW(db = make_unique<DuckDB>(dbdir, &config));
		// we can query the database
		Connection con(*db);
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM a"));
		// now kill the child
		finished[1] = true;
	}
}

