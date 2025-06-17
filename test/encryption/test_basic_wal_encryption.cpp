#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

using namespace duckdb;
using namespace std;

static idx_t GetWALFileVersion(FileSystem &fs, const string &path) {
	// A WAL file contains an 8-byte overall header
	//! The first 5 bytes are structured as followed:
	//! 100 - 000 - 98 - 101 - 000
	// (field_id - sep - wal_type - field_id - sep)
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	uint8_t wal_version;
	fs.Read(*handle, &wal_version, 1, 5);
	//! the 6th byte is the version number of the WAL
	return wal_version;
}

TEST_CASE("Test basic wal encryption", "[encryption]") {
	auto encrypted_database = TestCreatePath("encrypted_wal_test");
	auto encrypted_wal = encrypted_database + ".wal";

	auto encryption_config = GetTestConfig();

	encryption_config->options.enable_wal_encryption = true;
	encryption_config->options.user_key = make_shared_ptr<string>("asdf");
	encryption_config->options.contains_user_key = true;

	// set checkpoint wal size on 1TB
	encryption_config->options.checkpoint_wal_size = 1ULL << 40;
	encryption_config->options.checkpoint_on_shutdown = false;
	encryption_config->options.abort_on_wal_failure = false;

	DeleteDatabase(encrypted_database);
	{
		DuckDB db(encrypted_database, encryption_config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test AS SELECT -i a, -i b FROM range(100000) tbl(i);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (NULL, 22), (12, 21);"));
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=b+1 WHERE a=11;"));
	}

	// Check if WAL file exists
	LocalFileSystem lfs;
	if (!lfs.FileExists(encrypted_wal)) {
		throw std::runtime_error("WAL file does not exist");
	};

	//! Test WAL version
	auto encrypted_wal_version = GetWALFileVersion(lfs, encrypted_wal);
	if (encrypted_wal_version != 3) {
		//! For an encrypted WAL, the version is 3
		throw std::runtime_error("Encrypted wal version: " + to_string(encrypted_wal_version));
	}

	// reset encryption key
	encryption_config->options.user_key = make_shared_ptr<string>("asdf");

	{
		// Reload and read encrypted WAL
		DuckDB db(encrypted_database, encryption_config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SELECT a, b FROM test WHERE a>0 OR a IS NULL ORDER BY a;"));
	}

	DeleteDatabase(encrypted_database);
}

TEST_CASE("Test wrong key WAL encryption", "[encryption]") {
	auto encrypted_database = TestCreatePath("encrypted_wal_test");
	auto encrypted_wal = encrypted_database + ".wal";

	auto encryption_config = GetTestConfig();
	auto encryption_config_wrong = GetTestConfig();

	encryption_config->options.enable_wal_encryption = true;
	encryption_config->options.user_key = make_shared_ptr<string>("asdf");
	encryption_config->options.contains_user_key = true;

	// set checkpoint wal size on 1TB
	encryption_config->options.checkpoint_wal_size = 1ULL << 40;
	encryption_config->options.checkpoint_on_shutdown = false;
	encryption_config->options.abort_on_wal_failure = false;

	//! Give the wrong encryption key
	encryption_config_wrong->options.user_key = make_shared_ptr<string>("xxxx");
	LocalFileSystem lfs;

	DeleteDatabase(encrypted_database);
	{
		DuckDB db(encrypted_database, encryption_config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test AS SELECT -i a, -i b FROM range(100000) tbl(i);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (NULL, 22), (12, 21);"));
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=b+1 WHERE a=11;"));
	}

	// Check if WAL file exists
	if (!lfs.FileExists(encrypted_wal)) {
		throw std::runtime_error("WAL file does not exist");
	};

	//! Test WAL version
	auto encrypted_wal_version = GetWALFileVersion(lfs, encrypted_wal);
	if (encrypted_wal_version != 3) {
		//! For an encrypted WAL, the version is 3
		throw std::runtime_error("Encrypted wal version: " + to_string(encrypted_wal_version));
	}

	{
		// Reload with wrong encryption config
		try {
			// this should fail, because a wrong key is used to open the db file
			DuckDB db(encrypted_database, encryption_config_wrong.get());
			throw std::runtime_error("Wrong key used to open DB file, this should fail");
		} catch (const std::exception &e) {
			// Do nothing, because this should fail
		}
	}
	DeleteDatabase(encrypted_database);
}

TEST_CASE("Test pragma debug_disable_wal_encryption") {
	auto encrypted_database = TestCreatePath("encrypted_wal_test");
	auto encrypted_wal = encrypted_database + ".wal";

	auto encryption_config = GetTestConfig();

	encryption_config->options.enable_wal_encryption = true;
	encryption_config->options.user_key = make_shared_ptr<string>("asdf");
	encryption_config->options.contains_user_key = true;

	// set checkpoint wal size on 1TB
	encryption_config->options.checkpoint_wal_size = 1ULL << 40;
	encryption_config->options.checkpoint_on_shutdown = false;
	encryption_config->options.abort_on_wal_failure = false;

	LocalFileSystem lfs;

	DeleteDatabase(encrypted_database);

	{
		DuckDB db(encrypted_database, encryption_config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE A (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA debug_disable_wal_encryption;"));
		//! If you disable WAL encryption when WAL already exists
		//! All changes will be lost
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE B (a INTEGER);"));
	}

	// reset the encryption key
	encryption_config->options.user_key = make_shared_ptr<string>("asdf");

	{
		// reload the database
		DuckDB db(encrypted_database, encryption_config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("FROM A"));
		// FROM B should fail because it is written
		// after the PRAGMA is set
		REQUIRE_FAIL(con.Query("FROM B"));
	}

	DeleteDatabase(encrypted_database);
	// reset the encryption key
	encryption_config->options.user_key = make_shared_ptr<string>("asdf");
	{
		// reload the database
		DuckDB db(encrypted_database, encryption_config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("PRAGMA debug_disable_wal_encryption;"));
		//! if PRAGMA is directly set, no changes will be lost
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE A (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE B (a INTEGER);"));
	}

	// Check if WAL file exists
	if (!lfs.FileExists(encrypted_wal)) {
		throw std::runtime_error("WAL file does not exist");
	};

	//! WAL version should be 2, because it is now plaintext
	auto encrypted_wal_version = GetWALFileVersion(lfs, encrypted_wal);
	if (encrypted_wal_version != 2) {
		//! For an encrypted WAL, the version is 3
		throw std::runtime_error("Encrypted wal version: " + to_string(encrypted_wal_version));
	}

	// reset the encryption key
	encryption_config->options.user_key = make_shared_ptr<string>("asdf");

	{
		// reload the database
		DuckDB db(encrypted_database, encryption_config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("FROM A"));
		REQUIRE_NO_FAIL(con.Query("FROM B"));
	}
}

TEST_CASE("Test WAL version", "[encryption]") {
	auto plaintext_database = TestCreatePath("plaintext_wal_test");
	auto plaintext_wal = plaintext_database + ".wal";

	auto config = GetTestConfig();
	config->options.enable_wal_encryption = false;

	// make sure the database does not exist
	DeleteDatabase(plaintext_database);
	LocalFileSystem lfs;

	// set checkpoint wal size on 1TB
	config->options.checkpoint_wal_size = 1ULL << 40;
	config->options.checkpoint_on_shutdown = false;
	config->options.abort_on_wal_failure = false;

	auto db = make_uniq<DuckDB>(plaintext_database, config.get());
	auto conn = make_uniq<Connection>(*db);
	DeleteDatabase(plaintext_database);

	REQUIRE_NO_FAIL(conn->Query("CREATE TABLE test AS SELECT -i a, -i b FROM range(100000) tbl(i);"));
	REQUIRE_NO_FAIL(conn->Query("INSERT INTO test VALUES (11, 22), (NULL, 22), (12, 21);"));
	REQUIRE_NO_FAIL(conn->Query("UPDATE test SET b=b+1 WHERE a=11;"));

	auto handle = lfs.OpenFile(plaintext_wal, FileFlags::FILE_FLAGS_WRITE);

	// Test if WAL file exists
	if (!lfs.FileExists(plaintext_wal)) {
		throw std::runtime_error("WAL file does not exist");
	};

	auto wal_version = GetWALFileVersion(lfs, plaintext_wal);

	// open WAL file again
	if (wal_version != 2) {
		//! the WAL version should be 2 if plaintext
		throw std::runtime_error("Wal version: " + to_string(wal_version));
	}

	REQUIRE_NO_FAIL(conn->Query("SELECT a, b FROM test WHERE a>0 OR a IS NULL ORDER BY a;"));
}
