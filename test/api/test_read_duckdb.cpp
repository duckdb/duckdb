#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_file_path_manager.hpp"
#include "duckdb/main/database_manager.hpp"

using namespace duckdb;

TEST_CASE("Reader names are reused while a file is tracked", "[api][read_duckdb]") {
	DuckDB db(nullptr);
	auto &manager = DatabaseManager::Get(*db.instance);
	DatabaseFilePathManager paths;
	const string path = "reader.db";
	const auto on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	AttachOptions user_options({}, AccessMode::READ_ONLY);
	Identifier user_name("user");
	REQUIRE(paths.InsertDatabasePath(manager, path, user_name, on_conflict, user_options) ==
	        InsertDatabasePathResult::SUCCESS);

	AttachOptions reader_options({}, AccessMode::READ_ONLY);
	reader_options.visibility = AttachVisibility::HIDDEN;
	reader_options.is_reader = true;
	Identifier name;
	REQUIRE(paths.InsertDatabasePath(manager, path, name, on_conflict, reader_options) ==
	        InsertDatabasePathResult::SUCCESS);
	const auto reader_name = name;
	const string prefix = "__duckdb_reader_";
	REQUIRE(name.StartsWith(prefix));
	hugeint_t uuid;
	REQUIRE(UUID::FromString(name.GetIdentifierName().substr(prefix.size()), uuid, true));

	name.clear();
	REQUIRE(paths.InsertDatabasePath(manager, path, name, on_conflict, reader_options) ==
	        InsertDatabasePathResult::ALREADY_EXISTS);
	REQUIRE(name == reader_name);

	reader_options.stored_database_path.reset();
	name.clear();
	REQUIRE(paths.InsertDatabasePath(manager, path, name, on_conflict, reader_options) ==
	        InsertDatabasePathResult::SUCCESS);
	REQUIRE(name == reader_name);
	reader_options.stored_database_path.reset();
	user_options.stored_database_path.reset();
	REQUIRE(paths.ApproxDatabaseCount() == 0);
}
