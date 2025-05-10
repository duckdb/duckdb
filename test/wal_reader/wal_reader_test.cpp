#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/wal_reader.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/parsed_data/table_data_info.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <functional>

namespace duckdb {

const string WAL_FILE_EXTENSION = ".wal";

TEST_CASE("Test basic WAL reader functionality", "[wal_reader]") {
	string dbdir = TestCreatePath("wal_reader_test");
	{
		DuckDB db(dbdir);
		// Create a second connection for long-running read transaction
		Connection read_con(db);
		// Start a read-only transaction in the second connection
		REQUIRE_NO_FAIL(read_con.Query("BEGIN TRANSACTION READ ONLY"));

		// get the path to db and append ".wal" to it
		auto wal_path = dbdir + WAL_FILE_EXTENSION;

		{
			// Read write connection
			Connection con(db);

			// Create a table and insert some data to generate WAL entries
			REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (id INTEGER, name VARCHAR)"));
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 'test1')"));
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (2, 'test2')"));
		}

		// Create a WAL reader to read the WAL file
		auto file_system = FileSystem::CreateLocal();
		auto handle = file_system->OpenFile(wal_path, FileFlags::FILE_FLAGS_READ);
		auto &db_manager = DatabaseManager::Get(*db.instance.get());
		auto databases = db_manager.GetDatabases(*read_con.context);
		auto &attached_db = databases[0];
		auto file_reader = make_uniq<BufferedFileReader>(FileSystem::Get(attached_db), std::move(handle));
		WALReader reader(attached_db, std::move(file_reader));

		// Read and verify WAL entries
		unique_ptr<ParseInfo> entry;
		bool found_create_table = false;
		bool found_inserts = false;
		idx_t insert_count = 0;

		while ((entry = reader.Next()) != nullptr) {
			if (auto create_info = dynamic_cast<CreateInfo *>(entry.get())) {
				if (create_info->type == CatalogType::TABLE_ENTRY) {
					found_create_table = true;
					auto &create_table = (CreateTableInfo &)*create_info;
					REQUIRE(create_table.table == "test");
				}
			} else if (auto insert_info = dynamic_cast<TableDataInsertInfo *>(entry.get())) {
				found_inserts = true;
				insert_count++;
				REQUIRE(insert_info->chunk != nullptr);
				REQUIRE(insert_info->chunk->size() == 1); // We inserted one row at a time
				auto &col1 = insert_info->chunk->data[0];
				auto &col2 = insert_info->chunk->data[1];
				REQUIRE(col1.GetVectorType() == VectorType::FLAT_VECTOR);
				REQUIRE(col2.GetVectorType() == VectorType::FLAT_VECTOR);
			}
		}

		REQUIRE(found_create_table);
		REQUIRE(found_inserts);
		REQUIRE(insert_count == 2);

		// Abort the read-only transaction and close the second connection
		REQUIRE_NO_FAIL(read_con.Query("ROLLBACK"));
	}
}

namespace {
// Helper struct to organize test cases for WAL record types
struct WalRecordTestCase {
	string name;                                // Name of the test case
	string setup_sql;                           // SQL to generate this WAL record
	CatalogType type;                           // Type of the catalog entry (for Create/Drop)
	std::function<bool(ParseInfo *)> validator; // Custom validation function

	static WalRecordTestCase CreateTable() {
		return {"CreateTable", "CREATE TABLE test1 (id INTEGER)", CatalogType::TABLE_ENTRY,
		        [](ParseInfo *entry) -> bool {
			        if (auto create_info = dynamic_cast<CreateInfo *>(entry)) {
				        if (create_info->type == CatalogType::TABLE_ENTRY) {
					        auto &create_table = (CreateTableInfo &)*create_info;
					        return create_table.table == "test1";
				        }
			        }
			        return false;
		        }};
	}

	static WalRecordTestCase CreateView() {
		return {"CreateView", "CREATE VIEW test_view AS SELECT * FROM test1", CatalogType::VIEW_ENTRY,
		        [](ParseInfo *entry) -> bool {
			        if (auto create_info = dynamic_cast<CreateInfo *>(entry)) {
				        if (create_info->type == CatalogType::VIEW_ENTRY) {
					        auto &create_view = (CreateViewInfo &)*create_info;
					        return create_view.view_name == "test_view";
				        }
			        }
			        return false;
		        }};
	}

	static WalRecordTestCase Insert() {
		return {"Insert", "INSERT INTO test1 VALUES (1)", CatalogType::INVALID, [](ParseInfo *entry) -> bool {
			        if (auto insert_info = dynamic_cast<TableDataInsertInfo *>(entry)) {
				        return insert_info->chunk != nullptr && insert_info->chunk->size() == 1 &&
				               insert_info->chunk->data[0].GetVectorType() == VectorType::FLAT_VECTOR;
			        }
			        return false;
		        }};
	}

	static WalRecordTestCase Update() {
		return {"Update", "UPDATE test1 SET id = 2 WHERE id = 1", CatalogType::INVALID, [](ParseInfo *entry) -> bool {
			        if (auto update_info = dynamic_cast<TableDataUpdateInfo *>(entry)) {
				        return update_info->chunk != nullptr &&
				               update_info->chunk->data[0].GetVectorType() == VectorType::FLAT_VECTOR &&
				               !update_info->column_indexes.empty();
			        }
			        return false;
		        }};
	}

	static WalRecordTestCase Delete() {
		return {"Delete", "DELETE FROM test1 WHERE id = 2", CatalogType::INVALID, [](ParseInfo *entry) -> bool {
			        if (auto delete_info = dynamic_cast<TableDataDeleteInfo *>(entry)) {
				        return delete_info->chunk != nullptr &&
				               delete_info->chunk->data[0].GetVectorType() == VectorType::FLAT_VECTOR;
			        }
			        return false;
		        }};
	}

	static WalRecordTestCase DropView() {
		return {"DropView", "DROP VIEW test_view", CatalogType::VIEW_ENTRY, [](ParseInfo *entry) -> bool {
			        if (auto drop_info = dynamic_cast<DropInfo *>(entry)) {
				        return drop_info->type == CatalogType::VIEW_ENTRY && drop_info->name == "test_view";
			        }
			        return false;
		        }};
	}

	static WalRecordTestCase DropTable() {
		return {"DropTable", "DROP TABLE test1", CatalogType::TABLE_ENTRY, [](ParseInfo *entry) -> bool {
			        if (auto drop_info = dynamic_cast<DropInfo *>(entry)) {
				        return drop_info->type == CatalogType::TABLE_ENTRY && drop_info->name == "test1";
			        }
			        return false;
		        }};
	}
};
} // anonymous namespace

TEST_CASE("Test WAL reader with different entry types", "[wal_reader]") {
	string dbdir = TestCreatePath("wal_reader_test_types");
	{
		DuckDB db(dbdir);
		// Create a second connection for long-running read transaction
		Connection read_con(db);
		// Start a read-only transaction in the second connection
		REQUIRE_NO_FAIL(read_con.Query("BEGIN TRANSACTION READ ONLY"));

		// get the path to db and append ".wal" to it
		auto wal_path = dbdir + WAL_FILE_EXTENSION;

		// Define all test cases
		vector<WalRecordTestCase> test_cases = {WalRecordTestCase::CreateTable(), WalRecordTestCase::CreateView(),
		                                        WalRecordTestCase::Insert(),      WalRecordTestCase::Update(),
		                                        WalRecordTestCase::Delete(),      WalRecordTestCase::DropView(),
		                                        WalRecordTestCase::DropTable()};

		{
			// Read write connection
			Connection con(db);

			// Execute all test case setup SQL
			for (const auto &test_case : test_cases) {
				REQUIRE_NO_FAIL(con.Query(test_case.setup_sql));
			}
		}

		// Create a WAL reader to read the WAL file
		auto file_system = FileSystem::CreateLocal();
		auto handle = file_system->OpenFile(wal_path, FileFlags::FILE_FLAGS_READ);
		auto &db_manager = DatabaseManager::Get(*db.instance.get());
		auto databases = db_manager.GetDatabases(*read_con.context);
		auto &attached_db = databases[0];

		auto file_reader = make_uniq<BufferedFileReader>(FileSystem::Get(attached_db), std::move(handle));
		WALReader reader(attached_db, std::move(file_reader));

		// Read and verify WAL entries
		unique_ptr<ParseInfo> entry;
		vector<bool> found(test_cases.size(), false);

		while ((entry = reader.Next()) != nullptr) {
			// Try each test case's validator
			for (size_t i = 0; i < test_cases.size(); i++) {
				if (test_cases[i].validator(entry.get())) {
					found[i] = true;
					break;
				}
			}
		}

		// Verify all test cases were found
		for (size_t i = 0; i < test_cases.size(); i++) {
			INFO("Missing WAL record for: " << test_cases[i].name);
			REQUIRE(found[i]);
		}

		// Abort the read-only transaction and close the second connection
		REQUIRE_NO_FAIL(read_con.Query("ROLLBACK"));
	}
}

TEST_CASE("Test WAL reader error handling", "[wal_reader]") {
	string dbdir = TestCreatePath("wal_reader_test_errors");
	{
		DuckDB db(dbdir);
		// Create a second connection for long-running read transaction
		Connection read_con(db);
		// Start a read-only transaction in the second connection
		REQUIRE_NO_FAIL(read_con.Query("BEGIN TRANSACTION READ ONLY"));

		// get the path to db and append ".wal" to it
		auto wal_path = dbdir + WAL_FILE_EXTENSION;

		{
			// Read write connection
			Connection con(db);

			// Create some WAL entries
			REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (id INTEGER)"));
		}

		{
			auto &db_manager = DatabaseManager::Get(*db.instance.get());
			auto databases = db_manager.GetDatabases(*read_con.context);
			auto &attached_db = databases[0];

			// Test reading from an empty WAL file
			auto file_system = FileSystem::CreateLocal();
			auto empty_wal_path = dbdir + "_empty" + WAL_FILE_EXTENSION;
			auto handle = file_system->OpenFile(empty_wal_path,
			                                    FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileFlags::FILE_FLAGS_WRITE);
			auto empty_handle = file_system->OpenFile(empty_wal_path, FileFlags::FILE_FLAGS_READ);
			auto empty_file_reader =
			    make_uniq<BufferedFileReader>(FileSystem::Get(attached_db), std::move(empty_handle));
			WALReader empty_reader(attached_db, std::move(empty_file_reader));
			REQUIRE(empty_reader.Next() == nullptr);

			// Test trying to read after end of WAL
			handle = file_system->OpenFile(wal_path, FileFlags::FILE_FLAGS_READ);
			auto file_reader = make_uniq<BufferedFileReader>(FileSystem::Get(attached_db), std::move(handle));
			WALReader reader(attached_db, std::move(file_reader));

			// Read all entries
			while (reader.Next() != nullptr) {
			}
			// One more read should return nullptr
			REQUIRE(reader.Next() == nullptr);
		}

		// Abort the read-only transaction and close the second connection
		REQUIRE_NO_FAIL(read_con.Query("ROLLBACK"));
	}
}

} // namespace duckdb
