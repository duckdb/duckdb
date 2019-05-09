#include "storage/storage_manager.hpp"
#include "storage/checkpoint_manager.hpp"
#include "storage/single_file_block_manager.hpp"
#include "storage/meta_block_reader.hpp"

#include "catalog/catalog.hpp"
#include "common/file_system.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "transaction/transaction_manager.hpp"

#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_table_info.hpp"

using namespace duckdb;
using namespace std;

StorageManager::StorageManager(DuckDB &db, string path, bool read_only)
    : database(db),path(path), wal(db), read_only(read_only) {
}

StorageManager::~StorageManager() {
}

void StorageManager::Initialize() {
	bool in_memory = path.empty() || path == ":memory:";

	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// first initialize the base system catalogs
	// these are never written to the WAL
	auto transaction = database.transaction_manager->StartTransaction();

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	database.catalog->CreateSchema(*transaction, &info);

	// initialize default functions
	BuiltinFunctions::Initialize(*transaction, *database.catalog);

	// commit transactions
	database.transaction_manager->CommitTransaction(transaction);

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase();
	}
}

void StorageManager::LoadDatabase() {
	string wal_path = path + ".wal";
	// first check if the database exists
	if (!database.file_system->DirectoryExists(path)) {
		if (read_only) {
			throw CatalogException("Cannot open database \"%s\" in read-only mode: database does not exist",
			                       path.c_str());
		}
		// check if the WAL exists
		if (database.file_system->FileExists(wal_path)) {
			// WAL file exists but database file does not
			// remove the WAL
			database.file_system->RemoveFile(wal_path);
		}
		// initialize the block manager while creating a new db file
		block_manager = make_unique<SingleFileBlockManager>(*database.file_system, path, read_only, true);
	} else {
		// initialize the block manager while loading the current db file
		block_manager = make_unique<SingleFileBlockManager>(*database.file_system, path, read_only, false);
		//! Load from storage
		LoadFromStorage();
		// check if the WAL file exists
		if (database.file_system->FileExists(wal_path)) {
			// replay the WAL
			wal.Replay(wal_path);
			CheckpointManager checkpointer(*this);
			// checkpoint the database
			checkpointer.CreateCheckpoint();
			// remove the WAL
			database.file_system->RemoveFile(wal_path);
		}
	}
	// FIXME: check if temporary file exists and delete that if it does
	// initialize the WAL file
	wal.Initialize(wal_path);
}

void StorageManager::LoadFromStorage() {
	block_id_t meta_block = block_manager->GetMetaBlock();
	if (meta_block < 0) {
		// storage is empty
		return;
	}

	auto transaction = database.transaction_manager->StartTransaction();

	// create the MetaBlockReader to read from the storage
	MetaBlockReader reader(*block_manager, meta_block);
	uint32_t schema_count = reader.Read<uint32_t>();
	for(uint32_t i = 0; i < schema_count; i++) {
		ReadSchema(*transaction, reader);
	}
	database.transaction_manager->CommitTransaction(transaction);
}

void StorageManager::ReadSchema(Transaction &transaction, MetaBlockReader &reader) {
	// read the schema and create it in the catalog
	CreateSchemaInfo info;
	info.schema = reader.ReadString();
	info.if_not_exists = true;
	database.catalog->CreateSchema(transaction, &info);

	// read the table count and recreate the tables
	uint32_t table_count = reader.Read<uint32_t>();
	for(uint32_t i = 0; i < table_count; i++) {
		ReadTable(transaction, reader);
	}
}

void StorageManager::ReadTable(Transaction &transaction, MetaBlockReader &reader) {
	CreateTableInfo info;
	info.schema = reader.ReadString();
	info.table = reader.ReadString();
	uint32_t column_count = reader.Read<uint32_t>();
	for(uint32_t i = 0; i < column_count; i++) {
		string name = reader.ReadString();
		SQLType type = SQLType((SQLTypeId) reader.Read<uint8_t>());
		info.columns.push_back(ColumnDefinition(name, type));
	}
	database.catalog->CreateTable(transaction, &info);
	// FIXME: load table data
}
