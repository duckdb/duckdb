#include "storage/storage_manager.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/file_system.hpp"
#include "common/serializer.hpp"
#include "function/function.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "transaction/transaction_manager.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

StorageManager::StorageManager(DuckDB &database, string path) : path(path), database(database), wal(database) {
}

void StorageManager::Initialize() {
	bool in_memory = path.empty() || path == ":memory:";

	// first initialize the base system catalogs
	// these are never written to the WAL
	auto transaction = database.transaction_manager.StartTransaction();

	// create the default schema
	CreateSchemaInformation info;
	info.schema = DEFAULT_SCHEMA;
	database.catalog.CreateSchema(*transaction, &info);

	// initialize default functions
	BuiltinFunctions::Initialize(*transaction, database.catalog);

	// commit transactions
	database.transaction_manager.CommitTransaction(transaction);

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		LoadDatabase();
	}
}

void StorageManager::LoadDatabase() {
	int iteration = 0;
	// first check if the database exists
	if (!DirectoryExists(path)) {
		// have to create the directory
		CreateDirectory(path);
	} else {
		// load from the main storage if it exists
		iteration = LoadFromStorage();
		// directory already exists
		// verify that it is an existing database
		string wal_path = JoinPath(path, WAL_FILES[iteration]);
		if (FileExists(wal_path)) {
			// replay the WAL
			wal.Replay(wal_path);
			// switch the iteration target to the other one
			iteration = 1 - iteration;
			// checkpoint the WAL
			CreateCheckpoint(iteration);
		}
	}
	// initialize the WAL file
	string wal_path = JoinPath(path, WAL_FILES[iteration]);
	wal.Initialize(wal_path);
}

int StorageManager::LoadFromStorage() {
	ClientContext context(database);

	auto meta_info_path = JoinPath(path, DATABASE_INFO_FILE);
	// read the meta information, if there is any
	if (!FileExists(meta_info_path)) {
		// no meta file to read: skip the loading
		return 0;
	}

	context.transaction.BeginTransaction();
	// first read the meta information
	ifstream meta_info;
	meta_info.open(meta_info_path);
	if (!meta_info.good()) {
		throw IOException("Could not open meta file for writing!");
	}
	int64_t storage_version;
	int iteration;

	meta_info >> storage_version;
	meta_info >> iteration;

	// now we can start by actually reading the files
	auto storage_path_base = JoinPath(path, STORAGE_FILES[iteration]);
	auto schema_path = JoinPath(storage_path_base, SCHEMA_FILE);

	// read the list of schemas
	// FIXME: turn into function Open()
	ifstream schema_file;
	schema_file.open(schema_path);
	if (!schema_file.good()) {
		throw IOException("Could not open schema for writing!");
	}
	// END OF FIXME
	string schema_name;
	while (getline(schema_file, schema_name)) {
		// create the schema in the catalog
		CreateSchemaInformation info;
		info.schema = schema_name;
		info.if_not_exists = true;
		database.catalog.CreateSchema(context.ActiveTransaction(), &info);

		// now read the list of the tables belonging to this schema
		auto schema_directory_path = JoinPath(storage_path_base, schema_name);
		auto table_list_path = JoinPath(schema_directory_path, TABLE_LIST_FILE);

		// read the list of schemas
		// FIXME: turn into function Open()
		ifstream table_list_file;
		table_list_file.open(table_list_path);
		if (!table_list_file.good()) {
			throw IOException("Could not open schema for writing!");
		}
		// END OF FIXME
		string table_name;
		while (getline(table_list_file, table_name)) {
			// get the information of the table
			auto table_directory_path = JoinPath(schema_directory_path, table_name);
			auto table_meta_name = JoinPath(table_directory_path, TABLE_FILE);

			// FIXME: turn into function Open()
			ifstream table_file;
			table_file.open(table_meta_name, ifstream::binary);
			if (!table_file.good()) {
				throw IOException("Could not open table file for writing!");
			}
			// END OF FIXME
			// FIXME: turn into function ReadBinary()
			table_file.seekg(0, ios::end);
			size_t table_file_size = table_file.tellg();
			auto result = unique_ptr<char[]>(new char[table_file_size]);
			table_file.seekg(0, ios::beg);
			table_file.read(result.get(), table_file_size);
			// END OF FIXME

			// deserialize the CreateTableInformation
			Deserializer source((uint8_t *)result.get(), table_file_size);
			auto info = TableCatalogEntry::Deserialize(source);
			// create the table inside the catalog
			database.catalog.CreateTable(context.ActiveTransaction(), info.get());

			// now load the actual data
			auto *table = database.catalog.GetTable(context.ActiveTransaction(), info->schema, info->table);
			auto types = table->GetTypes();
			DataChunk chunk;
			chunk.Initialize(types);

			size_t chunk_count = 1;
			while (true) {
				auto chunk_name = JoinPath(table_directory_path, "chunk-" + to_string(chunk_count) + ".bin");
				if (!FileExists(chunk_name)) {
					break;
				}

				// FIXME: turn into function Open()
				ifstream chunk_file;
				chunk_file.open(chunk_name, ifstream::binary);
				if (!chunk_file.good()) {
					throw IOException("Could not open table file for writing!");
				}
				// END OF FIXME
				// FIXME: turn into function ReadBinary()
				chunk_file.seekg(0, ios::end);
				auto chunk_file_size = chunk_file.tellg();
				auto result = unique_ptr<char[]>(new char[chunk_file_size]);
				chunk_file.seekg(0, ios::beg);
				chunk_file.read(result.get(), chunk_file_size);
				// END OF FIXME
				// deserialize the chunk
				DataChunk insert_chunk;
				Deserializer source((uint8_t *)result.get(), chunk_file_size);
				insert_chunk.Deserialize(source);
				// insert the chunk into the table
				table->storage->Append(*table, context, insert_chunk);
				chunk_count++;
			}
		}
	}
	context.transaction.Commit();
	return iteration;
}

void StorageManager::CreateCheckpoint(int iteration) {
	auto transaction = database.transaction_manager.StartTransaction();

	assert(iteration == 0 || iteration == 1);
	auto storage_path_base = JoinPath(path, STORAGE_FILES[iteration]);
	if (DirectoryExists(storage_path_base)) {
		// have a leftover directory
		// remove it
		RemoveDirectory(storage_path_base);
	}
	// create the directory
	CreateDirectory(storage_path_base);

	// first we have to access the schemas
	auto schema_path = JoinPath(storage_path_base, SCHEMA_FILE);
	// FIXME
	ofstream schema_file;
	schema_file.open(schema_path);
	if (!schema_file.good()) {
		throw IOException("Could not open schemas file for writing!");
	}
	// END OF FIXME
	vector<SchemaCatalogEntry *> schemas;
	// scan the schemas and write them to the schemas.csv file
	database.catalog.schemas.Scan(*transaction, [&](CatalogEntry *entry) {
		schema_file << entry->name << "\n";
		schemas.push_back((SchemaCatalogEntry *)entry);
	});
	// FIXME: turn into function Close()
	schema_file.close();
	// check the success of the write
	if (schema_file.fail()) {
		throw IOException("Failed to write to schema!");
	}
	// END OF FIXME

	// now for each schema create a directory
	for (auto &schema : schemas) {
		// FIXME: schemas can have unicode, do something for file systems, maybe hash?
		auto schema_directory_path = JoinPath(storage_path_base, schema->name);
		assert(!DirectoryExists(schema_directory_path));
		// create the directory
		CreateDirectory(schema_directory_path);
		// create the file holding the list of tables for the schema
		auto table_list_path = JoinPath(schema_directory_path, TABLE_LIST_FILE);
		ofstream table_list_file;
		// FIXME
		table_list_file.open(table_list_path);
		if (!table_list_file.good()) {
			throw IOException("Could not open table list file for writing!");
		}
		// END OF FIXME

		// create the list of tables for the schema
		vector<TableCatalogEntry *> tables;
		schema->tables.Scan(*transaction, [&](CatalogEntry *entry) {
			table_list_file << entry->name << "\n";
			tables.push_back((TableCatalogEntry *)entry);
		});
		// FIXME
		table_list_file.close();
		// check the success of the write
		if (table_list_file.fail()) {
			throw IOException("Failed to write to table list!");
		}
		// END OF FIXME

		// now for each table, write the column meta information and the actual data
		for (auto &table : tables) {
			// first create a directory for the table information
			// FIXME: same problem as schemas, unicode and file systems may not agree
			auto table_directory_path = JoinPath(schema_directory_path, table->name);
			assert(!DirectoryExists(table_directory_path));
			// create the directory
			CreateDirectory(table_directory_path);

			auto table_meta_name = JoinPath(table_directory_path, TABLE_FILE);
			// FIXME
			ofstream table_file;
			table_file.open(table_meta_name, std::ofstream::binary);
			if (!table_file.good()) {
				throw IOException("Could not open table file for writing!");
			}
			// END OF FIXME

			// serialize the table information to a file
			Serializer serializer;
			table->Serialize(serializer);
			auto data = serializer.GetData();
			table_file.write((char *)data.data.get(), data.size);

			// FIXME
			table_file.close();
			// check the success of the write
			if (table_file.fail()) {
				throw IOException("Failed to write to table meta info!");
			}
			// END OF FIXME

			// now we have to write the actual binary
			// we do this by performing a scan of the table
			ScanStructure ss;
			table->storage->InitializeScan(ss);

			vector<column_t> column_ids;
			for (size_t i = 0; i < table->columns.size(); i++) {
				column_ids.push_back(i);
			}
			DataChunk chunk;
			auto types = table->GetTypes();
			chunk.Initialize(types);

			size_t chunk_count = 1;
			while (true) {
				chunk.Reset();
				table->storage->Scan(*transaction, chunk, column_ids, ss);
				if (chunk.size() == 0) {
					break;
				}
				auto chunk_name = JoinPath(table_directory_path, "chunk-" + to_string(chunk_count) + ".bin");
				// FIXME
				ofstream chunk_file;
				chunk_file.open(chunk_name, std::ofstream::binary);
				if (!chunk_file.good()) {
					throw IOException("Could not open DataChunk for writing!");
				}
				// END OF FIXME
				Serializer serializer;
				chunk.Serialize(serializer);
				auto data = serializer.GetData();
				chunk_file.write((char *)data.data.get(), data.size);

				// FIXME
				chunk_file.close();
				// check the success of the write
				if (chunk_file.fail()) {
					throw IOException("Failed to write to chunk file!");
				}
				// END OF FIXME
				chunk_count++;
			}
		}
	}
	// all the writes have been flushed and the entire database has been written
	// now we create the temporary meta information file
	auto meta_path = JoinPath(path, DATABASE_TEMP_INFO_FILE);
	ofstream meta_file;
	meta_file.open(meta_path);
	if (!meta_file.good()) {
		throw IOException("Could not open meta file for writing!");
	}

	//! Write information to the meta file
	meta_file << STORAGE_VERSION << "\n";
	meta_file << iteration << "\n";

	meta_file.close();
	// check the success of the write
	if (meta_file.fail()) {
		throw IOException("Failed to write to meta file!");
	}

	// now we move the meta information file over the old meta information file
	// this signifies a "completion" of the checkpoint
	auto permanent_meta_path = JoinPath(path, DATABASE_INFO_FILE);
	MoveFile(meta_path, permanent_meta_path);

	// we are now done writing
	// we can delete the directory for the other iteration because we do not need it anymore for consistency
	auto other_storage_path = JoinPath(path, STORAGE_FILES[1 - iteration]);
	auto other_wal_path = JoinPath(path, WAL_FILES[1 - iteration]);
	if (DirectoryExists(other_storage_path)) {
		RemoveDirectory(other_storage_path);
	}
	if (FileExists(other_wal_path)) {
		RemoveDirectory(other_wal_path);
	}
	transaction->Rollback();
}
