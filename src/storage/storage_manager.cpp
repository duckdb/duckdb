#include "storage/storage_manager.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/view_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/file_system.hpp"
#include "common/fstream_util.hpp"
#include "common/serializer.hpp"
#include "function/function.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "transaction/transaction_manager.hpp"

constexpr const int64_t STORAGE_VERSION = 1;
constexpr const char *DATABASE_INFO_FILE = "meta.info";
constexpr const char *DATABASE_TEMP_INFO_FILE = "meta.info.tmp";
constexpr const char *STORAGE_FILES[] = {"data-a", "data-b"};
constexpr const char *WAL_FILES[] = {"duckdb-a.wal", "duckdb-b.wal"};
constexpr const char *SCHEMA_FILE = "schemas.csv";
constexpr const char *TABLE_LIST_FILE = "tables.csv";
constexpr const char *VIEW_LIST_FILE = "views.csv";
constexpr const char *TABLE_FILE = "tableinfo.duck";

using namespace duckdb;
using namespace std;

StorageManager::StorageManager(DuckDB &database, string path, bool read_only) : path(path), database(database), wal(database), read_only(read_only) {
}

void StorageManager::Initialize() {
	bool in_memory = path.empty() || path == ":memory:";

	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

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
	if (!FileSystem::DirectoryExists(path)) {
		if (read_only) {
			throw CatalogException("Cannot open database \"%s\" in read-only mode: database does not exist", path.c_str());
		}
		// have to create the directory
		FileSystem::CreateDirectory(path);
	} else {
		// load from the main storage if it exists
		iteration = LoadFromStorage();
		// directory already exists
		// verify that it is an existing database
		string wal_path = FileSystem::JoinPath(path, WAL_FILES[iteration]);
		if (FileSystem::FileExists(wal_path)) {
			// replay the WAL
			wal.Replay(wal_path);
			if (!read_only) {
				// checkpoint the WAL
				// we only checkpoint if the database is opened in read-write mode
				// switch the iteration target to the other one
				iteration = 1 - iteration;
				// create the checkpoint
				CreateCheckpoint(iteration);
			}
		}
	}
	if (!read_only) {
		// initialize the WAL file
		string wal_path = FileSystem::JoinPath(path, WAL_FILES[iteration]);
		wal.Initialize(wal_path);
	}
}

int StorageManager::LoadFromStorage() {
	ClientContext context(database);

	auto meta_info_path = FileSystem::JoinPath(path, DATABASE_INFO_FILE);
	// read the meta information, if there is any
	if (!FileSystem::FileExists(meta_info_path)) {
		// no meta file to read: skip the loading
		return 0;
	}

	context.transaction.BeginTransaction();
	// first read the meta information
	fstream meta_info;
	FstreamUtil::OpenFile(meta_info_path, meta_info, ios_base::in);
	int64_t storage_version;
	int iteration;

	meta_info >> storage_version;
	meta_info >> iteration;

	// now we can start by actually reading the files
	auto storage_path_base = FileSystem::JoinPath(path, STORAGE_FILES[iteration]);
	auto schema_path = FileSystem::JoinPath(storage_path_base, SCHEMA_FILE);

	// read the list of schemas
	fstream schema_file;
	FstreamUtil::OpenFile(schema_path, schema_file, ios_base::in);

	string schema_name;
	while (getline(schema_file, schema_name)) {
		// create the schema in the catalog
		CreateSchemaInformation info;
		info.schema = schema_name;
		info.if_not_exists = true;
		database.catalog.CreateSchema(context.ActiveTransaction(), &info);

		// now read the list of the tables belonging to this schema
		auto schema_directory_path = FileSystem::JoinPath(storage_path_base, schema_name);
		auto table_list_path = FileSystem::JoinPath(schema_directory_path, TABLE_LIST_FILE);

		// read the list of tables
		fstream table_list_file;
		FstreamUtil::OpenFile(table_list_path, table_list_file, ios_base::in);
		string table_name;
		while (getline(table_list_file, table_name)) {
			// get the information of the table
			auto table_directory_path = FileSystem::JoinPath(schema_directory_path, table_name);
			auto table_meta_name = FileSystem::JoinPath(table_directory_path, TABLE_FILE);

			fstream table_file;
			FstreamUtil::OpenFile(table_meta_name, table_file, ios_base::binary | ios_base::in);
			auto result = FstreamUtil::ReadBinary(table_file);

			// deserialize the CreateTableInformation
			auto table_file_size = FstreamUtil::GetFileSize(table_file);
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
				auto chunk_name =
				    FileSystem::JoinPath(table_directory_path, "chunk-" + to_string(chunk_count) + ".bin");
				if (!FileSystem::FileExists(chunk_name)) {
					break;
				}

				fstream chunk_file;
				FstreamUtil::OpenFile(chunk_name, chunk_file, ios_base::binary | ios_base::in);
				auto result = FstreamUtil::ReadBinary(chunk_file);
				// deserialize the chunk
				DataChunk insert_chunk;
				auto chunk_file_size = FstreamUtil::GetFileSize(chunk_file);
				Deserializer source((uint8_t *)result.get(), chunk_file_size);
				insert_chunk.Deserialize(source);
				// insert the chunk into the table
				table->storage->Append(*table, context, insert_chunk);
				chunk_count++;
			}
		}

		auto view_list_path = FileSystem::JoinPath(schema_directory_path, VIEW_LIST_FILE);
		fstream view_list_file;
		FstreamUtil::OpenFile(view_list_path, view_list_file, ios_base::in);
		string view_name;
		while (getline(view_list_file, view_name)) {
			auto view_file_path = FileSystem::JoinPath(schema_directory_path, view_name + ".view");
			fstream view_file;
			FstreamUtil::OpenFile(view_file_path, view_file, ios_base::binary | ios_base::in);
			auto result = FstreamUtil::ReadBinary(view_file);
			// deserialize the CreateViewInformation
			auto view_file_size = FstreamUtil::GetFileSize(view_file);
			Deserializer source((uint8_t *)result.get(), view_file_size);
			auto info = ViewCatalogEntry::Deserialize(source);
			// create the table inside the catalog
			database.catalog.CreateView(context.ActiveTransaction(), info.get());
		}
	}
	context.transaction.Commit();
	return iteration;
}

void StorageManager::CreateCheckpoint(int iteration) {
	auto transaction = database.transaction_manager.StartTransaction();

	assert(iteration == 0 || iteration == 1);
	auto storage_path_base = FileSystem::JoinPath(path, STORAGE_FILES[iteration]);
	if (FileSystem::DirectoryExists(storage_path_base)) {
		// have a leftover directory
		// remove it
		FileSystem::RemoveDirectory(storage_path_base);
	}
	// create the directory
	FileSystem::CreateDirectory(storage_path_base);

	// first we have to access the schemas
	auto schema_path = FileSystem::JoinPath(storage_path_base, SCHEMA_FILE);

	fstream schema_file;
	FstreamUtil::OpenFile(schema_path, schema_file, ios_base::out);

	vector<SchemaCatalogEntry *> schemas;
	// scan the schemas and write them to the schemas.csv file
	database.catalog.schemas.Scan(*transaction, [&](CatalogEntry *entry) {
		schema_file << entry->name << '\n';
		schemas.push_back((SchemaCatalogEntry *)entry);
	});
	FstreamUtil::CloseFile(schema_file);

	// now for each schema create a directory
	for (auto &schema : schemas) {
		// FIXME: schemas can have unicode, do something for file systems, maybe hash?
		auto schema_directory_path = FileSystem::JoinPath(storage_path_base, schema->name);
		assert(!FileSystem::DirectoryExists(schema_directory_path));
		// create the directory
		FileSystem::CreateDirectory(schema_directory_path);
		// create the file holding the list of tables for the schema
		auto table_list_path = FileSystem::JoinPath(schema_directory_path, TABLE_LIST_FILE);
		fstream table_list_file;
		FstreamUtil::OpenFile(table_list_path, table_list_file, ios_base::out);

		auto view_list_path = FileSystem::JoinPath(schema_directory_path, VIEW_LIST_FILE);
		fstream view_list_file;
		FstreamUtil::OpenFile(view_list_path, view_list_file, ios_base::out);

		// create the list of tables for the schema
		vector<TableCatalogEntry *> tables;
		vector<ViewCatalogEntry *> views;

		schema->tables.Scan(*transaction, [&](CatalogEntry *entry) {
			switch (entry->type) {
			case CatalogType::TABLE:
				table_list_file << entry->name << '\n';
				tables.push_back((TableCatalogEntry *)entry);
				break;
			case CatalogType::VIEW:
				view_list_file << entry->name << '\n';
				views.push_back((ViewCatalogEntry *)entry);
				break;
			default:
				break;
			}
		});
		FstreamUtil::CloseFile(table_list_file);
		FstreamUtil::CloseFile(view_list_file);

		// now for each table, write the column meta information and the actual data
		for (auto &table : tables) {
			// first create a directory for the table information
			// FIXME: same problem as schemas, unicode and file systems may not agree
			auto table_directory_path = FileSystem::JoinPath(schema_directory_path, table->name);
			assert(!FileSystem::DirectoryExists(table_directory_path));
			// create the directory
			FileSystem::CreateDirectory(table_directory_path);

			auto table_meta_name = FileSystem::JoinPath(table_directory_path, TABLE_FILE);
			fstream table_file;
			FstreamUtil::OpenFile(table_meta_name, table_file, ios_base::binary | ios_base::out);

			// serialize the table information to a file
			Serializer serializer;
			table->Serialize(serializer);
			auto data = serializer.GetData();
			table_file.write((char *)data.data.get(), data.size);
			FstreamUtil::CloseFile(table_file);

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
				auto chunk_name =
				    FileSystem::JoinPath(table_directory_path, "chunk-" + to_string(chunk_count) + ".bin");
				fstream chunk_file;
				FstreamUtil::OpenFile(chunk_name, chunk_file, ios_base::binary | ios_base::out);

				Serializer serializer;
				chunk.Serialize(serializer);
				auto data = serializer.GetData();
				chunk_file.write((char *)data.data.get(), data.size);
				FstreamUtil::CloseFile(chunk_file);

				chunk_count++;
			}
		}

		// now for each view, write the query node and aliases to a file
		for (auto &view : views) {
			auto view_file_path = FileSystem::JoinPath(schema_directory_path, view->name + ".view");
			assert(!FileSystem::FileExists(view_file_path));
			fstream view_file;
			FstreamUtil::OpenFile(view_file_path, view_file, ios_base::binary | ios_base::out);

			// serialize the view information to a file
			Serializer serializer;
			view->Serialize(serializer);
			auto data = serializer.GetData();
			view_file.write((char *)data.data.get(), data.size);
			FstreamUtil::CloseFile(view_file);
		}
	}
	// all the writes have been flushed and the entire database has been written
	// now we create the temporary meta information file
	auto meta_path = FileSystem::JoinPath(path, DATABASE_TEMP_INFO_FILE);
	fstream meta_file;
	FstreamUtil::OpenFile(meta_path, meta_file, ios_base::out);

	//! Write information to the meta file
	meta_file << STORAGE_VERSION << '\n';
	meta_file << iteration << '\n';
	FstreamUtil::CloseFile(meta_file);

	// now we move the meta information file over the old meta information file
	// this signifies a "completion" of the checkpoint
	auto permanent_meta_path = FileSystem::JoinPath(path, DATABASE_INFO_FILE);
	FileSystem::MoveFile(meta_path, permanent_meta_path);

	// we are now done writing
	// we can delete the directory for the other iteration because we do not need it anymore for consistency
	auto other_storage_path = FileSystem::JoinPath(path, STORAGE_FILES[1 - iteration]);
	auto other_wal_path = FileSystem::JoinPath(path, WAL_FILES[1 - iteration]);
	if (FileSystem::DirectoryExists(other_storage_path)) {
		FileSystem::RemoveDirectory(other_storage_path);
	}
	if (FileSystem::FileExists(other_wal_path)) {
		FileSystem::RemoveFile(other_wal_path);
	}
	transaction->Rollback();
}

void StorageManager::BuildDataBlocks() {

	// First we have to define how many chuncks we fit in a block
	// We need #tuples, #columns and data type to calculate the size of a block
}
