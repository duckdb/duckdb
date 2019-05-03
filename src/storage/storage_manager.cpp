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
#include "common/types/chunk_collection.hpp"

#include <limits>

constexpr const int64_t STORAGE_VERSION = 1;

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
	// first check if the database exists
	string wal_path = path + ".wal";
	block_manager = make_unique<SingleFileBlockManager>(path);
	// if (!DirectoryExists(path)) {
	// 	// have to create the directory
	// 	CreateDirectory(path);
	// } else {
	// 	// load from the main storage if it exists
	// 	LoadFromStorage();
	// 	// directory already exists
	// 	// verify that it is an existing database
	// 	if (FileExists(wal_path)) {
	// 		// replay the WAL
	// 		wal.Replay(wal_path);
	// 		// checkpoint the WAL
	// 		CreateCheckpoint();
	// 		// remove the WAL
	// 		RemoveFile(wal_path);
	// 	}
	// }
	// initialize the WAL file
	wal.Initialize(wal_path);
}

void StorageManager::CreateCheckpoint() {
	auto transaction = database.transaction_manager.StartTransaction();

    //! Set up the writers for the checkpoints
	metadata_writer = make_unique<MetaBlockWriter>(*block_manager);
	tabledata_writer = make_unique<MetaBlockWriter>(*block_manager);

	// get the
	auto meta_block = metadata_writer->current_block->id;

	vector<SchemaCatalogEntry *> schemas;
	// we scan the schemas
	database.catalog.schemas.Scan(*transaction,
	                              [&](CatalogEntry *entry) { schemas.push_back((SchemaCatalogEntry *)entry); });
	// write the actual data into the database
	// write the amount of schemas
	metadata_writer->Write<uint32_t>(schemas.size());
	for (auto &schema : schemas) {
		WriteSchema(*transaction, schema);
	}
	// flush all the data to disk
	metadata_writer->Flush();
	tabledata_writer->Flush();
	// finally write the updated header
	block_manager->WriteHeader(STORAGE_VERSION, meta_block);
}

void StorageManager::WriteSchema(Transaction &transaction, SchemaCatalogEntry *schema) {
	// write the name of the schema
	metadata_writer->WriteString(schema->name);
	// then, we fetch the tables information
	vector<TableCatalogEntry *> tables;
	schema->tables.Scan(transaction, [&](CatalogEntry *entry) { tables.push_back((TableCatalogEntry *)entry); });
	// and store the amount of tables in the meta_block
	metadata_writer->Write<uint32_t>(tables.size());
	for (auto &table : tables) {
		WriteTable(transaction, table);
	}
}

void StorageManager::WriteTable(Transaction &transaction, TableCatalogEntry *table) {
	// write the table meta data
	Serializer serializer;
	//! and serialize the table information
	table->Serialize(serializer);
	auto serialized_data = serializer.GetData();
	//! write the seralized size
	metadata_writer->Write<uint32_t>(serialized_data.size);
	//! and the data itself
	metadata_writer->Write((const char *)serialized_data.data.get(), serialized_data.size);
	//! write the blockId for the table info
	metadata_writer->Write(tabledata_writer->current_block->id);
	//! and the offset to where the info starts
	metadata_writer->Write(tabledata_writer->current_block->offset);
	// now we need to write the table data
	WriteTableData(transaction, table);
}

void StorageManager::WriteTableData(Transaction &transaction, TableCatalogEntry *table) {
	// buffer chunks until our big buffer is full
	auto collection = make_unique<ChunkCollection>();
	ScanStructure ss;
	table->storage->InitializeScan(ss);
	// we want to fetch all the column ids from the scan
	// so make a list of all column ids of the table
	vector<column_t> column_ids;
	for (auto &column : table->columns) {
		column_ids.push_back(column.oid);
	}
    //! get all types and initialize the chunk
	auto types = table->GetTypes();
	DataChunk chunk;
	chunk.Initialize(types);
    //! This are pointers to the column data
	auto column_pointers = unique_ptr<vector<DataPointer>[]>(new vector<DataPointer>[table->columns.size()]);
	while (true) {
		chunk.Reset();
		// we scan the chunk
		table->storage->Scan(transaction, chunk, column_ids, ss);
		collection->Append(chunk);
		if (collection->count >= CHUNK_THRESHOLD || chunk.size() == 0) {
			if (collection->count > 0) {
				// chunk collection is full or scan is finished: now actually store the data
				for (size_t i = 0; i < table->storage->types.size(); i++) {
					// for each column store the data
					WriteColumnData(table, *collection, i, column_pointers[i]);
				}
				// reset the collection
				collection = make_unique<ChunkCollection>();
			}
			if (chunk.size() == 0) {
				break;
			}
		}
	}
	// write the table storage information
    // first, write the amount of columns
	tabledata_writer->Write<uint32_t>(table->columns.size());
	for (size_t i = 0; i < table->columns.size(); i++) {
        // get a reference to the data column
		auto &data_pointer_list = column_pointers[i];
		tabledata_writer->Write<uint32_t>(data_pointer_list.size());
		// then write the data pointers themselves
		for (size_t k = 0; k < data_pointer_list.size(); k++) {
			auto &data_pointer = data_pointer_list[k];
			tabledata_writer->Write<double>(data_pointer.min);
			tabledata_writer->Write<double>(data_pointer.max);
			tabledata_writer->Write<block_id_t>(data_pointer.block_id);
			tabledata_writer->Write<uint32_t>(data_pointer.offset);
		}
	}
}

template <class T>
static void WriteDataFromVector(Vector &vector, size_t start, size_t end, double &min, double &max, bool *&nullmask,
                                T *&data) {
	T *vector_data = (T *)vector.data;
	for (size_t k = start; k < end; k++) {
		*nullmask = vector.nullmask[k];
		*data = vector_data[k];

		//! store min and max
		if (*data < min) {
			min = *data;
		}
		if (*data > max) {
			max = *data;
		}

		nullmask++;
		data++;
	}
}

template <class T>
static char *WriteDataFromCollection(ChunkCollection &collection, size_t column_index, size_t start, size_t end,
                                     double &min, double &max, bool *nullmask, char *dataptr) {
	auto data = (T *)dataptr;
	// write
	size_t start_chunk = collection.LocateChunk(start);
	size_t end_chunk = collection.LocateChunk(end);

	// first chunk: might not write everything
	size_t chunk_start = start % STANDARD_VECTOR_SIZE;
	WriteDataFromVector(collection.chunks[start_chunk]->data[column_index], chunk_start,
	                    collection.chunks[start_chunk]->size(), min, max, nullmask, data);
	// for everything in the middle we write everything
	for (size_t chunk_idx = start_chunk + 1; chunk_idx < end_chunk; chunk_idx++) {
		WriteDataFromVector(collection.chunks[chunk_idx]->data[column_index], 0, collection.chunks[chunk_idx]->size(),
		                    min, max, nullmask, data);
	}
	// last chunk: might not write everything either
	size_t chunk_end = end % STANDARD_VECTOR_SIZE;
	WriteDataFromVector(collection.chunks[end_chunk]->data[column_index], 0, chunk_end, min, max, nullmask, data);
	return (char *)data;
}

static char *WriteDataFromCollection(TypeId type, ChunkCollection &collection, size_t column_index, size_t start,
                                     size_t end, double &min, double &max, bool *nullmask, char *data) {
	switch (type) {
	case TypeId::BOOLEAN:
		return WriteDataFromCollection<bool>(collection, column_index, start, end, min, max, nullmask, data);
	case TypeId::TINYINT:
		return WriteDataFromCollection<int8_t>(collection, column_index, start, end, min, max, nullmask, data);
	case TypeId::SMALLINT:
		return WriteDataFromCollection<int16_t>(collection, column_index, start, end, min, max, nullmask, data);
	case TypeId::INTEGER:
		return WriteDataFromCollection<int32_t>(collection, column_index, start, end, min, max, nullmask, data);
	case TypeId::BIGINT:
		return WriteDataFromCollection<int64_t>(collection, column_index, start, end, min, max, nullmask, data);
	case TypeId::FLOAT:
		return WriteDataFromCollection<float>(collection, column_index, start, end, min, max, nullmask, data);
	case TypeId::DOUBLE:
		return WriteDataFromCollection<double>(collection, column_index, start, end, min, max, nullmask, data);
	default:
		throw NotImplementedException("Unimplemented type for checkpoint write");
	}
}

void StorageManager::WriteColumnData(TableCatalogEntry *table, ChunkCollection &collection, int32_t column_index,
                                     vector<DataPointer> &column_pointers) {
	auto internal_type = table->storage->types[column_index];
	switch (internal_type) {
	case TypeId::VARCHAR:
		// string column
		throw NotImplementedException("Not checkpointing string columns yet");
		break;
	default:
		// numeric column
		// tuple_count, compression_type, data_offset
		size_t initial_size = BLOCK_SIZE - (sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t));
		size_t remaining_size = initial_size;
		// take tuples until we can't anymore
		size_t last_write = 0;
		for (size_t i = 0; i < collection.count; i++) {
			int tuple_size = GetTypeIdSize(internal_type) + sizeof(char);
			if (tuple_size > remaining_size || i + 1 == collection.count) {
				auto stored_buffer = unique_ptr<char[]>(new char[BLOCK_SIZE]);
				char *buffer = stored_buffer.get();

				// set the metadata first
				size_t tuple_count = i - last_write;
				// tuple count
				*((uint32_t *)buffer) = tuple_count;
				buffer += sizeof(uint32_t);
				// compression type
				*((uint32_t *)buffer) = 0;
				buffer += sizeof(uint32_t);
				// data offset
				*((uint32_t *)buffer) = tuple_count * sizeof(char);
				buffer += sizeof(uint32_t);

				DataPointer data_pointer;
				data_pointer.min = numeric_limits<double>::max();
				data_pointer.max = numeric_limits<double>::min();

				bool *nullmask = (bool *)buffer;
				char *end =
				    WriteDataFromCollection(internal_type, collection, column_index, last_write, i, data_pointer.min,
				                            data_pointer.max, nullmask, buffer + tuple_count * sizeof(char));

				auto block = block_manager->CreateBlock();
				block->Write(stored_buffer.get(), end - stored_buffer.get());

				data_pointer.block_id = block->id;
				data_pointer.offset = 0;

				column_pointers.push_back(data_pointer);

				last_write = i;
			}
		}

		break;
	}
}

void StorageManager::LoadFromStorage() {


}

// void StorageManager::CreateCheckpoint2(int iteration) {
// 	// we need a transaction to perform this operation
// 	auto transaction = database.transaction_manager.StartTransaction();

// 	assert(iteration == 0 || iteration == 1);
// 	auto storage_path_base = JoinPath(path, STORAGE_FILES[iteration]);


// 	// this is where our database will be stored
// 	if (DirectoryExists(storage_path_base)) {
// 		// have a leftover directory
// 		// remove it
// 		RemoveDirectory(storage_path_base);
// 	}
// 	// create the directory for the database
// 	CreateDirectory(storage_path_base);
// 	// first we have to access the schemas
// 	vector<SchemaCatalogEntry *> schemas;
// 	// we scan the schemas
// 	database.catalog.schemas.Scan(*transaction,
// 	                              [&](CatalogEntry *entry) { schemas.push_back((SchemaCatalogEntry *)entry); });
// 	// and now we create a metablock writer to write the schemas
// 	MetaBlockWriter metainfo_writer(*block_manager);
// 	// first we write the amount of schemas
// 	metainfo_writer.Write<uint32_t>(schemas.size());
// 	// now for each schema we write the meta info of the schema
// 	for (auto &schema : schemas) {
// 		// first the schema name
// 		metainfo_writer.WriteString(schema->name);
// 		// then, we fetch the tables
// 		vector<TableCatalogEntry *> tables;
// 		schema->tables.Scan(*transaction, [&](CatalogEntry *entry) { tables.push_back((TableCatalogEntry *)entry); });
// 		// and store the amount of tables
// 		metainfo_writer.Write<uint32_t>(tables.size());
// 		// now for each table we write the meta info of the table
// 		for (auto &table : tables) {
// 			// serialize the table information
// 			Serializer serializer;
// 			table->Serialize(serializer);
// 			auto table_serialized = serializer.GetData();
// 			// write the size of the serialized info
// 			metainfo_writer.Write<uint32_t>(table_serialized.size);
// 			// and the serealized info itself
// 			metainfo_writer.Write(reinterpret_cast<char *>(table_serialized.data.get()), table_serialized.size);
// 			// now we write the actual data
// 			// we do this by performing a scan over the table
// 			ScanStructure ss;
// 			table->storage->InitializeScan(ss);
// 			// storing the column ids
// 			vector<column_t> column_ids(table->columns.size()); // vector with 100 ints.
// 			for_each(begin(table->columns), end(table->columns),
// 			         [&](ColumnDefinition &column) { column_ids.push_back(column.oid); }); // Fill with 0, 1, ..., 99.
// 			// and column types
// 			auto types = table->GetTypes();
// 			DataChunk chunk;
// 			chunk.Initialize(types);
// 			size_t chunk_count = 1;
// 			MetaBlockWriter metadata_writer(*block_manager);
// 			block_manager->SetMetadataBlockId(metadata_writer.current_block->id);
// 			auto data_block = block_manager->CreateBlock();
// 			metadata_writer.Write<block_id_t>(data_block->id);
// 			// Then we iterate over the data to build the dataBlocks
// 			while (true) {
// 				chunk.Reset();
// 				// we scan the chunk
// 				table->storage->Scan(*transaction, chunk, column_ids, ss);
// 				// and check whether there is data
// 				if (chunk.size() == 0) {
// 					metadata_writer.Write<size_t>(data_block->tuple_count);
// 					// chunk does not have data, so we flush the datablock to disk
// 					block_manager->Flush(data_block);
// 					// and finish the storing
// 					break;
// 				}
// 				// The chunk has data
// 				// now we check whether the data block has space
// 				if (data_block->HasNoSpace(chunk)) {
// 					// data block does not have space
// 					metadata_writer.Write<size_t>(data_block->tuple_count);
// 					// we need to flush it to disk
// 					block_manager->Flush(data_block);
// 					// and create a new data block
// 					data_block = block_manager->CreateBlock();
// 					metadata_writer.Write<block_id_t>(data_block->id);
// 				}
// 				// we have data and space
// 				// now we can append the data to a block
// 				block_manager->AppendDataToBlock(chunk, data_block, metadata_writer);
// 				chunk_count++;
// 			}
// 			// all data is on disk
// 			// now we store the id of the first data_block of the current table
// 			metainfo_writer.Write<block_id_t>(block_manager->GetMetadataBlockId());
// 		}
// 	}
// }

// void StorageManager::LoadCheckpoint() {
// 	ClientContext context(database);
// 	context.transaction.BeginTransaction();
// 	// for now we just assume block 0 is the starting point
// 	auto root_block = block_manager->GetBlock(0);
// 	MetaBlockReader metainfo_reader(move(root_block));
// 	uint32_t schema_count = metainfo_reader.Read<uint32_t>();
// 	for (size_t schema_idx = 0; schema_idx != schema_count; schema_idx++) {
// 		// create the schema in the catalog
// 		CreateSchemaInformation info;
// 		info.schema = metainfo_reader.ReadString();
// 		info.if_not_exists = true;
// 		database.catalog.CreateSchema(context.ActiveTransaction(), &info);
// 		// iterate over the tables owned by this schema
// 		auto table_count = metainfo_reader.Read<uint32_t>();
// 		for (size_t table_idx = 0; table_idx != table_count; table_idx++) {
// 			// deserialize the CreateTableInformation
// 			auto table_serialized_size = metainfo_reader.Read<uint32_t>();
// 			char *serialized_table_info{nullptr};
// 			metainfo_reader.Read(serialized_table_info, table_serialized_size);
// 			Deserializer source((uint8_t *)serialized_table_info, table_serialized_size);
// 			auto info = TableCatalogEntry::Deserialize(source);
// 			// create the table in the catalog
// 			database.catalog.CreateTable(context.ActiveTransaction(), info.get());
// 			// get the id for the meta_data block
// 			auto metadata_block_id = metainfo_reader.Read<block_id_t>();
// 			auto metadata_block = block_manager->GetBlock(metadata_block_id);
// 			// and the table info to link to the data
// 			auto *table = database.catalog.GetTable(context.ActiveTransaction(), info->schema, info->table);
// 			// now we append data to data chunks
// 			DataChunk chunk;
// 			auto types = table->GetTypes();
// 			chunk.Initialize(types);
// 			size_t chunk_count = 1;
// 			while (true) {
// 				char *buffer{nullptr};
// 				block_manager->LoadTableData(*(table->storage), chunk, move(metadata_block));
// 				// deserialize the chunk DataChunk insert_chunk;
// 				// insert the chunk into the table
// 				table->storage->Append(*table, context, chunk);
// 				chunk_count++;
// 			}
// 		}
// 	}
// }