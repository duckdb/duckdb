#include "storage/checkpoint_manager.hpp"
#include "storage/block_manager.hpp"
#include "storage/meta_block_reader.hpp"

#include "common/serializer.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"

#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_table_info.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"

#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

constexpr uint64_t CheckpointManager::DATA_BLOCK_HEADER_SIZE;

CheckpointManager::CheckpointManager(StorageManager &manager) :
	block_manager(*manager.block_manager), database(manager.database) {

}

void CheckpointManager::CreateCheckpoint() {
	// assert that the checkpoint manager hasn't been used before
	assert(!metadata_writer);

	auto transaction = database.transaction_manager->StartTransaction();

    //! Set up the writers for the checkpoints
	metadata_writer = make_unique<MetaBlockWriter>(block_manager);
	tabledata_writer = make_unique<MetaBlockWriter>(block_manager);

	// get the id of the first meta block
	block_id_t meta_block = metadata_writer->block->id;

	vector<SchemaCatalogEntry *> schemas;
	// we scan the schemas
	database.catalog->schemas.Scan(*transaction,
	                              [&](CatalogEntry *entry) { schemas.push_back((SchemaCatalogEntry *)entry); });
	// write the actual data into the database
	// write the amount of schemas
	metadata_writer->Write<uint32_t>(schemas.size());
	for (auto &schema : schemas) {
		WriteSchema(*transaction, schema);
	}
	// flush the meta data to disk
	metadata_writer->Flush();
	tabledata_writer->Flush();

	// finally write the updated header
	DatabaseHeader header;
	header.meta_block = meta_block;
	block_manager.WriteHeader(header);
}

void CheckpointManager::LoadFromStorage() {
	block_id_t meta_block = block_manager.GetMetaBlock();
	if (meta_block < 0) {
		// storage is empty
		return;
	}

	ClientContext context(database);
	context.transaction.BeginTransaction();
	// create the MetaBlockReader to read from the storage
	MetaBlockReader reader(block_manager, meta_block);
	uint32_t schema_count = reader.Read<uint32_t>();
	for(uint32_t i = 0; i < schema_count; i++) {
		ReadSchema(context, reader);
	}
	context.transaction.Commit();
}

void CheckpointManager::WriteSchema(Transaction &transaction, SchemaCatalogEntry *schema) {
	// write the schema data
	schema->Serialize(*metadata_writer);
	// then, we fetch the tables information
	vector<TableCatalogEntry *> tables;
	schema->tables.Scan(transaction, [&](CatalogEntry *entry) { tables.push_back((TableCatalogEntry *)entry); });
	// and store the amount of tables in the meta_block
	metadata_writer->Write<uint32_t>(tables.size());
	for (auto &table : tables) {
		WriteTable(transaction, table);
	}
	// FIXME: free list?
}

void CheckpointManager::ReadSchema(ClientContext &context, MetaBlockReader &reader) {
	// read the schema and create it in the catalog
	auto info = SchemaCatalogEntry::Deserialize(reader);
	// we set if_not_exists to true to ignore the failure of recreating the main schema
	info->if_not_exists = true;
	database.catalog->CreateSchema(context.ActiveTransaction(), info.get());

	// read the table count and recreate the tables
	uint32_t table_count = reader.Read<uint32_t>();
	for(uint32_t i = 0; i < table_count; i++) {
		ReadTable(context, reader);
	}
}

void CheckpointManager::WriteTable(Transaction &transaction, TableCatalogEntry *table) {
	// write the table meta data
	table->Serialize(*metadata_writer);
	//! write the blockId for the table info
	metadata_writer->Write<block_id_t>(tabledata_writer->block->id);
	//! and the offset to where the info starts
	metadata_writer->Write<uint64_t>(tabledata_writer->offset);
	// now we need to write the table data
	WriteTableData(transaction, table);
}

void CheckpointManager::ReadTable(ClientContext &context, MetaBlockReader &reader) {
	// deserilize the table meta data
	auto info = TableCatalogEntry::Deserialize(reader);
	// create the table in the schema
	database.catalog->CreateTable(context.ActiveTransaction(), info.get());
	// now load the table data
	auto block_id = reader.Read<block_id_t>();
	auto offset = reader.Read<uint64_t>();
	// read the block containing the table data
	MetaBlockReader table_data_reader(block_manager, block_id);
	table_data_reader.offset = offset;
	// fetch the table from the catalog for writing
	auto table = database.catalog->GetTable(context.ActiveTransaction(), info->schema, info->table);
	ReadTableData(context, *table, table_data_reader);
}

void CheckpointManager::WriteTableData(Transaction &transaction, TableCatalogEntry *table) {
	// when writing table data we write columns to individual blocks
	// we scan the underlying table structure and write to the blocks
	// then flush the blocks to disk when they are full

	// initialize scan structures to prepare for the scan
	ScanStructure ss;
	table->storage->InitializeScan(ss);
	vector<column_t> column_ids;
	for (auto &column : table->columns) {
		column_ids.push_back(column.oid);
	}
    //! get all types of the table and initialize the chunk
	auto types = table->GetTypes();
	DataChunk chunk;
	chunk.Initialize(types);

	// the intermediate buffers that we write our data to
	vector<unique_ptr<Block>> blocks;
	vector<uint64_t> offsets;
	vector<uint64_t> tuple_counts;
	vector<uint64_t> row_numbers;
	// the pointers to the written column data for this table
	vector<vector<DataPointer>> data_pointers;
	data_pointers.resize(table->columns.size());
	// we want to fetch all the column ids from the scan
	// so make a list of all column ids of the table
	for (uint64_t i = 0; i < table->columns.size(); i++) {
		// for each column, create a block that serves as the buffer for that blocks data
		blocks.push_back(make_unique<Block>(-1));
		// initialize offsets, tuple counts and row number sizes
		offsets.push_back(DATA_BLOCK_HEADER_SIZE);
		tuple_counts.push_back(0);
		row_numbers.push_back(0);
	}
	while (true) {
		chunk.Reset();
		// now scan the table to construct the blocks
		table->storage->Scan(transaction, chunk, column_ids, ss);
		if (chunk.size() == 0) {
			break;
		}
		// for each column, we append whatever we can fit into the block
		for(uint64_t i = 0; i < table->columns.size(); i++) {
			TypeId type = GetInternalType(table->columns[i].type);
			if (TypeIsConstantSize(type)) {
				// constant size type: simple memcpy
				// first check if we can fit the chunk into the block
				uint64_t size = GetTypeIdSize(type) * chunk.size();
				if (offsets[i] + size < blocks[i]->size) {
					// data fits into block, write it and update the offset
					auto ptr = blocks[i]->buffer + offsets[i];
					VectorOperations::CopyToStorage(chunk.data[i], ptr);
					offsets[i] += size;
					tuple_counts[i] += chunk.size();
				} else {
					// FIXME: flush full block to disk
					// update offsets, row numbers and tuple counts
					offsets[i] = DATA_BLOCK_HEADER_SIZE;
					row_numbers[i] += tuple_counts[i];
					tuple_counts[i] = 0;
					// FIXME: append to data_pointers
					// FIXME: reset block after writing to disk
					throw NotImplementedException("Data does not fit into single block!");
				}
			} else {
				// FIXME: deal with writing strings that are bigger than one block (just stretch over multiple blocks?)
				throw NotImplementedException("VARCHAR not implemented yet");
				// assert(type == TypeId::VARCHAR);
				// // strings are inlined into the blob
				// // we use null-padding to store them
				// const char **strings = (const char **)data[i].data;
				// for (uint64_t j = 0; j < size(); j++) {
				// 	auto source = strings[j] ? strings[j] : NullValue<const char *>();
				// 	serializer.WriteString(source);
				// }
			}
		}
	}
	// finally we write the blocks that were not completely filled to disk
	// FIXME: pack together these unfilled blocks
	for(uint64_t i = 0; i < table->columns.size(); i++) {
		// we only write blocks that have at least one tuple in them
		if (tuple_counts[i] > 0) {
			// get a block id
			blocks[i]->id = block_manager.GetFreeBlockId();
			// construct the data pointer, FIXME: add statistics as well
			DataPointer data_pointer;
			data_pointer.block_id = blocks[i]->id;
			data_pointer.offset = 0;
			data_pointer.tuple_count = tuple_counts[i];
			data_pointers[i].push_back(data_pointer);
			// write the block
			block_manager.Write(*blocks[i]);
		}
	}
	// finally write the table storage information
	for (uint64_t i = 0; i < table->columns.size(); i++) {
        // get a reference to the data column
		auto &data_pointer_list = data_pointers[i];
		tabledata_writer->Write<uint64_t>(data_pointer_list.size());
		// then write the data pointers themselves
		for (uint64_t k = 0; k < data_pointer_list.size(); k++) {
			auto &data_pointer = data_pointer_list[k];
			tabledata_writer->Write<double>(data_pointer.min);
			tabledata_writer->Write<double>(data_pointer.max);
			tabledata_writer->Write<uint64_t>(data_pointer.tuple_count);
			tabledata_writer->Write<block_id_t>(data_pointer.block_id);
			tabledata_writer->Write<uint32_t>(data_pointer.offset);
		}
	}
}

void CheckpointManager::ReadTableData(ClientContext &context, TableCatalogEntry &table, MetaBlockReader &reader) {
	uint64_t column_count = table.columns.size();
	assert(column_count > 0);

	// load the data pointers for the table
	vector<vector<DataPointer>> data_pointers;
	data_pointers.resize(column_count);
	for(uint64_t col = 0; col < column_count; col++) {
		uint64_t data_pointer_count = reader.Read<uint64_t>();
		for(uint64_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
			DataPointer data_pointer;
			data_pointer.min = reader.Read<double>();
			data_pointer.max = reader.Read<double>();
			data_pointer.tuple_count = reader.Read<uint64_t>();
			data_pointer.block_id = reader.Read<block_id_t>();
			data_pointer.offset = reader.Read<uint32_t>();
			data_pointers[col].push_back(data_pointer);
		}
	}

	if (data_pointers[0].size() == 0) {
		// if the table is empty there is no loading to be done
		return;
	}

	vector<unique_ptr<Block>> blocks(column_count);
	vector<uint64_t> indexes(column_count);
	vector<uint64_t> offsets(column_count);
	vector<uint64_t> tuples_loaded(column_count);
	// initialize the blocks with the first block
	for(uint64_t col = 0; col < column_count; col++) {
		blocks[col] = make_unique<Block>(data_pointers[col][0].block_id);
		offsets[col] = data_pointers[col][0].offset + DATA_BLOCK_HEADER_SIZE;
		tuples_loaded[col] = 0;
		indexes[col] = 0;
		// read the data for the block from disk
		block_manager.Read(*blocks[col]);
	}

	// construct a DataChunk to hold the intermediate objects we will push into the database
	vector<TypeId> types;
	for(auto &col : table.columns) {
		types.push_back(GetInternalType(col.type));
	}
	DataChunk insert_chunk;
	insert_chunk.Initialize(types);

	// now load the actual data
	while(true) {
		// construct a DataChunk by loading tuples from each of the columns
		// FIXME: handle multiple blocks
		// FIXME: handle different types
		// FIXME: handle strings
		insert_chunk.Reset();
		for(uint64_t col = 0; col < column_count; col++) {
			DataPointer &pointer = data_pointers[col][indexes[col]];
			Vector storage_vector(types[col], (char*) blocks[col]->buffer + offsets[col]);
			storage_vector.count = std::min((uint64_t) STANDARD_VECTOR_SIZE, pointer.tuple_count - tuples_loaded[col]);
			if (storage_vector.count == 0) {
				// finished appending
				return;
			}

			VectorOperations::AppendFromStorage(storage_vector, insert_chunk.data[col]);

			tuples_loaded[col] += storage_vector.count;
			offsets[col] += storage_vector.count * GetTypeIdSize(types[col]);
		}

		table.storage->Append(table, context, insert_chunk);
	}
}
