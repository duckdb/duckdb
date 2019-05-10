#include "storage/checkpoint_manager.hpp"
#include "storage/block_manager.hpp"
#include "storage/meta_block_reader.hpp"

#include "common/serializer.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/types/null_value.hpp"

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
		WriteSchema(*transaction, *schema);
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

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteSchema(Transaction &transaction, SchemaCatalogEntry &schema) {
	// write the schema data
	schema.Serialize(*metadata_writer);
	// then, we fetch the tables information
	vector<TableCatalogEntry *> tables;
	schema.tables.Scan(transaction, [&](CatalogEntry *entry) { tables.push_back((TableCatalogEntry *)entry); });
	// and store the amount of tables in the meta_block
	metadata_writer->Write<uint32_t>(tables.size());
	for (auto &table : tables) {
		WriteTable(transaction, *table);
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

//===--------------------------------------------------------------------===//
// Table Metadata
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteTable(Transaction &transaction, TableCatalogEntry &table) {
	// write the table meta data
	table.Serialize(*metadata_writer);
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

//===--------------------------------------------------------------------===//
// Table Data
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteTableData(Transaction &transaction, TableCatalogEntry &table) {
	assert(blocks.size() == 0 && offsets.size() == 0 && tuple_counts.size() == 0 && row_numbers.size() == 0 && data_pointers.size() == 0);
	// when writing table data we write columns to individual blocks
	// we scan the underlying table structure and write to the blocks
	// then flush the blocks to disk when they are full

	// initialize scan structures to prepare for the scan
	ScanStructure ss;
	table.storage->InitializeScan(ss);
	vector<column_t> column_ids;
	for (auto &column : table.columns) {
		column_ids.push_back(column.oid);
	}
    //! get all types of the table and initialize the chunk
	auto types = table.GetTypes();
	DataChunk chunk;
	chunk.Initialize(types);

	// the pointers to the written column data for this table
	data_pointers.resize(table.columns.size());
	// we want to fetch all the column ids from the scan
	// so make a list of all column ids of the table
	for (uint64_t i = 0; i < table.columns.size(); i++) {
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
		table.storage->Scan(transaction, chunk, column_ids, ss);
		if (chunk.size() == 0) {
			break;
		}
		// for each column, we append whatever we can fit into the block
		for(uint64_t i = 0; i < table.columns.size(); i++) {
			assert(chunk.data[i].type == GetInternalType(table.columns[i].type));
			WriteColumnData(chunk, i);
		}
	}
	// finally we write the blocks that were not completely filled to disk
	// FIXME: pack together these unfilled blocks
	for(uint64_t i = 0; i < table.columns.size(); i++) {
		// we only write blocks that have data in them
		if (offsets[i] > DATA_BLOCK_HEADER_SIZE) {
			FlushBlock(i);
		}
	}
	// finally write the table storage information
	WriteDataPointers();
	// clear the data structures used for writing
	blocks.clear();
	offsets.clear();
	tuple_counts.clear();
	row_numbers.clear();
	data_pointers.clear();
}

//===--------------------------------------------------------------------===//
// Write Column Data to Block
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteColumnData(DataChunk &chunk, uint64_t column_index) {
	TypeId type = chunk.data[column_index].type;
	if (TypeIsConstantSize(type)) {
		// constant size type: simple memcpy
		// first check if we can fit the chunk into the block
		uint64_t size = GetTypeIdSize(type) * chunk.size();
		if (offsets[column_index] + size >= blocks[column_index]->size) {
			// data does not fit into block, flush block to disk
			// FIXME: append part of data that still fits into block
			FlushBlock(column_index);
		}
		// data fits into block, write it and update the offset
		auto ptr = blocks[column_index]->buffer + offsets[column_index];
		VectorOperations::CopyToStorage(chunk.data[column_index], ptr);
		offsets[column_index] += size;
		tuple_counts[column_index] += chunk.size();
	} else {
		assert(type == TypeId::VARCHAR);
		// we inline strings into the block
		VectorOperations::ExecType<const char*>(chunk.data[column_index], [&](const char *val, size_t i, size_t k) {
			if (chunk.data[column_index].nullmask[i]) {
				// NULL value
				val = NullValue<const char*>();
			}
			char *bufptr = (char*) blocks[column_index]->buffer;
			uint64_t size = strlen(val);
			// first write the size
			if (offsets[column_index] + sizeof(uint64_t) >= blocks[column_index]->size) {
				FlushBlock(column_index);
			}
			tuple_counts[column_index]++;
			memcpy(bufptr + offsets[column_index], &size, sizeof(uint64_t));
			offsets[column_index] += sizeof(uint64_t);
			uint64_t pos = 0;
			// now write the actual string
			while(pos < size) {
				uint64_t to_write = std::min(size - pos, blocks[column_index]->size - offsets[column_index]);
				memcpy(bufptr + offsets[column_index], val + pos, to_write);
				pos += to_write;
				offsets[column_index] += to_write;
				if (pos != size) {
					// could not write entire string, flush the block
					FlushBlock(column_index);
				}
			}
		});
	}
}

void CheckpointManager::FlushBlock(uint64_t column_index) {
	// get a block id
	blocks[column_index]->id = block_manager.GetFreeBlockId();
	// construct the data pointer, FIXME: add statistics as well
	DataPointer data_pointer;
	data_pointer.block_id = blocks[column_index]->id;
	data_pointer.offset = 0;
	data_pointer.tuple_count = tuple_counts[column_index];
	data_pointers[column_index].push_back(data_pointer);
	// write the block
	block_manager.Write(*blocks[column_index]);

	offsets[column_index] = DATA_BLOCK_HEADER_SIZE;
	row_numbers[column_index] += tuple_counts[column_index];
	tuple_counts[column_index] = 0;
}

//===--------------------------------------------------------------------===//
// Read Table Data
//===--------------------------------------------------------------------===//
void CheckpointManager::ReadTableData(ClientContext &context, TableCatalogEntry &table, MetaBlockReader &reader) {
	assert(blocks.size() == 0 && offsets.size() == 0 && tuple_counts.size() == 0 && indexes.size() == 0 && data_pointers.size() == 0);

	uint64_t column_count = table.columns.size();
	assert(column_count > 0);

	// load the data pointers for the table
	ReadDataPointers(column_count, reader);
	if (data_pointers[0].size() == 0) {
		// if the table is empty there is no loading to be done
		return;
	}

	// initialize the blocks for each column with the first block
	blocks.resize(column_count);
	offsets.resize(column_count);
	tuple_counts.resize(column_count);
	indexes.resize(column_count);
	for(uint64_t col = 0; col < column_count; col++) {
		ReadBlock(col, 0);
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
		insert_chunk.Reset();
		for(uint64_t col = 0; col < column_count; col++) {
			TypeId type = insert_chunk.data[col].type;
			if (TypeIsConstantSize(type)) {
				while(insert_chunk.data[col].count < STANDARD_VECTOR_SIZE) {
					uint64_t tuples_left = data_pointers[col][indexes[col]].tuple_count - tuple_counts[col];
					if (tuples_left == 0) {
						// no tuples left in this block
						// move to next block
						if (indexes[col] + 1 == data_pointers[col].size()) {
							// no next block
							break;
						}
						ReadBlock(col, indexes[col] + 1);
						tuples_left = data_pointers[col][indexes[col]].tuple_count - tuple_counts[col];
					}
					Vector storage_vector(types[col], (char*) blocks[col]->buffer + offsets[col]);
					storage_vector.count = std::min((uint64_t) STANDARD_VECTOR_SIZE, tuples_left);
					VectorOperations::AppendFromStorage(storage_vector, insert_chunk.data[col]);

					tuple_counts[col] += storage_vector.count;
					offsets[col] += storage_vector.count * GetTypeIdSize(types[col]);
				}
			} else {
				assert(type == TypeId::VARCHAR);
				while(insert_chunk.data[col].count < STANDARD_VECTOR_SIZE) {
					if (tuple_counts[col] >= data_pointers[col][indexes[col]].tuple_count) {
						// no tuples left in this block
						// move to next block
						if (indexes[col] + 1 == data_pointers[col].size()) {
							// no next block
							break;
						}
						ReadBlock(col, indexes[col] + 1);
					}
					ReadString(insert_chunk.data[col], col);
					tuple_counts[col]++;
				}
			}
		}
		if (insert_chunk.size() == 0) {
			break;
		}

		table.storage->Append(table, context, insert_chunk);
	}
	data_pointers.clear();
	blocks.clear();
	indexes.clear();
	offsets.clear();
	tuple_counts.clear();
}

void CheckpointManager::ReadBlock(uint64_t col, uint64_t block_nr) {
	blocks[col] = make_unique<Block>(data_pointers[col][block_nr].block_id);
	offsets[col] = data_pointers[col][block_nr].offset + DATA_BLOCK_HEADER_SIZE;
	tuple_counts[col] = 0;
	indexes[col] = block_nr;
	// read the data for the block from disk
	block_manager.Read(*blocks[col]);
}

void CheckpointManager::ReadString(Vector &vector, uint64_t col) {
	// first read the length of the string
	char *ptr = (char*) blocks[col]->buffer + offsets[col];
	uint64_t length = *((uint64_t*) ptr);
	offsets[col] += sizeof(uint64_t);
	ptr += sizeof(uint64_t);
	// now read the actual string
	// first allocate space for the string
	uint64_t pos = 0;
	auto data = unique_ptr<char[]>(new char[length + 1]);
	data[length] = '\0';
	// now read the string
	while(pos < length) {
		uint64_t to_read = std::min(length - pos, blocks[col]->size - offsets[col]);
		memcpy(data.get() + pos, blocks[col]->buffer + offsets[col], to_read);
		pos += to_read;
		offsets[col] += to_read;
		if (offsets[col] == blocks[col]->size) {
			ReadBlock(col, indexes[col] + 1);
		}
	}
	if (strcmp(data.get(), NullValue<const char*>()) == 0) {
		vector.nullmask[vector.count] = true;
	} else {
		auto strings = (const char**) vector.data;
		strings[vector.count] = vector.string_heap.AddString(data.get(), length);
	}
	vector.count++;
}


//===--------------------------------------------------------------------===//
// Data Pointers for Table Data
//===--------------------------------------------------------------------===//
void CheckpointManager::WriteDataPointers() {
	for (uint64_t i = 0; i < data_pointers.size(); i++) {
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

void CheckpointManager::ReadDataPointers(uint64_t column_count, MetaBlockReader &reader) {
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
}
