#include "storage/checkpoint_manager.hpp"
#include "storage/block_manager.hpp"

#include "common/serializer.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"

#include "main/database.hpp"

#include "transaction/transaction_manager.hpp"

using namespace duckdb;
using namespace std;

constexpr uint64_t CheckpointManager::DATA_BLOCK_HEADER_SIZE;

CheckpointManager::CheckpointManager(StorageManager &manager) :
	manager(manager), block_manager(*manager.block_manager), database(manager.database) {

}

void CheckpointManager::CreateCheckpoint() {
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

void CheckpointManager::WriteSchema(Transaction &transaction, SchemaCatalogEntry *schema) {
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

void CheckpointManager::WriteTable(Transaction &transaction, TableCatalogEntry *table) {
	// write the table meta data
	Serializer serializer;
	//! and serialize the table information
	table->Serialize(serializer);
	auto serialized_data = serializer.GetData();
	//! write the seralized size and data
	metadata_writer->Write<uint32_t>(serialized_data.size);
	metadata_writer->Write((const char *)serialized_data.data.get(), serialized_data.size);
	//! write the blockId for the table info
	metadata_writer->Write(tabledata_writer->block->id);
	//! and the offset to where the info starts
	metadata_writer->Write(tabledata_writer->offset);
	// now we need to write the table data
	WriteTableData(transaction, table);
}

void CheckpointManager::WriteTableData(Transaction &transaction, TableCatalogEntry *table) {
	// when writing table data we write columns to individual blocks
	// we scan the underlying table structure and write to the blocks
	// then flush the blocks to disk when they are full
	ScanStructure ss;
	table->storage->InitializeScan(ss);

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
	vector<column_t> column_ids;
	for (auto &column : table->columns) {
		// for each column, create a block that serves as the buffer for that blocks data
		blocks.push_back(make_unique<Block>(-1));
		// initialize offsets, tuple counts and row number sizes
		offsets.push_back(DATA_BLOCK_HEADER_SIZE);
		tuple_counts.push_back(0);
		row_numbers.push_back(0);
		column_ids.push_back(column.oid);
	}
    //! get all types of the table and initialize the chunk
	auto types = table->GetTypes();
	DataChunk chunk;
	chunk.Initialize(types);
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
				// for (size_t j = 0; j < size(); j++) {
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
			data_pointers[i].push_back(data_pointer);
			// write the block
			block_manager.Flush(*blocks[i]);
		}
	}
	// finally write the table storage information
    // first, write the amount of columns
	tabledata_writer->Write<uint32_t>(table->columns.size());
	for (size_t i = 0; i < table->columns.size(); i++) {
        // get a reference to the data column
		auto &data_pointer_list = data_pointers[i];
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
