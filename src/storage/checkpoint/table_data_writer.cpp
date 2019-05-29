#include "storage/checkpoint/table_data_writer.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "common/types/null_value.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

TableDataWriter::TableDataWriter(CheckpointManager &manager) :
	manager(manager) {
}

void TableDataWriter::WriteTableData(Transaction &transaction, TableCatalogEntry &table) {
	assert(blocks.size() == 0);

	// when writing table data we write columns to individual blocks
	// we scan the underlying table structure and write to the blocks
	// then flush the blocks to disk when they are full

	// initialize scan structures to prepare for the scan
	DataTable::ScanState state;
	table.storage->InitializeScan(state);
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
	for (index_t i = 0; i < table.columns.size(); i++) {
		// for each column, create a block that serves as the buffer for that blocks data
		blocks.push_back(make_unique<Block>(INVALID_BLOCK));
		// initialize offsets, tuple counts and row number sizes
		offsets.push_back(0);
		tuple_counts.push_back(0);
		row_numbers.push_back(0);
	}
	while (true) {
		chunk.Reset();
		// now scan the table to construct the blocks
		table.storage->Scan(transaction, chunk, column_ids, state);
		if (chunk.size() == 0) {
			break;
		}
		// for each column, we append whatever we can fit into the block
		for (index_t i = 0; i < table.columns.size(); i++) {
			assert(chunk.data[i].type == GetInternalType(table.columns[i].type));
			WriteColumnData(chunk, i);
		}
	}
	// finally we write the blocks that were not completely filled to disk
	// FIXME: pack together these unfilled blocks
	for (index_t i = 0; i < table.columns.size(); i++) {
		// we only write blocks that have data in them
		if (offsets[i] > 0) {
			FlushBlock(i);
		}
	}
	// finally write the table storage information
	WriteDataPointers();
}

//===--------------------------------------------------------------------===//
// Write Column Data to Block
//===--------------------------------------------------------------------===//
void TableDataWriter::WriteColumnData(DataChunk &chunk, uint64_t column_index) {
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
		VectorOperations::ExecType<const char *>(chunk.data[column_index], [&](const char *val, size_t i, size_t k) {
			if (chunk.data[column_index].nullmask[i]) {
				// NULL value
				val = NullValue<const char *>();
			}
			WriteString(column_index, val);
		});
	}
}

void TableDataWriter::FlushBlock(uint64_t col) {
	// get a block id
	blocks[col]->id = manager.block_manager.GetFreeBlockId();
	// construct the data pointer, FIXME: add statistics as well
	DataPointer data_pointer;
	data_pointer.block_id = blocks[col]->id;
	data_pointer.offset = 0;
	data_pointer.row_start = row_numbers[col];
	data_pointer.tuple_count = tuple_counts[col];
	data_pointers[col].push_back(data_pointer);
	// write the block
	manager.block_manager.Write(*blocks[col]);

	offsets[col] = 0;
	row_numbers[col] += tuple_counts[col];
	tuple_counts[col] = 0;
}

void TableDataWriter::WriteString(uint64_t col, const char *val) {
	uint64_t size = strlen(val);
	// first write the length of the string
	if (offsets[col] + sizeof(uint64_t) >= blocks[col]->size) {
		FlushBlock(col);
	}

	char *bufptr = (char *)blocks[col]->buffer;
	*((uint64_t *)(bufptr + offsets[col])) = size;
	offsets[col] += sizeof(uint64_t);

	tuple_counts[col]++;
	// now write the actual string
	uint64_t pos = 0;
	while (pos < size) {
		uint64_t to_write = std::min(size - pos, blocks[col]->size - offsets[col]);
		memcpy(bufptr + offsets[col], val + pos, to_write);

		pos += to_write;
		offsets[col] += to_write;
		if (pos != size) {
			assert(offsets[col] == blocks[col]->size);
			// could not write entire string, flush the block
			FlushBlock(col);
		}
	}
}

void TableDataWriter::WriteDataPointers() {
	for (index_t i = 0; i < data_pointers.size(); i++) {
		// get a reference to the data column
		auto &data_pointer_list = data_pointers[i];
		manager.tabledata_writer->Write<uint64_t>(data_pointer_list.size());
		// then write the data pointers themselves
		for (index_t k = 0; k < data_pointer_list.size(); k++) {
			auto &data_pointer = data_pointer_list[k];
			manager.tabledata_writer->Write<double>(data_pointer.min);
			manager.tabledata_writer->Write<double>(data_pointer.max);
			manager.tabledata_writer->Write<uint64_t>(data_pointer.row_start);
			manager.tabledata_writer->Write<uint64_t>(data_pointer.tuple_count);
			manager.tabledata_writer->Write<block_id_t>(data_pointer.block_id);
			manager.tabledata_writer->Write<uint32_t>(data_pointer.offset);
		}
	}
}
