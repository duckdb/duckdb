#include "storage/checkpoint/table_data_writer.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "common/types/null_value.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/serializer/buffered_serializer.hpp"

using namespace duckdb;
using namespace std;

constexpr char TableDataWriter::BIG_STRING_MARKER[];

static index_t GetTypeHeaderSize(SQLType type);

TableDataWriter::TableDataWriter(CheckpointManager &manager, TableCatalogEntry &table)
    : manager(manager), table(table) {
}

void TableDataWriter::WriteTableData(Transaction &transaction) {
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
	dictionaries.resize(table.columns.size());
	data_pointers.resize(table.columns.size());
	// we want to fetch all the column ids from the scan
	// so make a list of all column ids of the table
	for (index_t i = 0; i < table.columns.size(); i++) {
		// for each column, create a block that serves as the buffer for that blocks data
		blocks.push_back(make_unique<Block>(INVALID_BLOCK));
		// initialize offsets, tuple counts and row number sizes
		offsets.push_back(GetTypeHeaderSize(table.columns[i].type));
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
void TableDataWriter::WriteColumnData(DataChunk &chunk, index_t column_index) {
	TypeId type = chunk.data[column_index].type;
	if (TypeIsConstantSize(type)) {
		// constant size type: simple memcpy
		// first check if we can fit the chunk into the block
		index_t size = GetTypeIdSize(type) * chunk.size();
		// check if we need to flush
		// FIXME: append part of data that still fits into block if it does not fit entirely
		FlushIfFull(column_index, size);
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

void TableDataWriter::FlushBlock(index_t col) {
	if (tuple_counts[col] == 0) {
		return;
	}
	assert(offsets[col] + dictionaries[col].size < blocks[col]->size);
	// get a block id
	blocks[col]->id = manager.block_manager.GetFreeBlockId();
	if (table.columns[col].type.id == SQLTypeId::VARCHAR) {
		// for varchar columns, write the dictionary to the buffer
		FlushDictionary(col);
	}
	// construct the data pointer, FIXME: add statistics as well
	DataPointer data_pointer;
	data_pointer.block_id = blocks[col]->id;
	data_pointer.offset = 0;
	data_pointer.row_start = row_numbers[col];
	data_pointer.tuple_count = tuple_counts[col];
	data_pointers[col].push_back(data_pointer);
	// write the block
	manager.block_manager.Write(*blocks[col]);

	offsets[col] = GetTypeHeaderSize(table.columns[col].type);
	row_numbers[col] += tuple_counts[col];
	tuple_counts[col] = 0;
}

void TableDataWriter::FlushIfFull(index_t col, index_t write_size) {
	if (offsets[col] + dictionaries[col].size + write_size >= blocks[col]->size) {
		// data does not fit into block, flush block to disk
		FlushBlock(col);
	}
}

void TableDataWriter::FlushDictionary(index_t col) {
	assert(table.columns[col].type.id == SQLTypeId::VARCHAR);
	assert(dictionaries[col].size > 0);
	// write the dictionary size offset to the start of the block
	*((int32_t *)blocks[col]->buffer) = offsets[col];
	// now write the strings to the block
	for (auto &entry : dictionaries[col].offsets) {
		memcpy(blocks[col]->buffer + offsets[col] + entry.second, entry.first.c_str(), entry.first.size() + 1);
	}
	// reset the dictionary
	dictionaries[col].offsets.clear();
	dictionaries[col].size = 0;
}

static index_t GetTypeHeaderSize(SQLType type) {
	if (type.id == SQLTypeId::VARCHAR) {
		return TableDataWriter::BLOCK_HEADER_STRING;
	} else {
		return TableDataWriter::BLOCK_HEADER_NUMERIC;
	}
}

static string BigStringMarker(block_id_t id) {
	BufferedSerializer serializer(TableDataWriter::BIG_STRING_MARKER_SIZE);
	serializer.Write<char>(TableDataWriter::BIG_STRING_MARKER[0]);
	serializer.Write<char>(TableDataWriter::BIG_STRING_MARKER[1]);
	serializer.Write<block_id_t>(id);
	auto data = serializer.GetData();
	assert(data.size == TableDataWriter::BIG_STRING_MARKER_SIZE);
	return string((char *)data.data.get(), data.size);
}

void TableDataWriter::WriteString(index_t col, const char *val) {
	FlushIfFull(col, sizeof(int32_t));
	// create the string value
	string str_value(val);
	if (str_value.size() + 1 > blocks[col]->size - BLOCK_HEADER_STRING - sizeof(int32_t)) {
		// string can never fit in a single block, insert a special marker followed by the block id and offset where it
		// is stored create the special marker indicating it is a big string
		MetaBlockWriter writer(manager.block_manager);
		string marker = BigStringMarker(writer.block->id);
		// write the string to the overflow blocks
		writer.WriteString(str_value);
		// now write the marker in the dictionary
		str_value = marker;
	}
	// add the string to the dictionary
	auto entry = dictionaries[col].offsets.find(str_value);
	int32_t offset;
	if (entry == dictionaries[col].offsets.end()) {
		// not in the dictionary yet, add it to the dictionary
		// first check if we have room for the string in our dictionary
		FlushIfFull(col, sizeof(int32_t) + str_value.size() + 1);
		// now add the string to the dictionary
		offset = dictionaries[col].size;
		dictionaries[col].offsets[str_value] = offset;
		dictionaries[col].size += str_value.size() + 1;
	} else {
		// in the dictionary, only need to write the offset
		// check if we have room to write the offset
		offset = entry->second;
	}
	// now write the offset of this string into the buffer
	*((int32_t *)(blocks[col]->buffer + offsets[col])) = offset;
	offsets[col] += sizeof(int32_t);
	tuple_counts[col]++;
}

void TableDataWriter::WriteDataPointers() {
	for (index_t i = 0; i < data_pointers.size(); i++) {
		// get a reference to the data column
		auto &data_pointer_list = data_pointers[i];
		manager.tabledata_writer->Write<index_t>(data_pointer_list.size());
		// then write the data pointers themselves
		for (index_t k = 0; k < data_pointer_list.size(); k++) {
			auto &data_pointer = data_pointer_list[k];
			manager.tabledata_writer->Write<double>(data_pointer.min);
			manager.tabledata_writer->Write<double>(data_pointer.max);
			manager.tabledata_writer->Write<index_t>(data_pointer.row_start);
			manager.tabledata_writer->Write<index_t>(data_pointer.tuple_count);
			manager.tabledata_writer->Write<block_id_t>(data_pointer.block_id);
			manager.tabledata_writer->Write<uint32_t>(data_pointer.offset);
		}
	}
}
