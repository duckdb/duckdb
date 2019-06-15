#include "storage/checkpoint/table_data_reader.hpp"
#include "storage/checkpoint/table_data_writer.hpp"
#include "storage/meta_block_reader.hpp"

#include "storage/meta_block_reader.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "common/types/null_value.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

TableDataReader::TableDataReader(CheckpointManager &manager, TableCatalogEntry &table, MetaBlockReader &reader)
    : manager(manager), table(table), reader(reader) {
}

void TableDataReader::ReadTableData(ClientContext &context) {
	assert(blocks.size() == 0);

	index_t column_count = table.columns.size();
	assert(column_count > 0);

	// load the data pointers for the table
	ReadDataPointers(column_count);
	if (data_pointers[0].size() == 0) {
		// if the table is empty there is no loading to be done
		return;
	}

	// initialize the blocks for each column with the first block
	blocks.resize(column_count);
	offsets.resize(column_count);
	tuple_counts.resize(column_count);
	indexes.resize(column_count);
	dictionary_start.resize(column_count);
	for (index_t col = 0; col < column_count; col++) {
		blocks[col] = make_unique<Block>(INVALID_BLOCK);
		indexes[col] = 0;
		ReadBlock(col);
	}

	// construct a DataChunk to hold the intermediate objects we will push into the database
	vector<TypeId> types;
	for (auto &col : table.columns) {
		types.push_back(GetInternalType(col.type));
	}
	DataChunk insert_chunk;
	insert_chunk.Initialize(types);

	// now load the actual data
	while (true) {
		// construct a DataChunk by loading tuples from each of the columns
		insert_chunk.Reset();
		for (index_t col = 0; col < column_count; col++) {
			TypeId type = insert_chunk.data[col].type;
			if (TypeIsConstantSize(type)) {
				while (insert_chunk.data[col].count < STANDARD_VECTOR_SIZE) {
					index_t tuples_left = data_pointers[col][indexes[col] - 1].tuple_count - tuple_counts[col];
					if (tuples_left == 0) {
						// no tuples left in this block
						// move to next block
						if (!ReadBlock(col)) {
							// no next block
							break;
						}
						tuples_left = data_pointers[col][indexes[col] - 1].tuple_count - tuple_counts[col];
					}
					Vector storage_vector(types[col], blocks[col]->buffer + offsets[col]);
					storage_vector.count = std::min((index_t)STANDARD_VECTOR_SIZE, tuples_left);
					VectorOperations::AppendFromStorage(storage_vector, insert_chunk.data[col]);

					tuple_counts[col] += storage_vector.count;
					offsets[col] += storage_vector.count * GetTypeIdSize(types[col]);
				}
			} else {
				assert(type == TypeId::VARCHAR);
				while (insert_chunk.data[col].count < STANDARD_VECTOR_SIZE) {
					if (tuple_counts[col] == data_pointers[col][indexes[col] - 1].tuple_count) {
						if (!ReadBlock(col)) {
							// no more tuples left to read
							break;
						}
					}
					ReadString(insert_chunk.data[col], col);
				}
			}
		}
		if (insert_chunk.size() == 0) {
			break;
		}

		table.storage->Append(table, context, insert_chunk);
	}
}

bool TableDataReader::ReadBlock(index_t col) {
	index_t block_nr = indexes[col];
	if (block_nr >= data_pointers[col].size()) {
		// ran out of blocks
		return false;
	}
	blocks[col]->id = data_pointers[col][block_nr].block_id;
	offsets[col] = data_pointers[col][block_nr].offset;
	tuple_counts[col] = 0;
	indexes[col]++;
	// read the data for the block from disk
	manager.block_manager.Read(*blocks[col]);
	if (table.columns[col].type == SQLTypeId::VARCHAR) {
		// read the dictionary offset for string columns
		int32_t dictionary_offset = *((int32_t *)(blocks[col]->buffer + offsets[col]));
		dictionary_start[col] = blocks[col]->buffer + offsets[col] + dictionary_offset;
		offsets[col] += TableDataWriter::BLOCK_HEADER_STRING;
	}
	return true;
}

void TableDataReader::ReadString(Vector &vector, index_t col) {
	// read the offset of the string into the
	int32_t offset = *((int32_t *)(blocks[col]->buffer + offsets[col]));
	offsets[col] += sizeof(int32_t);
	// now fetch the string from the dictionary
	tuple_counts[col]++;
	auto str_value = (char *)(dictionary_start[col] + offset);
	// place the string into the vector
	if (strcmp(str_value, TableDataWriter::BIG_STRING_MARKER) == 0) {
		// big string, read the block id and offset from where to get the actual string
		auto block_id = *((block_id_t *)(str_value + 2 * sizeof(char)));
		// now read the actual string
		MetaBlockReader reader(manager.block_manager, block_id);
		string big_string = reader.Read<string>();
		// add the string to the vector
		auto strings = (const char **)vector.data;
		strings[vector.count] = vector.string_heap.AddString(big_string);
	} else if (strcmp(str_value, NullValue<const char *>()) == 0) {
		vector.nullmask[vector.count] = true;
	} else {
		auto strings = (const char **)vector.data;
		strings[vector.count] = vector.string_heap.AddString(str_value);
	}
	vector.count++;
}

void TableDataReader::ReadDataPointers(index_t column_count) {
	data_pointers.resize(column_count);
	for (index_t col = 0; col < column_count; col++) {
		index_t data_pointer_count = reader.Read<index_t>();
		for (index_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
			DataPointer data_pointer;
			data_pointer.min = reader.Read<double>();
			data_pointer.max = reader.Read<double>();
			data_pointer.row_start = reader.Read<index_t>();
			data_pointer.tuple_count = reader.Read<index_t>();
			data_pointer.block_id = reader.Read<block_id_t>();
			data_pointer.offset = reader.Read<uint32_t>();
			data_pointers[col].push_back(data_pointer);
		}
	}
}
