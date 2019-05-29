#include "storage/checkpoint/table_data_reader.hpp"
#include "storage/meta_block_reader.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "common/types/null_value.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

TableDataReader::TableDataReader(CheckpointManager &manager) :
	manager(manager) {
}

void TableDataReader::ReadTableData(ClientContext &context, TableCatalogEntry &table, MetaBlockReader &reader) {
	assert(blocks.size() == 0);

	count_t column_count = table.columns.size();
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
					count_t tuples_left = data_pointers[col][indexes[col] - 1].tuple_count - tuple_counts[col];
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
					storage_vector.count = std::min((count_t)STANDARD_VECTOR_SIZE, tuples_left);
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

bool TableDataReader::ReadBlock(uint64_t col) {
	uint64_t block_nr = indexes[col];
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
	return true;
}

void TableDataReader::ReadString(Vector &vector, uint64_t col) {
	// first read the length of the string
	assert(offsets[col] + sizeof(uint64_t) < blocks[col]->size);

	char *bufptr = (char *)blocks[col]->buffer;
	uint64_t size = *((uint64_t *)(bufptr + offsets[col]));
	offsets[col] += sizeof(uint64_t);

	tuple_counts[col]++;
	// now read the actual string
	// first allocate space for the string
	auto data = unique_ptr<char[]>(new char[size + 1]);
	data[size] = '\0';
	char *val = data.get();
	// now read the string
	uint64_t pos = 0;
	while (pos < size) {
		uint64_t to_read = std::min(size - pos, blocks[col]->size - offsets[col]);
		memcpy(val + pos, bufptr + offsets[col], to_read);

		pos += to_read;
		offsets[col] += to_read;
		if (pos != size) {
			assert(offsets[col] == blocks[col]->size);
			if (!ReadBlock(col)) {
				throw IOException("Corrupt database file: could not read string fully");
			}
		}
	}
	if (strcmp(data.get(), NullValue<const char *>()) == 0) {
		vector.nullmask[vector.count] = true;
	} else {
		auto strings = (const char **)vector.data;
		strings[vector.count] = vector.string_heap.AddString(val, size);
	}
	vector.count++;
}

void TableDataReader::ReadDataPointers(uint64_t column_count, MetaBlockReader &reader) {
	data_pointers.resize(column_count);
	for (index_t col = 0; col < column_count; col++) {
		uint64_t data_pointer_count = reader.Read<uint64_t>();
		for (index_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
			DataPointer data_pointer;
			data_pointer.min = reader.Read<double>();
			data_pointer.max = reader.Read<double>();
			data_pointer.row_start = reader.Read<uint64_t>();
			data_pointer.tuple_count = reader.Read<uint64_t>();
			data_pointer.block_id = reader.Read<block_id_t>();
			data_pointer.offset = reader.Read<uint32_t>();
			data_pointers[col].push_back(data_pointer);
		}
	}
}
