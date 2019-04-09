#include "storage/directory_block_manager.hpp"
#include "storage/meta_block_writer.hpp"

using namespace std;
using namespace duckdb;

unique_ptr<Block> DirectoryBlockManager::GetBlock(block_id_t id) {
	// load an existing block
	// the file should exist, if it doesn't we throw an exception
	auto block_path = GetBlockPath(id);
	return unique_ptr<DirectoryBlock>(new DirectoryBlock(id, block_path));
}

unique_ptr<Block> DirectoryBlockManager::CreateBlock() {
	while (FileExists(GetBlockPath(free_id))) {
		free_id++;
	}
	auto block = unique_ptr<DirectoryBlock>(new DirectoryBlock(free_id, GetBlockPath(free_id)));
	free_id++;
	return move(block);
}

void DirectoryBlockManager::Flush(unique_ptr<Block> &block) {
	assert(block->offset > 0);
	// the amount of data to be flushed
	auto buffered_size = block->offset;
	// delegates the writing to disk to block
	block->Write(data_buffer.get(), buffered_size);
	// calls deleter for the old buffer and reset it
	data_buffer.reset(new char[BLOCK_SIZE]);
}

void DirectoryBlockManager::AppendDataToBlock(DataChunk &chunk, unique_ptr<Block> &block,
                                              MetaBlockWriter &meta_writer) {
	char *dataptr = data_buffer.get();
	for (size_t col_idx = 0; col_idx < chunk.column_count; col_idx++) {
		auto &vec = chunk.data[col_idx];
		auto type = chunk.data[col_idx].type;
		// Write the beggining(offset) of the column
		meta_writer.Write<uint32_t>(block->offset);
		if (TypeIsConstantSize(type)) {
			// constant size type: simple memcpy
			auto data_length = (GetTypeIdSize(type) * vec.count);
			VectorOperations::CopyToStorage(vec, dataptr + block->offset);
			block->offset += data_length;
		} else {
			assert(type == TypeId::VARCHAR);
			// strings are inlined into the block
			// we use null-padding to store them
			const char **strings = (const char **)vec.data;
			VectorOperations::Exec(vec, [&](size_t i, size_t k) {
				auto source = vec.nullmask[i] ? strings[k] : NullValue<const char *>();
				size_t str_length = strlen(source);
				memcpy(dataptr + block->offset, source, str_length);
				block->offset += str_length;
			});
		}
		// we need to store the offset for each column data inside the header
		meta_writer.Write<uint32_t>(block->offset);
		meta_writer.Write<size_t>(chunk.size());
	}
	block->tuple_count += chunk.size();
}

void DirectoryBlockManager::LoadTableData(DataTable &table, DataChunk &chunk, unique_ptr<Block> meta_block) {
	MetaBlockReader reader(move(meta_block));
	auto block_id = reader.Read<block_id_t>();
	BlockEntry entry;
	entry.block = GetBlock(block_id);
	entry.row_offset = reader.Read<uint64_t>();
	table.data_blocks.push_back(move(entry));
	auto &block = *entry.block;
	for (size_t col_idx = 0; col_idx < chunk.column_count; col_idx++) {
		auto column_offset_begin = reader.Read<uint32_t>();
		auto column_offset_end = reader.Read<uint32_t>();
		auto rows = reader.Read<size_t>();

		auto data_length = column_offset_end - column_offset_begin;
		auto type = chunk.data[col_idx].type;
		char buffer_data[BLOCK_SIZE];
		if (TypeIsConstantSize(type)) {
			// constant size type: simple memcpy
			block.Read(buffer_data, column_offset_begin, data_length);
			Vector v(chunk.data[col_idx].type, (char *)buffer_data);
			v.count = rows;
			VectorOperations::AppendFromStorage(v, chunk.data[col_idx]);
		} else {
			const char **strings = (const char **)chunk.data[col_idx].data;
			for (size_t j = 0; j < rows; j++) {
				// read the strings
				auto str = block.ReadString(column_offset_begin);
				// now add the string to the StringHeap of the vector
				// and write the pointer into the vector
				if (IsNullValue<const char *>((const char *)str.c_str())) {
					strings[j] = nullptr;
					chunk.data[col_idx].nullmask[j] = true;
				} else {
					strings[j] = chunk.data[col_idx].string_heap.AddString(str);
				}
			}
		}
		chunk.data[col_idx].count = rows;
	}
}