#include "duckdb/storage/compression/dict_fsst/compression.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "fsst.h"

namespace duckdb {
namespace dict_fsst {

DictFSSTCompressionCompressState::DictFSSTCompressionCompressState(ColumnDataCheckpointData &checkpoint_data_p,
                                                                   const CompressionInfo &info)
    : DictFSSTCompressionState(info), checkpoint_data(checkpoint_data_p),
      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_DICT_FSST)),
      heap(BufferAllocator::Get(checkpoint_data.GetDatabase())) {
	CreateEmptySegment(checkpoint_data.GetRowGroup().start);
}

void DictFSSTCompressionCompressState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpoint_data.GetDatabase();
	auto &type = checkpoint_data.GetType();

	auto compressed_segment =
	    ColumnSegment::CreateTransientSegment(db, function, type, row_start, info.GetBlockSize(), info.GetBlockSize());
	current_segment = std::move(compressed_segment);

	// Reset the buffers and the string map.
	current_string_map.clear();
	index_buffer.clear();

	// Reserve index 0 for null strings.
	index_buffer.push_back(0);
	selection_buffer.clear();

	current_width = 0;
	next_width = 0;

	// Reset the pointers into the current segment.
	auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
	current_handle = buffer_manager.Pin(current_segment->block);
	current_dictionary = DictFSSTCompression::GetDictionary(*current_segment, current_handle);
	current_end_ptr = current_handle.Ptr() + current_dictionary.end;
}

void DictFSSTCompressionCompressState::Verify() {
	current_dictionary.Verify(info.GetBlockSize());
	D_ASSERT(current_segment->count == selection_buffer.size());
	D_ASSERT(DictFSSTCompression::HasEnoughSpace(current_segment->count.load(), index_buffer.size(),
	                                             current_dictionary.size, current_width, info.GetBlockSize()));
	D_ASSERT(current_dictionary.end == info.GetBlockSize());
	D_ASSERT(index_buffer.size() == current_string_map.size() + 1); // +1 is for null value
}

bool DictFSSTCompressionCompressState::LookupString(string_t str) {
	auto search = current_string_map.find(str);
	auto has_result = search != current_string_map.end();

	if (has_result) {
		latest_lookup_result = search->second;
	}
	return has_result;
}

void DictFSSTCompressionCompressState::AddNewString(string_t str) {
	UncompressedStringStorage::UpdateStringStats(current_segment->stats, str);

	// Copy string to dict
	// New entries are added to the start (growing backwards)
	// [............xxxxooooooooo]
	// x: new string
	// o: existing string
	// .: (currently) unused space
	current_dictionary.size += str.GetSize();
	auto dict_pos = current_end_ptr - current_dictionary.size;
	memcpy(dict_pos, str.GetData(), str.GetSize());
	current_dictionary.Verify(info.GetBlockSize());
	D_ASSERT(current_dictionary.end == info.GetBlockSize());

	// Update buffers and map
	index_buffer.push_back(current_dictionary.size);
	selection_buffer.push_back(UnsafeNumericCast<uint32_t>(index_buffer.size() - 1));
	if (str.IsInlined()) {
		current_string_map.insert({str, index_buffer.size() - 1});
	} else {
		current_string_map.insert({heap.AddBlob(str), index_buffer.size() - 1});
	}
	DictFSSTCompression::SetDictionary(*current_segment, current_handle, current_dictionary);

	current_width = next_width;
	current_segment->count++;
}

void DictFSSTCompressionCompressState::AddNull() {
	selection_buffer.push_back(0);
	current_segment->count++;
}

void DictFSSTCompressionCompressState::AddLastLookup() {
	selection_buffer.push_back(latest_lookup_result);
	current_segment->count++;
}

bool DictFSSTCompressionCompressState::HasRoomForString(bool new_string, idx_t string_size) {
	static constexpr uint16_t FSST_SYMBOL_TABLE_SIZE = sizeof(duckdb_fsst_decoder_t);

	auto block_size = info.GetBlockSize();

	if (!new_string) {
		return DictFSSTCompression::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size(),
		                                           current_dictionary.size, current_width, block_size);
	}
	next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
	return DictFSSTCompression::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size() + 1,
	                                           current_dictionary.size + string_size, next_width, block_size);
}

void DictFSSTCompressionCompressState::Flush(bool final) {
	auto next_start = current_segment->start + current_segment->count;

	auto segment_size = Finalize();
	auto &state = checkpoint_data.GetCheckpointState();
	state.FlushSegment(std::move(current_segment), std::move(current_handle), segment_size);

	if (!final) {
		CreateEmptySegment(next_start);
	}
}

idx_t DictFSSTCompressionCompressState::Finalize() {
	auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
	auto handle = buffer_manager.Pin(current_segment->block);
	D_ASSERT(current_dictionary.end == info.GetBlockSize());

	// calculate sizes
	auto compressed_selection_buffer_size =
	    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
	auto index_buffer_size = index_buffer.size() * sizeof(uint32_t);
	auto total_size = DictFSSTCompression::DICTIONARY_HEADER_SIZE + compressed_selection_buffer_size +
	                  index_buffer_size + current_dictionary.size;

	// calculate ptr and offsets
	auto base_ptr = handle.Ptr();
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(base_ptr);
	auto compressed_selection_buffer_offset = DictFSSTCompression::DICTIONARY_HEADER_SIZE;
	auto index_buffer_offset = compressed_selection_buffer_offset + compressed_selection_buffer_size;

	// Write compressed selection buffer
	BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_selection_buffer_offset,
	                                               (sel_t *)(selection_buffer.data()), current_segment->count,
	                                               current_width);

	// Write the index buffer
	memcpy(base_ptr + index_buffer_offset, index_buffer.data(), index_buffer_size);

	// Store sizes and offsets in segment header
	Store<uint32_t>(NumericCast<uint32_t>(index_buffer_offset), data_ptr_cast(&header_ptr->index_buffer_offset));
	Store<uint32_t>(NumericCast<uint32_t>(index_buffer.size()), data_ptr_cast(&header_ptr->index_buffer_count));
	Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));

	D_ASSERT(current_width == BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1));
	D_ASSERT(DictFSSTCompression::HasEnoughSpace(current_segment->count, index_buffer.size(), current_dictionary.size,
	                                             current_width, info.GetBlockSize()));
	D_ASSERT((uint64_t)*max_element(std::begin(selection_buffer), std::end(selection_buffer)) ==
	         index_buffer.size() - 1);

	// Early-out, if the block is sufficiently full.
	if (total_size >= info.GetCompactionFlushLimit()) {
		return info.GetBlockSize();
	}

	// Sufficient space: calculate how much space we can save.
	auto move_amount = info.GetBlockSize() - total_size;

	// Move the dictionary to align it with the offsets.
	auto new_dictionary_offset = index_buffer_offset + index_buffer_size;
	memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
	        current_dictionary.size);
	current_dictionary.end -= move_amount;
	D_ASSERT(current_dictionary.end == total_size);

	// Write the new dictionary with the updated "end".
	DictFSSTCompression::SetDictionary(*current_segment, handle, current_dictionary);
	return total_size;
}

} // namespace dict_fsst
} // namespace duckdb
