#include "duckdb/storage/compression/dict_fsst/compression.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "fsst.h"

namespace duckdb {
namespace dict_fsst {

DictFSSTCompressionCompressState::DictFSSTCompressionCompressState(ColumnDataCheckpointData &checkpoint_data_p,
                                                                   const CompressionInfo &info)
    : DictFSSTCompressionState(info), checkpoint_data(checkpoint_data_p),
      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_DICT_FSST)),
      encoded_input(BufferAllocator::Get(checkpoint_data.GetDatabase())) {
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

optional_idx DictFSSTCompressionCompressState::LookupString(string_t str) {
	auto search = current_string_map.find(str);
	auto has_result = search != current_string_map.end();

	if (!has_result) {
		return optional_idx();
	}
	return search->second;
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
		string_t dictionary_string((const char *)dict_pos, UnsafeNumericCast<uint32_t>(str.GetSize())); // NOLINT
		D_ASSERT(!dictionary_string.IsInlined());
		current_string_map.insert({dictionary_string, index_buffer.size() - 1});
	}
	DictFSSTCompression::SetDictionary(*current_segment, current_handle, current_dictionary);

	current_width = next_width;
	current_segment->count++;
}

void DictFSSTCompressionCompressState::AddNull() {
	selection_buffer.push_back(0);
	current_segment->count++;
}

void DictFSSTCompressionCompressState::AddLookup(uint32_t lookup_result) {
	selection_buffer.push_back(lookup_result);
	current_segment->count++;
}

idx_t DictFSSTCompressionCompressState::RequiredSpace(bool new_string, idx_t string_size) {

	if (!new_string) {
		return DictFSSTCompression::RequiredSpace(current_segment->count.load() + 1, index_buffer.size(),
		                                          current_dictionary.size, current_width);
	}
	auto next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
	return DictFSSTCompression::RequiredSpace(current_segment->count.load() + 1, index_buffer.size() + 1,
	                                          current_dictionary.size + string_size, next_width);
}

void DictFSSTCompressionCompressState::Flush(bool final) {
	auto next_start = current_segment->start + current_segment->count;
	append_state = DictionaryAppendState::REGULAR;

	auto segment_size = Finalize();
	auto &state = checkpoint_data.GetCheckpointState();
	state.FlushSegment(std::move(current_segment), std::move(current_handle), segment_size);

	if (!final) {
		CreateEmptySegment(next_start);
	}
}

void DictFSSTCompressionCompressState::EncodeInputStrings(UnifiedVectorFormat &input, idx_t count) {
	D_ASSERT(append_state == DictionaryAppendState::ENCODED);

	encoded_input.Reset();
	D_ASSERT(encoder);
	auto fsst_encoder = (duckdb_fsst_encoder_t *)(encoder);

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;
	//! We could use the 'current_string_map' but this won't be in-order
	// and we want to preserve the order of the dictionary after rewriting
	auto data = input.GetData<string_t>(input);
	idx_t total_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = input.sel->get_index(i);
		auto &str = data[idx];
		fsst_string_sizes.push_back(str.GetSize());
		fsst_string_ptrs.push_back((unsigned char *)str.GetData()); // NOLINT
		total_size += str.GetSize();
	}

	size_t output_buffer_size = 7 + 2 * total_size; // size as specified in fsst.h
	auto compressed_ptrs = vector<unsigned char *>(count, nullptr);
	auto compressed_sizes = vector<size_t>(count, 0);
	auto compressed_buffer = make_unsafe_uniq_array_uninitialized<unsigned char>(output_buffer_size);

	auto res =
	    duckdb_fsst_compress(fsst_encoder, count, &fsst_string_sizes[0], &fsst_string_ptrs[0], output_buffer_size,
	                         compressed_buffer.get(), &compressed_sizes[0], &compressed_ptrs[0]);
	if (res != count) {
		throw FatalException("FSST compression failed to compress all input strings");
	}

	auto &strings = encoded_input.input_data;
	auto &heap = encoded_input.heap;
	for (idx_t i = 0; i < count; i++) {
		uint32_t size = UnsafeNumericCast<uint32_t>(compressed_sizes[i]);
		string_t encoded_string((const char *)compressed_ptrs[i], size); // NOLINT;
		strings.push_back(heap.AddString(std::move(encoded_string)));
	}
}

bool DictFSSTCompressionCompressState::EncodeDictionary() {
	if (current_dictionary.size < DICTIONARY_ENCODE_THRESHOLD) {
		append_state = DictionaryAppendState::NOT_ENCODED;
		return false;
	}
	append_state = DictionaryAppendState::ENCODED;

	// Encode the dictionary:
	// first prepare the input to create the fsst_encoder
	// create the encoder
	// allocate for enough space to encode the dictionary
	// encode the dictionary
	// write the (exported) symbol table to the end of the segment
	// then rewrite the dictionary + index_buffer + current_string_map

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;
	data_ptr_t last_start = current_end_ptr;
	//! We could use the 'current_string_map' but this won't be in-order
	// and we want to preserve the order of the dictionary after rewriting
	for (auto offset : index_buffer) {
		auto start = current_end_ptr - offset;
		size_t size = UnsafeNumericCast<size_t>(last_start - start);
		fsst_string_sizes.push_back(size);
		fsst_string_ptrs.push_back((unsigned char *)start); // NOLINT
		last_start = start;
	}

	auto string_count = index_buffer.size();
	encoder = duckdb_fsst_create(string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0], 0);
	auto fsst_encoder = (duckdb_fsst_encoder_t *)(encoder);

	size_t output_buffer_size = 7 + 2 * current_dictionary.size; // size as specified in fsst.h
	auto compressed_ptrs = vector<unsigned char *>(string_count, nullptr);
	auto compressed_sizes = vector<size_t>(string_count, 0);
	auto compressed_buffer = make_unsafe_uniq_array_uninitialized<unsigned char>(output_buffer_size);

	auto res =
	    duckdb_fsst_compress(fsst_encoder, string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0],
	                         output_buffer_size, compressed_buffer.get(), &compressed_sizes[0], &compressed_ptrs[0]);
	if (res != string_count) {
		throw FatalException("FSST compression failed to compress all dictionary strings");
	}

	// Write the exported symbol table to the end of the segment
	unsigned char fsst_serialized_symbol_table[sizeof(duckdb_fsst_decoder_t)];
	auto symbol_table_size = duckdb_fsst_export(fsst_encoder, fsst_serialized_symbol_table);
	current_end_ptr -= symbol_table_size;
	memcpy(current_end_ptr, (void *)fsst_serialized_symbol_table, symbol_table_size);

	// Rewrite the dictionary
	current_string_map.clear();
	uint32_t offset = 0;
	for (idx_t i = 0; i < string_count; i++) {
		auto &start = compressed_ptrs[i];
		auto &size = compressed_sizes[i];
		offset += size;
		index_buffer[i] = offset;
		auto dest = current_end_ptr - offset;
		memcpy(dest, start, size);
		string_t dictionary_string((const char *)start, UnsafeNumericCast<uint32_t>(size)); // NOLINT
		current_string_map.insert({dictionary_string, i});
	}
	current_dictionary.size = offset;
	current_dictionary.end -= symbol_table_size;
	DictFSSTCompression::SetDictionary(*current_segment, current_handle, current_dictionary);
	return true;
}

const string_t &DictFSSTCompressionCompressState::GetString(const string_t *strings, idx_t index, idx_t raw_index) {
	if (append_state != DictionaryAppendState::ENCODED) {
		return strings[index];
	}
	return encoded_input.input_data[raw_index];
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
