#include "decoder/dictionary_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

DictionaryDecoder::DictionaryDecoder(ColumnReader &reader)
    : reader(reader), offset_buffer(reader.encoding_buffers[0]), valid_sel(STANDARD_VECTOR_SIZE),
      dictionary_selection_vector(STANDARD_VECTOR_SIZE), dictionary_size(0) {
}

void DictionaryDecoder::InitializeDictionary(idx_t new_dictionary_size) {
	auto old_dict_size = dictionary_size;
	dictionary_size = new_dictionary_size;
	// we use the first value in the dictionary to keep a NULL
	if (!dictionary) {
		dictionary = make_uniq<Vector>(reader.type, dictionary_size + 1);
	} else if (dictionary_size > old_dict_size) {
		dictionary->Resize(old_dict_size, dictionary_size + 1);
	}
	dictionary_id = reader.reader.file_name + "_" + reader.schema.name + "_" + std::to_string(reader.chunk_read_offset);
	// we use the first entry as a NULL, dictionary vectors don't have a separate validity mask
	FlatVector::Validity(*dictionary).SetInvalid(0);
	reader.Plain(reader.block, nullptr, dictionary_size, 1, *dictionary);
}

void DictionaryDecoder::InitializePage() {
	// where is it otherwise??
	auto &block = reader.block;
	auto dict_width = block->read<uint8_t>();
	// TODO somehow dict_width can be 0 ?
	dict_decoder = make_uniq<RleBpDecoder>(block->ptr, block->len, dict_width);
	block->inc(block->len);
}

void DictionaryDecoder::ConvertDictToSelVec(uint32_t *offsets, const SelectionVector &rows, idx_t count,
                                            idx_t result_offset) {
	D_ASSERT(count <= STANDARD_VECTOR_SIZE);
	for (idx_t idx = 0; idx < count; idx++) {
		auto row_idx = rows.get_index(idx);
		auto offset = offsets[idx];
		if (offset >= dictionary_size) {
			throw std::runtime_error("Parquet file is likely corrupted, dictionary offset out of range");
		}
		dictionary_selection_vector.set_index(row_idx, offset + 1);
	}
}

void DictionaryDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	if (!dictionary || dictionary_size < 0) {
		throw std::runtime_error("Parquet file is likely corrupted, missing dictionary");
	}
	idx_t valid_count = read_count;
	auto sel = FlatVector::IncrementalSelectionVector();
	if (defines) {
		valid_count = 0;
		for (idx_t i = 0; i < read_count; i++) {
			valid_sel.set_index(valid_count, i);
			dictionary_selection_vector.set_index(i, 0);
			valid_count += defines[result_offset + i] == reader.max_define;
		}
		if (valid_count < read_count) {
			sel = &valid_sel;
		}
	}
	if (valid_count > 0) {
		// for the valid entries - decode the offsets
		offset_buffer.resize(reader.reader.allocator, sizeof(uint32_t) * valid_count);
		dict_decoder->GetBatch<uint32_t>(offset_buffer.ptr, valid_count);
		ConvertDictToSelVec(reinterpret_cast<uint32_t *>(offset_buffer.ptr), *sel, valid_count, result_offset);
	}
#ifdef DEBUG
	dictionary_selection_vector.Verify(read_count, dictionary_size + 1);
#endif
	if (result_offset == 0) {
		result.Dictionary(*dictionary, dictionary_size + 1, dictionary_selection_vector, read_count);
		DictionaryVector::SetDictionaryId(result, dictionary_id);
		D_ASSERT(result.GetVectorType() == VectorType::DICTIONARY_VECTOR);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(*dictionary, result, dictionary_selection_vector, read_count, 0, result_offset);
	}
}

void DictionaryDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	if (!dictionary || dictionary_size < 0) {
		throw std::runtime_error("Parquet file is likely corrupted, missing dictionary");
	}
	idx_t valid_count = reader.GetValidCount(defines, skip_count);
	// skip past the valid offsets
	dict_decoder->Skip(valid_count);
}

} // namespace duckdb
