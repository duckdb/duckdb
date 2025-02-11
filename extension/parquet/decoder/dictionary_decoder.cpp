#include "decoder/dictionary_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

namespace duckdb {

DictionaryDecoder::DictionaryDecoder(ColumnReader &reader)
    : reader(reader), offset_buffer(reader.encoding_buffers[0]), valid_sel(STANDARD_VECTOR_SIZE),
      dictionary_selection_vector(STANDARD_VECTOR_SIZE), dictionary_size(0) {
}

void DictionaryDecoder::InitializeDictionary(idx_t new_dictionary_size, optional_ptr<const TableFilter> filter,
                                             bool has_defines) {
	auto old_dict_size = dictionary_size;
	dictionary_size = new_dictionary_size;
	filter_result.reset();
	filter_count = 0;
	can_have_nulls = has_defines;
	// we use the first value in the dictionary to keep a NULL
	if (!dictionary) {
		dictionary = make_uniq<Vector>(reader.Type(), dictionary_size + 1);
	} else if (dictionary_size > old_dict_size) {
		dictionary->Resize(old_dict_size, dictionary_size + 1);
	}
	dictionary_id =
	    reader.reader.file_name + "_" + reader.Schema().name + "_" + std::to_string(reader.chunk_read_offset);
	// we use the last entry as a NULL, dictionary vectors don't have a separate validity mask
	auto &dict_validity = FlatVector::Validity(*dictionary);
	dict_validity.Reset(dictionary_size + 1);
	if (can_have_nulls) {
		dict_validity.SetInvalid(dictionary_size);
	}
	reader.Plain(reader.block, nullptr, dictionary_size, 0, *dictionary);

	if (filter && CanFilter(*filter)) {
		// no filter result yet - apply filter to the dictionary
		// initialize the filter result - setting everything to false
		filter_result = make_unsafe_uniq_array<bool>(dictionary_size);

		// apply the filter
		UnifiedVectorFormat vdata;
		dictionary->ToUnifiedFormat(dictionary_size, vdata);
		SelectionVector dict_sel;
		filter_count = dictionary_size;
		ColumnSegment::FilterSelection(dict_sel, *dictionary, vdata, *filter, dictionary_size, filter_count);

		// now set all matching tuples to true
		for (idx_t i = 0; i < filter_count; i++) {
			auto idx = dict_sel.get_index(i);
			filter_result[idx] = true;
		}
	}
}

void DictionaryDecoder::InitializePage() {
	// where is it otherwise??
	auto &block = reader.block;
	auto dict_width = block->read<uint8_t>();
	dict_decoder = make_uniq<RleBpDecoder>(block->ptr, block->len, dict_width);
	block->inc(block->len);
}

void DictionaryDecoder::ConvertDictToSelVec(uint32_t *offsets, const SelectionVector &rows, idx_t count) {
	D_ASSERT(count <= STANDARD_VECTOR_SIZE);
	for (idx_t idx = 0; idx < count; idx++) {
		auto row_idx = rows.get_index(idx);
		auto offset = offsets[idx];
		if (offset >= dictionary_size) {
			throw std::runtime_error("Parquet file is likely corrupted, dictionary offset out of range");
		}
		dictionary_selection_vector.set_index(row_idx, offset);
	}
}

idx_t DictionaryDecoder::GetValidValues(uint8_t *defines, idx_t read_count, idx_t result_offset) {
	idx_t valid_count = read_count;
	if (defines) {
		D_ASSERT(can_have_nulls);
		valid_count = 0;
		for (idx_t i = 0; i < read_count; i++) {
			valid_sel.set_index(valid_count, i);
			dictionary_selection_vector.set_index(i, dictionary_size);
			valid_count += defines[result_offset + i] == reader.MaxDefine();
		}
	}
	return valid_count;
}

idx_t DictionaryDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	if (!dictionary || dictionary_size < 0) {
		throw std::runtime_error("Parquet file is likely corrupted, missing dictionary");
	}
	idx_t valid_count = GetValidValues(defines, read_count, result_offset);
	if (valid_count == read_count) {
		// all values are valid - we can directly decompress the offsets into the selection vector
		dict_decoder->GetBatch<uint32_t>(data_ptr_cast(dictionary_selection_vector.data()), valid_count);
		// we do still need to verify the offsets though
		for (idx_t idx = 0; idx < valid_count; idx++) {
			if (dictionary_selection_vector.get_index(idx) >= dictionary_size) {
				throw std::runtime_error("Parquet file is likely corrupted, dictionary offset out of range");
			}
		}
	} else if (valid_count > 0) {
		// for the valid entries - decode the offsets
		offset_buffer.resize(reader.reader.allocator, sizeof(uint32_t) * valid_count);
		dict_decoder->GetBatch<uint32_t>(offset_buffer.ptr, valid_count);
		ConvertDictToSelVec(reinterpret_cast<uint32_t *>(offset_buffer.ptr), valid_sel, valid_count);
	}
#ifdef DEBUG
	dictionary_selection_vector.Verify(read_count, dictionary_size + can_have_nulls);
#endif
	if (result_offset == 0) {
		result.Dictionary(*dictionary, dictionary_size + can_have_nulls, dictionary_selection_vector, read_count);
		DictionaryVector::SetDictionaryId(result, dictionary_id);
		D_ASSERT(result.GetVectorType() == VectorType::DICTIONARY_VECTOR);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(*dictionary, result, dictionary_selection_vector, read_count, 0, result_offset);
	}
	return valid_count;
}

void DictionaryDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	if (!dictionary || dictionary_size < 0) {
		throw std::runtime_error("Parquet file is likely corrupted, missing dictionary");
	}
	idx_t valid_count = reader.GetValidCount(defines, skip_count);
	// skip past the valid offsets
	dict_decoder->Skip(valid_count);
}

bool DictionarySupportsFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = filter.Cast<ConjunctionOrFilter>();
		for (auto &child_filter : conjunction.child_filters) {
			if (!DictionarySupportsFilter(*child_filter)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : conjunction.child_filters) {
			if (!DictionarySupportsFilter(*child_filter)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NOT_NULL:
		return true;
	case TableFilterType::IS_NULL:
	case TableFilterType::DYNAMIC_FILTER:
	case TableFilterType::OPTIONAL_FILTER:
	case TableFilterType::STRUCT_EXTRACT:
	default:
		return false;
	}
}

bool DictionaryDecoder::CanFilter(const TableFilter &filter) {
	if (dictionary_size == 0) {
		return false;
	}
	// We can only push the filter if the filter removes NULL values
	if (!DictionarySupportsFilter(filter)) {
		return false;
	}
	return true;
}

void DictionaryDecoder::Filter(uint8_t *defines, idx_t read_count, Vector &result, const TableFilter &filter,
                               SelectionVector &sel, idx_t &approved_tuple_count) {
	if (!dictionary || dictionary_size < 0) {
		throw std::runtime_error("Parquet file is likely corrupted, missing dictionary");
	}
	D_ASSERT(filter_count > 0);
	// read the dictionary values
	auto valid_count = Read(defines, read_count, result, 0);

	// apply the filter by checking the dictionary offsets directly
	uint32_t *offsets;
	if (valid_count == read_count) {
		offsets = dictionary_selection_vector.data();
	} else {
		offsets = reinterpret_cast<uint32_t *>(offset_buffer.ptr);
	}
	D_ASSERT(offsets);
	SelectionVector new_sel(valid_count);
	approved_tuple_count = 0;
	for (idx_t idx = 0; idx < valid_count; idx++) {
		auto row_idx = valid_sel.get_index(idx);
		auto offset = offsets[idx];
		if (!filter_result[offset]) {
			// does not pass the filter
			continue;
		}
		new_sel.set_index(approved_tuple_count++, row_idx);
	}
	if (approved_tuple_count < read_count) {
		sel.Initialize(new_sel);
	}
}

} // namespace duckdb
