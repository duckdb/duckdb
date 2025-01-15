#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "fsst.h"

static constexpr uint16_t FSST_SYMBOL_TABLE_SIZE = sizeof(duckdb_fsst_decoder_t);

namespace duckdb {
namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
bool DictFSSTCompression::HasEnoughSpace(idx_t current_count, idx_t dict_count, idx_t dict_size,
                                         bitpacking_width_t packing_width, bitpacking_width_t string_lengths_width,
                                         const idx_t block_size) {
	return RequiredSpace(current_count, dict_count, dict_size, packing_width, string_lengths_width) <= block_size;
}

idx_t DictFSSTCompression::RequiredSpace(idx_t current_count, idx_t dict_count, idx_t dict_size,
                                         bitpacking_width_t packing_width, bitpacking_width_t string_lengths_width) {
	idx_t base_space = DICTIONARY_HEADER_SIZE + dict_size;
	idx_t string_number_space = BitpackingPrimitives::GetRequiredSize(current_count, packing_width);
	idx_t index_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);

	idx_t used_space = base_space + index_space + string_number_space;
	return used_space;
}

StringDictionaryContainer DictFSSTCompression::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_size));
	container.end = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_end));
	return container;
}

void DictFSSTCompression::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                        StringDictionaryContainer container) {
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	Store<uint32_t>(container.size, data_ptr_cast(&header_ptr->dict_size));
	Store<uint32_t>(container.end, data_ptr_cast(&header_ptr->dict_end));
}

DictFSSTCompressionState::DictFSSTCompressionState(const CompressionInfo &info) : CompressionState(info) {
}
DictFSSTCompressionState::~DictFSSTCompressionState() {
}

bool DictFSSTCompressionState::DryAppendToCurrentSegment(bool is_new, UnifiedVectorFormat &vdata, idx_t count,
                                                         idx_t index, idx_t raw_index) {
	auto strings = vdata.GetData<string_t>(vdata);
	auto string_data = GetString(strings, index, raw_index);
	auto &str = string_data.Get();
	auto required_space = RequiredSpace(is_new, str.GetSize());

	auto block_size = info.GetBlockSize();

	switch (append_state) {
	case DictionaryAppendState::REGULAR: {
		auto symbol_table_threshold = block_size - FSST_SYMBOL_TABLE_SIZE;
		if (required_space > symbol_table_threshold) {
			//! Decide whether or not to encode the dictionary
			if (EncodeDictionary()) {
				EncodeInputStrings(vdata, count);
				auto encoded_string = GetString(strings, index, raw_index);
				required_space = RequiredSpace(is_new, encoded_string.Get().GetSize());
				if (required_space > block_size) {
					//! Even after encoding the dictionary, we can't add this string
					return false;
				}
			} else {
				if (required_space > block_size) {
					return false;
				}
			}
		}
		return true;
	}
	case DictionaryAppendState::ENCODED: {
		return required_space <= (block_size - FSST_SYMBOL_TABLE_SIZE);
	}
	case DictionaryAppendState::NOT_ENCODED: {
		return required_space <= block_size;
	}
	case DictionaryAppendState::ENCODED_ALL_UNIQUE: {
		throw NotImplementedException("TODO");
	}
	};
	throw InternalException("Unhandled DictionaryAppendState");
}

bool DictFSSTCompressionState::UpdateState(Vector &scan_vector, idx_t count) {
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	Verify();

	if (IsEncoded()) {
		// The dictionary has been encoded
		// to look up a string in the dictionary, the input needs to be encoded as well
		EncodeInputStrings(vdata, count);
	}

	static constexpr idx_t STRING_SIZE_LIMIT = 16384;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		idx_t string_size = 0;
		optional_idx lookup_result;
		auto row_is_valid = vdata.validity.RowIsValid(idx);

		auto string_data = GetString(data, idx, i);
		if (row_is_valid) {
			auto &str = string_data.Get();
			string_size = str.GetSize();
			if (!string_data.encoded_string) {
				if (string_size >= STRING_SIZE_LIMIT / 2) {
					// This string could potentially expand by 2x when encoded by FSST
					return false;
				}
			} else {
				if (string_size >= STRING_SIZE_LIMIT) {
					throw FatalException("Encoded string expanded by more than 2x somehow!?");
				}
			}
			lookup_result = LookupString(str);
		}

		bool new_string = !lookup_result.IsValid();
		bool fits = DryAppendToCurrentSegment(new_string, vdata, count, idx, i);

		if (!fits) {
			Flush();
			lookup_result = optional_idx();
			fits = DryAppendToCurrentSegment(true, vdata, count, idx, i);
			if (!fits) {
				throw InternalException("Dictionary compression could not write to new segment");
			}
		}

		if (!row_is_valid) {
			AddNull();
		} else if (lookup_result.IsValid()) {
			AddLookup(UnsafeNumericCast<uint32_t>(lookup_result.GetIndex()));
		} else {
			auto string = GetString(data, idx, i);
			AddNewString(string);
		}

		Verify();
	}

	return true;
}

} // namespace dict_fsst
} // namespace duckdb
