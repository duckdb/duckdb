#include "duckdb/storage/compression/dict_fsst/common.hpp"

namespace duckdb {
namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
bool DictFSSTCompression::HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                         bitpacking_width_t packing_width, const idx_t block_size) {
	return RequiredSpace(current_count, index_count, dict_size, packing_width) <= block_size;
}

idx_t DictFSSTCompression::RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                         bitpacking_width_t packing_width) {
	idx_t base_space = DICTIONARY_HEADER_SIZE + dict_size;
	idx_t string_number_space = BitpackingPrimitives::GetRequiredSize(current_count, packing_width);
	idx_t index_space = index_count * sizeof(uint32_t);

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

bool DictFSSTCompressionState::UpdateState(Vector &scan_vector, idx_t count) {
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	Verify();

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		idx_t string_size = 0;
		bool new_string = false;
		auto row_is_valid = vdata.validity.RowIsValid(idx);

		if (row_is_valid) {
			string_size = data[idx].GetSize();
			if (string_size >= StringUncompressed::GetStringBlockLimit(info.GetBlockSize())) {
				// Big strings not implemented for dictionary compression
				return false;
			}
			new_string = !LookupString(data[idx]);
		}

		bool fits = HasRoomForString(new_string, string_size);
		if (!fits) {
			// TODO: Check if the dictionary requires FSST encoding
			Flush();
			new_string = true;

			fits = HasRoomForString(new_string, string_size);
			if (!fits) {
				throw InternalException("Dictionary compression could not write to new segment");
			}
		}

		if (!row_is_valid) {
			AddNull();
		} else if (new_string) {
			AddNewString(data[idx]);
		} else {
			AddLastLookup();
		}

		Verify();
	}

	return true;
}

} // namespace dict_fsst
} // namespace duckdb
