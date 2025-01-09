#include "duckdb/storage/compression/dictionary/common.hpp"

namespace duckdb {
namespace dictionary {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

StringDictionaryContainer DictionaryCompression::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_size));
	container.end = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_end));
	return container;
}

void DictionaryCompression::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                          StringDictionaryContainer container) {
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	Store<uint32_t>(container.size, data_ptr_cast(&header_ptr->dict_size));
	Store<uint32_t>(container.end, data_ptr_cast(&header_ptr->dict_end));
}

} // namespace dictionary
} // namespace duckdb
