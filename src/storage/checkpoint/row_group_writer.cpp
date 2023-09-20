#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

CompressionType RowGroupWriter::GetColumnCompressionType(idx_t i) {
	return table.GetColumn(LogicalIndex(i)).CompressionType();
}

void RowGroupWriter::RegisterPartialBlock(PartialBlockAllocation &&allocation) {
	partial_block_manager.RegisterPartialBlock(std::move(allocation));
}

PartialBlockAllocation RowGroupWriter::GetBlockAllocation(uint32_t segment_size) {
	return partial_block_manager.GetBlockAllocation(segment_size);
}

void SingleFileRowGroupWriter::WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state,
                                                       Serializer &serializer) {
	const auto &data_pointers = column_checkpoint_state.data_pointers;
	serializer.WriteList(100, "data_pointers", data_pointers.size(), [&](Serializer::List &list, idx_t i) {
		auto &data_pointer = data_pointers[i];
		list.WriteElement(data_pointer);
	});
}

MetadataWriter &SingleFileRowGroupWriter::GetPayloadWriter() {
	return table_data_writer;
}

} // namespace duckdb
