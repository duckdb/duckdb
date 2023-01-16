#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {

CompressionType RowGroupWriter::GetColumnCompressionType(idx_t i) {
	return table.columns.GetColumn(LogicalIndex(i)).CompressionType();
}

void RowGroupWriter::RegisterPartialBlock(PartialBlockAllocation &&allocation) {
	partial_block_manager.RegisterPartialBlock(std::move(allocation));
}

PartialBlockAllocation RowGroupWriter::GetBlockAllocation(uint32_t segment_size) {
	return partial_block_manager.GetBlockAllocation(segment_size);
}

void SingleFileRowGroupWriter::WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state) {
	auto &meta_writer = table_data_writer;
	const auto &data_pointers = column_checkpoint_state.data_pointers;

	meta_writer.Write<idx_t>(data_pointers.size());
	// then write the data pointers themselves
	for (idx_t k = 0; k < data_pointers.size(); k++) {
		auto &data_pointer = data_pointers[k];
		meta_writer.Write<idx_t>(data_pointer.row_start);
		meta_writer.Write<idx_t>(data_pointer.tuple_count);
		meta_writer.Write<block_id_t>(data_pointer.block_pointer.block_id);
		meta_writer.Write<uint32_t>(data_pointer.block_pointer.offset);
		meta_writer.Write<CompressionType>(data_pointer.compression_type);
		data_pointer.statistics->Serialize(meta_writer);
	}
}

MetaBlockWriter &SingleFileRowGroupWriter::GetPayloadWriter() {
	return table_data_writer;
}

} // namespace duckdb
