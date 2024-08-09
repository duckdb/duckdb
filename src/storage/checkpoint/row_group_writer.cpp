#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

CompressionType RowGroupWriter::GetColumnCompressionType(idx_t i) {
	return table.GetColumn(LogicalIndex(i)).CompressionType();
}

SingleFileRowGroupWriter::SingleFileRowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_manager,
                                                   TableDataWriter &writer, MetadataWriter &table_data_writer)
    : RowGroupWriter(table, partial_block_manager), writer(writer), table_data_writer(table_data_writer) {
}

CheckpointType SingleFileRowGroupWriter::GetCheckpointType() const {
	return writer.GetCheckpointType();
}

MetadataWriter &SingleFileRowGroupWriter::GetPayloadWriter() {
	return table_data_writer;
}

} // namespace duckdb
