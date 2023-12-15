#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

CompressionType RowGroupWriter::GetColumnCompressionType(idx_t i) {
	return table.GetColumn(LogicalIndex(i)).CompressionType();
}

void SingleFileRowGroupWriter::WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state,
                                                       Serializer &serializer) {
	const auto &data_pointers = column_checkpoint_state.data_pointers;
	serializer.WriteProperty(100, "data_pointers", data_pointers);
}

MetadataWriter &SingleFileRowGroupWriter::GetPayloadWriter() {
	return table_data_writer;
}

} // namespace duckdb
