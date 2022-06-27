#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

namespace duckdb {

TableDataWriter::TableDataWriter(DatabaseInstance &, CheckpointManager &checkpoint_manager, TableCatalogEntry &table,
                                 MetaBlockWriter &meta_writer)
    : checkpoint_manager(checkpoint_manager), table(table), meta_writer(meta_writer) {
}

TableDataWriter::~TableDataWriter() {
}

BlockPointer TableDataWriter::WriteTableData() {
	// start scanning the table and append the data to the uncompressed segments
	return table.storage->Checkpoint(*this);
}

CompressionType TableDataWriter::GetColumnCompressionType(idx_t i) {
	return table.columns[i].CompressionType();
}

} // namespace duckdb
