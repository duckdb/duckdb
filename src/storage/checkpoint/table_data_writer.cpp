#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

namespace duckdb {

TableDataWriter::TableDataWriter(DatabaseInstance &, CheckpointManager &checkpoint_manager, TableCatalogEntry &table,
                                 MetaBlockWriter &table_data_writer, MetaBlockWriter &meta_data_writer)
    : checkpoint_manager(checkpoint_manager), table(table), table_data_writer(table_data_writer),
      meta_data_writer(meta_data_writer) {
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData() {
	// start scanning the table and append the data to the uncompressed segments
	table.storage->Checkpoint(*this);
}

CompressionType TableDataWriter::GetColumnCompressionType(idx_t i) {
	return table.columns[i].CompressionType();
}

} // namespace duckdb
