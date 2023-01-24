#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {

TableDataWriter::TableDataWriter(TableCatalogEntry &table) : table(table) {
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData() {
	// start scanning the table and append the data to the uncompressed segments
	table.storage->Checkpoint(*this);
}

CompressionType TableDataWriter::GetColumnCompressionType(idx_t i) {
	return table.columns.GetColumn(LogicalIndex(i)).CompressionType();
}

void TableDataWriter::AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> &&writer) {
	row_group_pointers.push_back(std::move(row_group_pointer));
	writer.reset();
}

SingleFileTableDataWriter::SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                                     TableCatalogEntry &table, MetaBlockWriter &table_data_writer,
                                                     MetaBlockWriter &meta_data_writer)
    : TableDataWriter(table), checkpoint_manager(checkpoint_manager), table_data_writer(table_data_writer),
      meta_data_writer(meta_data_writer) {
}

unique_ptr<RowGroupWriter> SingleFileTableDataWriter::GetRowGroupWriter(RowGroup &row_group) {
	return make_unique<SingleFileRowGroupWriter>(table, checkpoint_manager.partial_block_manager, table_data_writer);
}

void SingleFileTableDataWriter::FinalizeTable(vector<unique_ptr<BaseStatistics>> &&global_stats, DataTableInfo *info) {
	// store the current position in the metadata writer
	// this is where the row groups for this table start
	auto pointer = table_data_writer.GetBlockPointer();

	for (auto &stats : global_stats) {
		stats->Serialize(table_data_writer);
	}
	// now start writing the row group pointers to disk
	table_data_writer.Write<uint64_t>(row_group_pointers.size());
	for (auto &row_group_pointer : row_group_pointers) {
		RowGroup::Serialize(row_group_pointer, table_data_writer);
	}

	// Pointer to the table itself goes to the metadata stream.
	meta_data_writer.Write<block_id_t>(pointer.block_id);
	meta_data_writer.Write<uint64_t>(pointer.offset);

	// Now we serialize indexes in the table_metadata_writer
	std::vector<BlockPointer> index_pointers = info->indexes.SerializeIndexes(table_data_writer);

	// Write-off to metadata block ids and offsets of indexes
	meta_data_writer.Write<idx_t>(index_pointers.size());
	for (auto &block_info : index_pointers) {
		meta_data_writer.Write<idx_t>(block_info.block_id);
		meta_data_writer.Write<idx_t>(block_info.offset);
	}
}

} // namespace duckdb
