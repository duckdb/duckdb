#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

TableDataWriter::TableDataWriter(TableCatalogEntry &table_p) : table(table_p.Cast<DuckTableEntry>()) {
	D_ASSERT(table_p.IsDuckTable());
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData(Serializer &metadata_serializer) {
	// start scanning the table and append the data to the uncompressed segments
	table.GetStorage().Checkpoint(*this, metadata_serializer);
}

CompressionType TableDataWriter::GetColumnCompressionType(idx_t i) {
	return table.GetColumn(LogicalIndex(i)).CompressionType();
}

void TableDataWriter::AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> &&writer) {
	row_group_pointers.push_back(std::move(row_group_pointer));
	writer.reset();
}

SingleFileTableDataWriter::SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                                     TableCatalogEntry &table, MetadataWriter &table_data_writer)
    : TableDataWriter(table), checkpoint_manager(checkpoint_manager), table_data_writer(table_data_writer) {
}

unique_ptr<RowGroupWriter> SingleFileTableDataWriter::GetRowGroupWriter(RowGroup &row_group) {
	return make_uniq<SingleFileRowGroupWriter>(table, checkpoint_manager.partial_block_manager, table_data_writer);
}

void SingleFileTableDataWriter::FinalizeTable(TableStatistics &&global_stats, DataTableInfo *info,
                                              Serializer &metadata_serializer) {
	// store the current position in the metadata writer
	// this is where the row groups for this table start
	auto pointer = table_data_writer.GetMetaBlockPointer();

	// Serialize statistics as a single unit
	BinarySerializer stats_serializer(table_data_writer);
	stats_serializer.Begin();
	global_stats.Serialize(stats_serializer);
	stats_serializer.End();

	// now start writing the row group pointers to disk
	table_data_writer.Write<uint64_t>(row_group_pointers.size());
	idx_t total_rows = 0;
	for (auto &row_group_pointer : row_group_pointers) {
		auto row_group_count = row_group_pointer.row_start + row_group_pointer.tuple_count;
		if (row_group_count > total_rows) {
			total_rows = row_group_count;
		}

		// Each RowGroup is its own unit
		BinarySerializer row_group_serializer(table_data_writer);
		row_group_serializer.Begin();
		RowGroup::Serialize(row_group_pointer, row_group_serializer);
		row_group_serializer.End();
	}

	auto index_pointers = info->indexes.SerializeIndexes(table_data_writer);

	// Now begin the metadata as a unit
	// Pointer to the table itself goes to the metadata stream.
	metadata_serializer.WriteProperty(101, "table_pointer", pointer);
	metadata_serializer.WriteProperty(102, "total_rows", total_rows);
	metadata_serializer.WriteProperty(103, "index_pointers", index_pointers);
}

} // namespace duckdb
