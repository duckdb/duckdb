#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

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

void TableDataWriter::AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> writer) {
	row_group_pointers.push_back(std::move(row_group_pointer));
}

TaskScheduler &TableDataWriter::GetScheduler() {
	return TaskScheduler::GetScheduler(table.ParentCatalog().GetDatabase());
}

SingleFileTableDataWriter::SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                                     TableCatalogEntry &table, MetadataWriter &table_data_writer)
    : TableDataWriter(table), checkpoint_manager(checkpoint_manager), table_data_writer(table_data_writer) {
}

unique_ptr<RowGroupWriter> SingleFileTableDataWriter::GetRowGroupWriter(RowGroup &row_group) {
	return make_uniq<SingleFileRowGroupWriter>(table, checkpoint_manager.partial_block_manager, *this,
	                                           table_data_writer);
}

CheckpointType SingleFileTableDataWriter::GetCheckpointType() const {
	return checkpoint_manager.GetCheckpointType();
}

void SingleFileTableDataWriter::FinalizeTable(const TableStatistics &global_stats, DataTableInfo *info,
                                              Serializer &serializer) {
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

	// Now begin the metadata as a unit
	// Pointer to the table itself goes to the metadata stream.
	serializer.WriteProperty(101, "table_pointer", pointer);
	serializer.WriteProperty(102, "total_rows", total_rows);

	auto index_storage_infos = info->GetIndexes().GetStorageInfos();
	// write empty block pointers for forwards compatibility
	vector<BlockPointer> compat_block_pointers;
	serializer.WriteProperty(103, "index_pointers", compat_block_pointers);
	serializer.WritePropertyWithDefault(104, "index_storage_infos", index_storage_infos);
}

} // namespace duckdb
