#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

TableDataWriter::TableDataWriter(TableCatalogEntry &table_p) : table(table_p.Cast<DuckTableEntry>()) {
	D_ASSERT(table_p.IsDuckTable());
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData() {
	// start scanning the table and append the data to the uncompressed segments
	table.GetStorage().Checkpoint(*this);
}

CompressionType TableDataWriter::GetColumnCompressionType(idx_t i) {
	return table.GetColumn(LogicalIndex(i)).CompressionType();
}

void TableDataWriter::AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> &&writer) {
	row_group_pointers.push_back(std::move(row_group_pointer));
	writer.reset();
}

SingleFileTableDataWriter::SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                                     TableCatalogEntry &table, MetadataWriter &table_data_writer,
                                                     MetadataWriter &meta_data_writer)
    : TableDataWriter(table), checkpoint_manager(checkpoint_manager), table_data_writer(table_data_writer),
      meta_data_writer(meta_data_writer) {
}

unique_ptr<RowGroupWriter> SingleFileTableDataWriter::GetRowGroupWriter(RowGroup &row_group) {
	return make_uniq<SingleFileRowGroupWriter>(table, checkpoint_manager.partial_block_manager, table_data_writer);
}

void SingleFileTableDataWriter::FinalizeTable(TableStatistics &&global_stats, DataTableInfo *info) {

	auto pointer = table_data_writer.GetMetaBlockPointer();

	// This is the table data stream
	BinarySerializer table_serializer(table_data_writer);
	table_serializer.Begin();
	table_serializer.WriteProperty(100, "table_stats", global_stats);

	// now start writing the row group pointers to disk
	idx_t total_rows = 0;
	table_serializer.WriteProperty<uint64_t>(101, "row_group_count", row_group_pointers.size());
	table_serializer.WriteList(
	    102, "row_group_pointers", row_group_pointers.size(), [&](FormatSerializer::List &list, idx_t i) {
		    auto &row_group_pointer = row_group_pointers[i];
		    auto row_group_count = row_group_pointer.row_start + row_group_pointer.tuple_count;
		    if (row_group_count > total_rows) {
			    total_rows = row_group_count;
		    }

		    list.WriteObject([&](FormatSerializer &obj) { RowGroup::FormatSerialize(row_group_pointer, obj); });
	    });
	table_serializer.End();

	// TODO: replace with FormatSerialize
	// serialize indexes in the table stream
	auto index_pointers = info->indexes.SerializeIndexes(table_data_writer);

	// Now we write to the metadata stream
	BinarySerializer meta_serializer(meta_data_writer);
	meta_serializer.Begin();

	// Pointer to the table itself goes to the metadata stream.
	meta_serializer.WriteProperty(100, "table_pointer", pointer);
	meta_serializer.WriteProperty(101, "total_rows", total_rows);

	// Write-off to metadata block ids and offsets of indexes
	meta_serializer.WriteProperty(102, "index_pointers", index_pointers);
	meta_serializer.End();

	// throw InternalException("TODO: FinalizeTable");
	// store the current position in the metadata writer
	// this is where the row groups for this table start
	/*
	auto pointer = table_data_writer.GetMetaBlockPointer();

	global_stats.Serialize(table_data_writer);

	// now start writing the row group pointers to disk
	table_data_writer.Write<uint64_t>(row_group_pointers.size());
	idx_t total_rows = 0;
	for (auto &row_group_pointer : row_group_pointers) {
	    auto row_group_count = row_group_pointer.row_start + row_group_pointer.tuple_count;
	    if (row_group_count > total_rows) {
	        total_rows = row_group_count;
	    }
	    RowGroup::Serialize(row_group_pointer, table_data_writer);
	}

	// Pointer to the table itself goes to the metadata stream.
	meta_data_writer.Write<idx_t>(pointer.block_pointer);
	meta_data_writer.Write<uint64_t>(pointer.offset);
	meta_data_writer.Write<idx_t>(total_rows);

	// Now we serialize indexes in the table_metadata_writer
	auto index_pointers = info->indexes.SerializeIndexes(table_data_writer);

	// Write-off to metadata block ids and offsets of indexes
	meta_data_writer.Write<idx_t>(index_pointers.size());
	for (auto &block_info : index_pointers) {
	    meta_data_writer.Write<block_id_t>(block_info.block_id);
	    meta_data_writer.Write<uint32_t>(block_info.offset);
	}*/
}

} // namespace duckdb
