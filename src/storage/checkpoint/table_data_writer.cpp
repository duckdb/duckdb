#include "duckdb/storage/checkpoint/table_data_writer.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

TableDataWriter::TableDataWriter(TableCatalogEntry &table_p, QueryContext context)
    : table(table_p.Cast<DuckTableEntry>()), context(context.GetClientContext()) {
	D_ASSERT(table_p.IsDuckTable());
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData(Serializer &metadata_serializer) {
	// start scanning the table and append the data to the uncompressed segments
	table.GetStorage().Checkpoint(*this, metadata_serializer);
}

void TableDataWriter::AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> writer) {
	row_group_pointers.push_back(std::move(row_group_pointer));
}

DatabaseInstance &TableDataWriter::GetDatabase() {
	return table.ParentCatalog().GetDatabase();
}

unique_ptr<TaskExecutor> TableDataWriter::CreateTaskExecutor() {
	if (context) {
		return make_uniq<TaskExecutor>(*context);
	}
	return make_uniq<TaskExecutor>(TaskScheduler::GetScheduler(GetDatabase()));
}

SingleFileTableDataWriter::SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                                     TableCatalogEntry &table, MetadataWriter &table_data_writer)
    : TableDataWriter(table, checkpoint_manager.GetClientContext()), checkpoint_manager(checkpoint_manager),
      table_data_writer(table_data_writer) {
}

unique_ptr<RowGroupWriter> SingleFileTableDataWriter::GetRowGroupWriter(RowGroup &row_group) {
	return make_uniq<SingleFileRowGroupWriter>(table, checkpoint_manager.partial_block_manager, *this,
	                                           table_data_writer);
}

CheckpointType SingleFileTableDataWriter::GetCheckpointType() const {
	return checkpoint_manager.GetCheckpointType();
}

MetadataManager &SingleFileTableDataWriter::GetMetadataManager() {
	return checkpoint_manager.GetMetadataManager();
}

void SingleFileTableDataWriter::WriteUnchangedTable(MetaBlockPointer pointer, idx_t total_rows) {
	existing_pointer = pointer;
	existing_rows = total_rows;
}

void SingleFileTableDataWriter::FinalizeTable(const TableStatistics &global_stats, DataTableInfo &info,
                                              RowGroupCollection &collection, Serializer &serializer) {
	MetaBlockPointer pointer;
	idx_t total_rows;
	if (!existing_pointer.IsValid()) {
		// write the metadata
		// store the current position in the metadata writer
		// this is where the row groups for this table start
		pointer = table_data_writer.GetMetaBlockPointer();

		// Serialize statistics as a single unit
		BinarySerializer stats_serializer(table_data_writer, serializer.GetOptions());
		stats_serializer.Begin();
		global_stats.Serialize(stats_serializer);
		stats_serializer.End();

		// now start writing the row group pointers to disk
		table_data_writer.Write<uint64_t>(row_group_pointers.size());
		total_rows = 0;
		for (auto &row_group_pointer : row_group_pointers) {
			auto row_group_count = row_group_pointer.row_start + row_group_pointer.tuple_count;
			if (row_group_count > total_rows) {
				total_rows = row_group_count;
			}

			// Each RowGroup is its own unit
			BinarySerializer row_group_serializer(table_data_writer, serializer.GetOptions());
			row_group_serializer.Begin();
			RowGroup::Serialize(row_group_pointer, row_group_serializer);
			row_group_serializer.End();
		}
		collection.FinalizeCheckpoint(pointer);
	} else {
		// we have existing metadata and the table is unchanged - write a pointer to the existing metadata
		pointer = existing_pointer;
		total_rows = existing_rows.GetIndex();

		// label the blocks as used again to prevent them from being freed
		auto &metadata_manager = checkpoint_manager.GetMetadataManager();
		MetadataReader reader(metadata_manager, pointer);
		auto blocks = reader.GetRemainingBlocks();
		metadata_manager.ClearModifiedBlocks(blocks);
	}

	// Now begin the metadata as a unit
	// Pointer to the table itself goes to the metadata stream.
	serializer.WriteProperty(101, "table_pointer", pointer);
	serializer.WriteProperty(102, "total_rows", total_rows);

	auto v1_0_0_storage = serializer.GetOptions().serialization_compatibility.serialization_version < 3;
	case_insensitive_map_t<Value> options;
	if (!v1_0_0_storage) {
		options.emplace("v1_0_0_storage", v1_0_0_storage);
	}
	auto index_storage_infos = info.GetIndexes().SerializeToDisk(context, options);

	auto debug_verify_blocks = DBConfig::GetSetting<DebugVerifyBlocksSetting>(GetDatabase());
	if (debug_verify_blocks) {
		for (auto &entry : index_storage_infos) {
			for (auto &allocator : entry.allocator_infos) {
				for (auto &block : allocator.block_pointers) {
					checkpoint_manager.verify_block_usage_count[block.block_id]++;
				}
			}
		}
	}

	// write empty block pointers for forwards compatibility
	vector<BlockPointer> compat_block_pointers;
	serializer.WriteProperty(103, "index_pointers", compat_block_pointers);
	serializer.WritePropertyWithDefault(104, "index_storage_infos", index_storage_infos);
}

} // namespace duckdb
