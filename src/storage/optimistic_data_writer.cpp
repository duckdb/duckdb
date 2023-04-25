#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

namespace duckdb {

OptimisticDataWriter::OptimisticDataWriter(DataTable &table) : table(table) {
}

OptimisticDataWriter::OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent)
    : table(table), partial_manager(std::move(parent.partial_manager)),
      written_blocks(std::move(parent.written_blocks)) {
	if (partial_manager) {
		partial_manager->FlushPartialBlocks();
	}
}

OptimisticDataWriter::~OptimisticDataWriter() {
}

bool OptimisticDataWriter::PrepareWrite() {
	// check if we should pre-emptively write the table to disk
	if (table.info->IsTemporary() || StorageManager::Get(table.info->db).InMemory()) {
		return false;
	}
	// we should! write the second-to-last row group to disk
	// allocate the partial block-manager if none is allocated yet
	if (!partial_manager) {
		auto &block_manager = table.info->table_io_manager->GetBlockManagerForRowData();
		partial_manager = make_uniq<PartialBlockManager>(block_manager);
	}
	return true;
}

void OptimisticDataWriter::CheckFlushToDisk(RowGroupCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}
	// flush second-to-last row group
	auto row_group = row_groups.GetRowGroup(-2);
	FlushToDisk(row_group);
}

void OptimisticDataWriter::FlushToDisk(RowGroup *row_group) {
	// flush the specified row group
	D_ASSERT(row_group);
	//! The set of column compression types (if any)
	vector<CompressionType> compression_types;
	D_ASSERT(compression_types.empty());
	for (auto &column : table.column_definitions) {
		compression_types.push_back(column.CompressionType());
	}
	auto row_group_pointer = row_group->WriteToDisk(*partial_manager, compression_types);

	// update the set of written blocks
	for (idx_t col_idx = 0; col_idx < row_group_pointer.statistics.size(); col_idx++) {
		row_group_pointer.states[col_idx]->GetBlockIds(written_blocks);
	}
}

void OptimisticDataWriter::FlushToDisk(RowGroupCollection &row_groups, bool force) {
	if (!partial_manager) {
		if (!force) {
			// no partial manager - nothing to flush
			return;
		}
		if (!PrepareWrite()) {
			return;
		}
	}
	// flush the last row group
	FlushToDisk(row_groups.GetRowGroup(-1));
}

void OptimisticDataWriter::FinalFlush() {
	if (!partial_manager) {
		return;
	}
	// then flush the partial manager
	partial_manager->FlushPartialBlocks();
	partial_manager.reset();
}

void OptimisticDataWriter::Rollback() {
	if (partial_manager) {
		partial_manager->Clear();
		partial_manager.reset();
	}
	if (!written_blocks.empty()) {
		auto &block_manager = table.info->table_io_manager->GetBlockManagerForRowData();
		for (auto block_id : written_blocks) {
			block_manager.MarkBlockAsFree(block_id);
		}
	}
}

} // namespace duckdb
