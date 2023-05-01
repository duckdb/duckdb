#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {

OptimisticDataWriter::OptimisticDataWriter(DataTable &table, shared_ptr<PartialBlockManager> partial_manager_p)
    : table(table), partial_manager(std::move(partial_manager_p)), written_anything(false) {
	if (!partial_manager) {
		throw InternalException("Cannot create an optimistic data writer without a partial block manager");
	}
}

OptimisticDataWriter::OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent)
    : table(table), partial_manager(parent.partial_manager), written_anything(parent.written_anything) {
}

OptimisticDataWriter::~OptimisticDataWriter() {
}

bool OptimisticDataWriter::PrepareWrite() {
	// check if we should pre-emptively write the table to disk
	if (table.info->IsTemporary() || StorageManager::Get(table.info->db).InMemory()) {
		return false;
	}
	// we should! write the second-to-last row group to disk
	written_anything = true;
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
	row_group->WriteToDisk(*partial_manager, compression_types);
}

void OptimisticDataWriter::FlushToDisk(RowGroupCollection &row_groups, bool force) {
	if (!written_anything) {
		if (!force) {
			// nothing has been written yet - return
			return;
		}
		if (!PrepareWrite()) {
			return;
		}
	}
	// flush the last row group
	FlushToDisk(row_groups.GetRowGroup(-1));
}

} // namespace duckdb
