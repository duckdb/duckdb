#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {

OptimisticDataWriter::OptimisticDataWriter(ClientContext &context, DataTable &table) : context(context), table(table) {
}

OptimisticDataWriter::OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent)
    : context(parent.GetClientContext()), table(table) {
	if (parent.partial_manager) {
		parent.partial_manager->ClearBlocks();
	}
}

OptimisticDataWriter::~OptimisticDataWriter() {
}

bool OptimisticDataWriter::PrepareWrite() {
	// check if we should pre-emptively write the table to disk
	if (table.IsTemporary() || StorageManager::Get(table.GetAttached()).InMemory()) {
		return false;
	}
	// we should! write the second-to-last row group to disk
	// allocate the partial block-manager if none is allocated yet
	if (!partial_manager) {
		auto &block_manager = table.GetTableIOManager().GetBlockManagerForRowData();
		partial_manager =
		    make_uniq<PartialBlockManager>(QueryContext(context), block_manager, PartialBlockType::APPEND_TO_TABLE);
	}
	return true;
}

unique_ptr<OptimisticWriteCollection> OptimisticDataWriter::CreateCollection(DataTable &storage,
                                                                             const vector<LogicalType> &insert_types) {
	auto table_info = storage.GetDataTableInfo();
	auto &io_manager = TableIOManager::Get(storage);

	// Create the local row group collection.
	auto max_row_id = NumericCast<idx_t>(MAX_ROW_ID);
	auto row_groups = make_shared_ptr<RowGroupCollection>(std::move(table_info), io_manager, insert_types, max_row_id);

	auto result = make_uniq<OptimisticWriteCollection>();
	result->collection = std::move(row_groups);
	return result;
}

void OptimisticDataWriter::WriteNewRowGroup(OptimisticWriteCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}
	// flush second-to-last row group
	auto row_group = row_groups.collection->GetRowGroup(-2);
	FlushToDisk(*row_group);
}

void OptimisticDataWriter::WriteLastRowGroup(OptimisticWriteCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}
	// flush second-to-last row group
	auto row_group = row_groups.collection->GetRowGroup(-1);
	if (!row_group) {
		return;
	}
	FlushToDisk(*row_group);
}

void OptimisticDataWriter::FlushToDisk(RowGroup &row_group) {
	//! The set of column compression types (if any)
	vector<CompressionType> compression_types;
	D_ASSERT(compression_types.empty());
	for (auto &column : table.Columns()) {
		compression_types.push_back(column.CompressionType());
	}
	RowGroupWriteInfo info(*partial_manager, compression_types);
	row_group.WriteToDisk(info);
}

void OptimisticDataWriter::Merge(OptimisticDataWriter &other) {
	if (!other.partial_manager) {
		return;
	}
	if (!partial_manager) {
		partial_manager = std::move(other.partial_manager);
		return;
	}
	partial_manager->Merge(*other.partial_manager);
	other.partial_manager.reset();
}

void OptimisticDataWriter::FinalFlush() {
	if (partial_manager) {
		partial_manager->FlushPartialBlocks();
		partial_manager.reset();
	}
}

void OptimisticDataWriter::Rollback() {
	if (partial_manager) {
		partial_manager->Rollback();
		partial_manager.reset();
	}
}

} // namespace duckdb
