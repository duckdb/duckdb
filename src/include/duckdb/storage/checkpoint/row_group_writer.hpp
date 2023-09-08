//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/row_group_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint_manager.hpp"

namespace duckdb {
struct ColumnCheckpointState;
class CheckpointWriter;
class ColumnData;
class ColumnSegment;
class RowGroup;
class BaseStatistics;
class SegmentStatistics;

// Writes data for an entire row group.
class RowGroupWriter {
public:
	RowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_manager)
	    : table(table), partial_block_manager(partial_block_manager) {
	}
	virtual ~RowGroupWriter() {
	}

	CompressionType GetColumnCompressionType(idx_t i);

	virtual void WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state, Serializer &serializer) = 0;

	virtual MetadataWriter &GetPayloadWriter() = 0;

	void RegisterPartialBlock(PartialBlockAllocation &&allocation);
	PartialBlockAllocation GetBlockAllocation(uint32_t segment_size);

	PartialBlockManager &GetPartialBlockManager() {
		return partial_block_manager;
	}

protected:
	TableCatalogEntry &table;
	PartialBlockManager &partial_block_manager;
};

// Writes data for an entire row group.
class SingleFileRowGroupWriter : public RowGroupWriter {
public:
	SingleFileRowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_manager,
	                         MetadataWriter &table_data_writer)
	    : RowGroupWriter(table, partial_block_manager), table_data_writer(table_data_writer) {
	}

	//! MetadataWriter is a cursor on a given BlockManager. This returns the
	//! cursor against which we should write payload data for the specified RowGroup.
	MetadataWriter &table_data_writer;

public:
	virtual void WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state,
	                                     Serializer &serializer) override;

	virtual MetadataWriter &GetPayloadWriter() override;
};

} // namespace duckdb
