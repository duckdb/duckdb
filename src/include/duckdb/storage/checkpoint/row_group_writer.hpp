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

	virtual CheckpointType GetCheckpointType() const = 0;
	virtual MetadataWriter &GetPayloadWriter() = 0;

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
	                         TableDataWriter &writer, MetadataWriter &table_data_writer);

public:
	CheckpointType GetCheckpointType() const override;
	MetadataWriter &GetPayloadWriter() override;

private:
	//! Underlying writer object
	TableDataWriter &writer;
	//! MetadataWriter is a cursor on a given BlockManager. This returns the
	//! cursor against which we should write payload data for the specified RowGroup.
	MetadataWriter &table_data_writer;
};

} // namespace duckdb
