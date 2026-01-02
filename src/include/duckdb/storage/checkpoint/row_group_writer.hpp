//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/row_group_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint_manager.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

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
	RowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_manager);
	virtual ~RowGroupWriter() {
	}

	const vector<CompressionType> &GetCompressionTypes() const {
		return compression_types;
	}

	virtual CheckpointOptions GetCheckpointOptions() const = 0;
	virtual WriteStream &GetPayloadWriter() = 0;
	virtual MetaBlockPointer GetMetaBlockPointer() = 0;
	virtual optional_ptr<MetadataManager> GetMetadataManager() = 0;
	virtual void StartWritingColumns(vector<MetaBlockPointer> &column_metadata) {
	}
	virtual void FinishWritingColumns() {
	}

	DatabaseInstance &GetDatabase();
	PartialBlockManager &GetPartialBlockManager() {
		return partial_block_manager;
	}

protected:
	TableCatalogEntry &table;
	PartialBlockManager &partial_block_manager;
	vector<CompressionType> compression_types;
};

// Writes data for an entire row group.
class SingleFileRowGroupWriter : public RowGroupWriter {
public:
	SingleFileRowGroupWriter(TableCatalogEntry &table, PartialBlockManager &partial_block_manager,
	                         TableDataWriter &writer, MetadataWriter &table_data_writer);

public:
	CheckpointOptions GetCheckpointOptions() const override;
	WriteStream &GetPayloadWriter() override;
	MetaBlockPointer GetMetaBlockPointer() override;
	optional_ptr<MetadataManager> GetMetadataManager() override;
	void StartWritingColumns(vector<MetaBlockPointer> &column_metadata) override;
	void FinishWritingColumns() override;

private:
	//! Underlying writer object
	TableDataWriter &writer;
	//! MetadataWriter is a cursor on a given BlockManager. This returns the
	//! cursor against which we should write payload data for the specified RowGroup.
	MetadataWriter &table_data_writer;
};

} // namespace duckdb
