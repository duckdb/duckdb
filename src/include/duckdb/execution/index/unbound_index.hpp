//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/unbound_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

class ColumnDataCollection;

enum class BufferedIndexReplay : uint8_t { INSERT_ENTRY = 0, DEL_ENTRY = 1 };
static constexpr idx_t RESIZE_THRESHOLD = 128;

struct BufferedIndexData {
	BufferedIndexReplay type;

	// Buffered replay data.
	unique_ptr<ColumnDataCollection> data;

	// Intermediate staging buffer before spilling to ColumnDataCollection data, since ColumnDataCollections
	// only allow storing in fixed STANDARD_VECTOR_SIZE buffers, we keep this intermediate, resizable small_chunk
	// to avoid eating up memory.
	// Internally, it starts out at size 1 and grows by powers of 2. If we finish buffering in this BufferedIndexData
	// element (i.e., switching the type of operation we are buffering), and there are more than RESIZE_THRESHOLD
	// empty slots in small_chunk, it gets resized down to its size.
	// For example, if UnboundIndex is buffering 1 INSERT -> 1 DELETE -> 1 INSERT -> 1 DELETE -> ...
	// it will only ever store in small_chunk and avoid storing in ColumnDataCollection data.
	unique_ptr<DataChunk> small_chunk;

	BufferedIndexData(BufferedIndexReplay replay_type);
};

class UnboundIndex final : public Index {
private:
	//! The CreateInfo of the index.
	unique_ptr<CreateInfo> create_info;
	//! The serialized storage information of the index.
	IndexStorageInfo storage_info;
	//! Buffer for WAL replays.
	vector<BufferedIndexData> buffered_replays;

	//! Maps the column IDs in the buffered replays to a physical table offset.
	//! For example, column [i] in a buffered ColumnDataCollection is the data for an Indexed column with
	//! physical table index mapped_column_ids[i].
	//! This is in sorted order of physical column IDs.
	vector<StorageIndex> mapped_column_ids;

	//! Resize or destroy small_chunk if it has too many free slots or is empty.
	//! If the chunk is empty, it is destroyed. If unused capacity exceeds fixed threshold defined in unbound_index
	//! it is resized down to how many elements it is storing (capacity = size).
	static void ResizeSmallChunk(unique_ptr<DataChunk> &small_chunk, Allocator &allocator,
	                             const vector<LogicalType> &types);

public:
	UnboundIndex(unique_ptr<CreateInfo> create_info, IndexStorageInfo storage_info, TableIOManager &table_io_manager,
	             AttachedDatabase &db);

public:
	bool IsBound() const override {
		return false;
	}
	const string &GetIndexType() const override {
		return GetCreateInfo().index_type;
	}
	const string &GetIndexName() const override {
		return GetCreateInfo().index_name;
	}
	IndexConstraintType GetConstraintType() const override {
		return GetCreateInfo().constraint_type;
	}
	const CreateIndexInfo &GetCreateInfo() const {
		return create_info->Cast<CreateIndexInfo>();
	}
	const IndexStorageInfo &GetStorageInfo() const {
		return storage_info;
	}
	const vector<unique_ptr<ParsedExpression>> &GetParsedExpressions() const {
		return GetCreateInfo().parsed_expressions;
	}
	const string &GetTableName() const {
		return GetCreateInfo().table;
	}

	void CommitDrop() override;

	//! Buffer Index delete or insert (replay_type) data chunk.
	//! See note above on mapped_column_ids, this function assumes that index_column_chunk maps into
	//! mapped_column_ids_p to get the physical column index for each Indexed column in the chunk.
	void BufferChunk(DataChunk &index_column_chunk, Vector &row_ids, const vector<StorageIndex> &mapped_column_ids_p,
	                 BufferedIndexReplay replay_type);
	bool HasBufferedReplays() const {
		return !buffered_replays.empty();
	}

	vector<BufferedIndexData> &GetBufferedReplays() {
		return buffered_replays;
	}
	const vector<StorageIndex> &GetMappedColumnIds() const {
		return mapped_column_ids;
	}
};

} // namespace duckdb
