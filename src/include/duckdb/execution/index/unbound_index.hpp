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

inline idx_t GetReplayTypeIndex(BufferedIndexReplay type) {
	return static_cast<idx_t>(type);
}

struct ReplayRange {
	BufferedIndexReplay type;
	idx_t start;
	idx_t end;
	explicit ReplayRange(BufferedIndexReplay replay_type, idx_t start_p, idx_t end_p)
	    : type(replay_type), start(start_p), end(end_p) {
	}
};

// All inserts and deletes to be replayed are stored in their respective buffers.
// Since the inserts and deletes may be interleaved, however, ranges stores the ordering of operations
// and their offsets in the respective buffer.
// Simple example:
// ranges[0] - INSERT_ENTRY, [0,6]
// ranges[1] - DEL_ENTRY,    [0,3]
// ranges[2] - INSERT_ENTRY  [7,12]
// So even though the buffered_inserts has all the insert data from [0,12], ranges gives us the intervals for
// replaying the index operations in the right order.
struct BufferedIndexReplays {
	vector<ReplayRange> ranges;
	unique_ptr<ColumnDataCollection> buffered_inserts;
	unique_ptr<ColumnDataCollection> buffered_deletes;

	BufferedIndexReplays() = default;

	unique_ptr<ColumnDataCollection> &GetBuffer(BufferedIndexReplay replay_type) {
		if (replay_type == BufferedIndexReplay::INSERT_ENTRY) {
			return buffered_inserts;
		}
		return buffered_deletes;
	}

	bool HasBufferedReplays() const {
		return !ranges.empty();
	}
};

class UnboundIndex final : public Index {
private:
	//! The CreateInfo of the index.
	unique_ptr<CreateInfo> create_info;
	//! The serialized storage information of the index.
	IndexStorageInfo storage_info;

	BufferedIndexReplays buffered_replays;

	//! Maps the column IDs in the buffered replays to a physical table offset.
	//! For example, column [i] in a buffered ColumnDataCollection is the data for an Indexed column with
	//! physical table index mapped_column_ids[i].
	//! This is in sorted order of physical column IDs.
	vector<StorageIndex> mapped_column_ids;

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
		return buffered_replays.HasBufferedReplays();
	}

	BufferedIndexReplays &GetBufferedReplays() {
		return buffered_replays;
	}

	const vector<StorageIndex> &GetMappedColumnIds() const {
		return mapped_column_ids;
	}
};

} // namespace duckdb
