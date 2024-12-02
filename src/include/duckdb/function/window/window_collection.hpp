//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

// A wrapper for building ColumnDataCollections in parallel
class WindowCollection {
public:
	using ColumnDataCollectionPtr = unique_ptr<ColumnDataCollection>;
	using ColumnDataCollectionSpec = pair<idx_t, optional_ptr<ColumnDataCollection>>;
	using ColumnSet = unordered_set<column_t>;

	WindowCollection(BufferManager &buffer_manager, idx_t count, const vector<LogicalType> &types);

	idx_t ColumnCount() const {
		return types.size();
	}

	idx_t size() const { // NOLINT
		return count;
	}

	const vector<LogicalType> &GetTypes() const {
		return types;
	}

	//! Update a thread-local collection for appending data to a given row
	void GetCollection(idx_t row_idx, ColumnDataCollectionSpec &spec);
	//! Single-threaded, idempotent ordered combining of all the appended data.
	void Combine(const ColumnSet &build_validity);

	//! The collection data. May be null if the column count is 0.
	ColumnDataCollectionPtr inputs;
	//! Global validity mask
	vector<atomic<bool>> all_valids;
	//! Optional validity mask for the entire collection
	vector<ValidityMask> validities;

	//! The collection columns
	const vector<LogicalType> types;
	//! The collection rows
	const idx_t count;

	//! Guard for range updates
	mutex lock;
	//! The paging buffer manager to use
	BufferManager &buffer_manager;
	//! The component column data collections
	vector<ColumnDataCollectionPtr> collections;
	//! The (sorted) collection ranges
	using Range = pair<idx_t, idx_t>;
	vector<Range> ranges;
};

class WindowBuilder {
public:
	explicit WindowBuilder(WindowCollection &collection);

	//! Add a new chunk at the given index
	void Sink(DataChunk &chunk, idx_t input_idx);

	//! The collection we are helping to build
	WindowCollection &collection;
	//! The thread's current input collection
	using ColumnDataCollectionSpec = WindowCollection::ColumnDataCollectionSpec;
	ColumnDataCollectionSpec sink;
	//! The state used for appending to the collection
	ColumnDataAppendState appender;
	//! Are all the sunk rows valid?
	bool all_valid = true;
};

class WindowCursor {
public:
	WindowCursor(const WindowCollection &paged, column_t col_idx);
	WindowCursor(const WindowCollection &paged, vector<column_t> column_ids);

	//! Is the scan in range?
	inline bool RowIsVisible(idx_t row_idx) const {
		return (row_idx < state.next_row_index && state.current_row_index <= row_idx);
	}
	//! The offset of the row in the given state
	inline sel_t RowOffset(idx_t row_idx) const {
		D_ASSERT(RowIsVisible(row_idx));
		return UnsafeNumericCast<sel_t>(row_idx - state.current_row_index);
	}
	//! Scan the next chunk
	inline bool Scan() {
		return paged.inputs->Scan(state, chunk);
	}
	//! Seek to the given row
	inline idx_t Seek(idx_t row_idx) {
		if (!RowIsVisible(row_idx)) {
			D_ASSERT(paged.inputs.get());
			paged.inputs->Seek(row_idx, state, chunk);
		}
		return RowOffset(row_idx);
	}
	//! Check a collection cell for nullity
	bool CellIsNull(idx_t col_idx, idx_t row_idx) {
		D_ASSERT(chunk.ColumnCount() > col_idx);
		auto index = Seek(row_idx);
		auto &source = chunk.data[col_idx];
		return FlatVector::IsNull(source, index);
	}
	//! Read a typed cell
	template <typename T>
	T GetCell(idx_t col_idx, idx_t row_idx) {
		D_ASSERT(chunk.ColumnCount() > col_idx);
		auto index = Seek(row_idx);
		auto &source = chunk.data[col_idx];
		const auto data = FlatVector::GetData<T>(source);
		return data[index];
	}
	//! Copy a single value
	void CopyCell(idx_t col_idx, idx_t row_idx, Vector &target, idx_t target_offset) {
		D_ASSERT(chunk.ColumnCount() > col_idx);
		auto index = Seek(row_idx);
		auto &source = chunk.data[col_idx];
		VectorOperations::Copy(source, target, index + 1, index, target_offset);
	}

	unique_ptr<WindowCursor> Copy() const {
		return make_uniq<WindowCursor>(paged, state.column_ids);
	}

	//! The pageable data
	const WindowCollection &paged;
	//! The state used for reading the collection
	ColumnDataScanState state;
	//! The data chunk read into
	DataChunk chunk;
};

} // namespace duckdb
