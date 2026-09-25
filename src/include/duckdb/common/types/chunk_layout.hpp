//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/chunk_layout.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct ChunkLayoutData {
	vector<LogicalType> types;
};

class ChunkLayoutBuilder;
class ChunkLayout;
class ChunkProjection;

//! A column handle belongs to one layout, including copies of that layout.
class ChunkColumn {
	friend class ChunkColumnGroup;
	friend class ChunkLayout;
	friend class ChunkLayoutBuilder;
	friend class ChunkProjection;

private:
	ChunkColumn(shared_ptr<const ChunkLayoutData> layout, idx_t index) : layout(std::move(layout)), index(index) {
	}
	shared_ptr<const ChunkLayoutData> layout;
	idx_t index;
};

class ChunkColumnGroup {
	friend class ChunkLayout;
	friend class ChunkLayoutBuilder;

public:
	idx_t ColumnCount() const {
		return count;
	}
	ChunkColumn Column(idx_t index) const;

private:
	ChunkColumnGroup(shared_ptr<const ChunkLayoutData> layout, idx_t offset, idx_t count)
	    : layout(std::move(layout)), offset(offset), count(count) {
	}
	shared_ptr<const ChunkLayoutData> layout;
	idx_t offset;
	idx_t count;
};

//! A borrowed contiguous group of vectors. The chunk must outlive the view.
class ChunkColumnView {
	friend class ChunkLayout;

public:
	idx_t ColumnCount() const {
		return count;
	}
	idx_t RowCount() const {
		return chunk.size();
	}
	Vector &Column(idx_t index) const;
	void ReferenceInto(DataChunk &target) const;
	//! Populate only this group; the caller finalizes the containing chunk's row count.
	void ReferenceFrom(const ChunkColumnView &source) const;
	//! Access the vector array required by aggregate callbacks; empty groups return null.
	optional_ptr<Vector> ContiguousVectors() const;
	//! Access the same columns after a row selection has been applied to the chunk.
	ChunkColumnView Rebind(DataChunk &target) const;

private:
	ChunkColumnView(DataChunk &chunk, idx_t offset, idx_t count) : chunk(chunk), offset(offset), count(count) {
	}
	DataChunk &chunk;
	idx_t offset;
	idx_t count;
};

//! Immutable schema; group and column handles remain valid across copies and moves.
class ChunkLayout {
	friend class ChunkLayoutBuilder;
	friend class ChunkProjection;

public:
	const vector<LogicalType> &GetTypes() const {
		return data->types;
	}
	ChunkColumnGroup AllColumns() const;
	//! Resolve a handle for APIs that still require a flat column index.
	idx_t GetColumnIndex(const ChunkColumn &column) const;
	Vector &Column(DataChunk &chunk, const ChunkColumn &column) const;
	ChunkColumnView Columns(DataChunk &chunk, const ChunkColumnGroup &group) const;
	void Verify(const DataChunk &chunk) const;

private:
	explicit ChunkLayout(shared_ptr<const ChunkLayoutData> data) : data(std::move(data)) {
	}
	shared_ptr<const ChunkLayoutData> data;
};

class ChunkLayoutBuilder {
public:
	ChunkLayoutBuilder();
	ChunkLayoutBuilder(const ChunkLayoutBuilder &) = delete;
	ChunkLayoutBuilder &operator=(const ChunkLayoutBuilder &) = delete;

	ChunkColumn AddColumn(const LogicalType &type);
	ChunkColumnGroup AddColumns(const vector<LogicalType> &types);
	ChunkLayout Build();

private:
	shared_ptr<ChunkLayoutData> data;
};

//! A complete projection into a layout; source columns can be reordered or repeated.
class ChunkProjection {
public:
	ChunkProjection(ChunkLayout source, ChunkLayout target, vector<ChunkColumn> columns);
	void Reference(DataChunk &source, DataChunk &target) const;

private:
	ChunkLayout source_layout;
	ChunkLayout target_layout;
	vector<column_t> columns;
};

} // namespace duckdb
