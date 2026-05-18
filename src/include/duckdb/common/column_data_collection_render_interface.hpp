//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/column_data_collection_render_interface.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
class ColumnDataCollection;

//! Abstract read-only interface over columnar data, used by BoxRenderer.
//! Two implementations: one wrapping a real ColumnDataCollection, one fetching lazily.
class ColumnDataCollectionRenderInterface {
public:
	virtual ~ColumnDataCollectionRenderInterface() = default;

	virtual const vector<LogicalType> &Types() const = 0;
	virtual idx_t Count() const = 0;
	virtual idx_t ColumnCount() const = 0;
	virtual idx_t ChunkCount() const = 0;
	virtual void FetchChunk(idx_t chunk_idx, DataChunk &result) const = 0;
};

//! Classic implementation: wraps a real ColumnDataCollection and forwards all calls.
class ColumnDataCollectionWrapper : public ColumnDataCollectionRenderInterface {
public:
	explicit ColumnDataCollectionWrapper(const ColumnDataCollection &collection);

	const vector<LogicalType> &Types() const override;
	idx_t Count() const override;
	idx_t ColumnCount() const override;
	idx_t ChunkCount() const override;
	void FetchChunk(idx_t chunk_idx, DataChunk &result) const override;

private:
	const ColumnDataCollection &collection;
};

} // namespace duckdb
