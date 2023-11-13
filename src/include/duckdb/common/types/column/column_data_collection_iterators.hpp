//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column/column_data_collection_iterators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_scan_states.hpp"

namespace duckdb {
class ColumnDataCollection;

class ColumnDataChunkIterationHelper {
public:
	DUCKDB_API ColumnDataChunkIterationHelper(const ColumnDataCollection &collection, vector<column_t> column_ids);

private:
	const ColumnDataCollection &collection;
	vector<column_t> column_ids;

private:
	class ColumnDataChunkIterator;

	class ColumnDataChunkIterator {
	public:
		DUCKDB_API explicit ColumnDataChunkIterator(const ColumnDataCollection *collection_p,
		                                            vector<column_t> column_ids);

		const ColumnDataCollection *collection;
		ColumnDataScanState scan_state;
		shared_ptr<DataChunk> scan_chunk;
		idx_t row_index;

	public:
		DUCKDB_API void Next();

		DUCKDB_API ColumnDataChunkIterator &operator++();
		DUCKDB_API bool operator!=(const ColumnDataChunkIterator &other) const;
		DUCKDB_API DataChunk &operator*() const;
	};

public:
	ColumnDataChunkIterator begin() {
		return ColumnDataChunkIterator(&collection, column_ids);
	}
	ColumnDataChunkIterator end() {
		return ColumnDataChunkIterator(nullptr, vector<column_t>());
	}
};

class ColumnDataRowIterationHelper {
public:
	DUCKDB_API ColumnDataRowIterationHelper(const ColumnDataCollection &collection);

private:
	const ColumnDataCollection &collection;

private:
	class ColumnDataRowIterator;

	class ColumnDataRowIterator {
	public:
		DUCKDB_API explicit ColumnDataRowIterator(const ColumnDataCollection *collection_p);

		const ColumnDataCollection *collection;
		ColumnDataScanState scan_state;
		shared_ptr<DataChunk> scan_chunk;
		ColumnDataRow current_row;

	public:
		void Next();

		DUCKDB_API ColumnDataRowIterator &operator++();
		DUCKDB_API bool operator!=(const ColumnDataRowIterator &other) const;
		DUCKDB_API const ColumnDataRow &operator*() const;
	};

public:
	DUCKDB_API ColumnDataRowIterator begin();
	DUCKDB_API ColumnDataRowIterator end();
};

} // namespace duckdb
