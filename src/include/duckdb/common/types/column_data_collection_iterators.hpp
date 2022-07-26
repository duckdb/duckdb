//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column_data_collection_iterators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column_data_scan_states.hpp"

namespace duckdb {
class ColumnDataCollection;

class ColumnDataChunkIterationHelper {
public:
	ColumnDataChunkIterationHelper(const ColumnDataCollection &collection, vector<column_t> column_ids);

private:
	const ColumnDataCollection &collection;
	vector<column_t> column_ids;

private:
	class ColumnDataChunkIterator;

	class ColumnDataChunkIterator {
	public:
		explicit ColumnDataChunkIterator(const ColumnDataCollection *collection_p, vector<column_t> column_ids);

		const ColumnDataCollection *collection;
		ColumnDataScanState scan_state;
		shared_ptr<DataChunk> scan_chunk;
		idx_t row_index;

	public:
		void Next();

		ColumnDataChunkIterator &operator++();
		bool operator!=(const ColumnDataChunkIterator &other) const;
		DataChunk &operator*() const;
	};

public:
	DUCKDB_API ColumnDataChunkIterator begin() {
		return ColumnDataChunkIterator(&collection, column_ids);
	}
	DUCKDB_API ColumnDataChunkIterator end() {
		return ColumnDataChunkIterator(nullptr, vector<column_t>());
	}
};

class ColumnDataRowIterationHelper {
public:
	ColumnDataRowIterationHelper(const ColumnDataCollection &collection);

private:
	const ColumnDataCollection &collection;

private:
	class ColumnDataRowIterator;

	class ColumnDataRowIterator {
	public:
		explicit ColumnDataRowIterator(const ColumnDataCollection *collection_p);

		const ColumnDataCollection *collection;
		ColumnDataScanState scan_state;
		shared_ptr<DataChunk> scan_chunk;
		ColumnDataRow current_row;

	public:
		void Next();

		ColumnDataRowIterator &operator++();
		bool operator!=(const ColumnDataRowIterator &other) const;
		const ColumnDataRow &operator*() const;
	};

public:
	DUCKDB_API ColumnDataRowIterator begin() {
		return ColumnDataRowIterator(&collection);
	}
	DUCKDB_API ColumnDataRowIterator end() {
		return ColumnDataRowIterator(nullptr);
	}
};

} // namespace duckdb
