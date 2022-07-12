//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_data_collection_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/row_layout.hpp"

namespace duckdb {

class BufferHandle;
class RowDataCollection;
class DataChunk;

//! Used to scan the data into DataChunks after sorting
struct RowDataCollectionScanner {
public:
	using Types = vector<LogicalType>;

	struct ScanState {
		explicit ScanState(const RowDataCollectionScanner &scanner_p) : scanner(scanner_p), block_idx(0), entry_idx(0) {
		}

		void PinData();

		//! The data layout
		const RowDataCollectionScanner &scanner;

		idx_t block_idx;
		idx_t entry_idx;

		BufferHandle data_handle;
		BufferHandle heap_handle;
	};

	//! Ensure that heap blocks correspond to row blocks
	static void SwizzleBlocks(RowDataCollection &swizzled_block_collection, RowDataCollection &swizzled_string_heap,
	                          RowDataCollection &block_collection, RowDataCollection &string_heap,
	                          const RowLayout &layout);

	RowDataCollectionScanner(RowDataCollection &rows, RowDataCollection &heap, const RowLayout &layout,
	                         bool flush = true);

	//! The type layout of the payload
	inline const vector<LogicalType> &GetTypes() const {
		return layout.GetTypes();
	}

	//! The number of rows scanned so far
	inline idx_t Scanned() const {
		return total_scanned;
	}

	//! The number of remaining rows
	inline idx_t Remaining() const {
		return total_count - total_scanned;
	}
	//! Scans the next data chunk from the sorted data
	void Scan(DataChunk &chunk);

private:
	//! The row data being scanned
	RowDataCollection &rows;
	//! The row heap being scanned
	RowDataCollection &heap;
	//! The data layout
	const RowLayout layout;
	//! Read state
	ScanState read_state;
	//! The total count of sorted_data
	const idx_t total_count;
	//! The number of rows scanned so far
	idx_t total_scanned;
	//! Addresses used to gather from the sorted data
	Vector addresses = Vector(LogicalType::POINTER);
	//! Whether to flush the blocks after scanning
	const bool flush;
};

} // namespace duckdb
