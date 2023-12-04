//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/row_data_collection_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/row/row_layout.hpp"

namespace duckdb {

class BufferHandle;
class RowDataCollection;
struct RowDataBlock;
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

		// We must pin ALL blocks we are going to gather from
		vector<BufferHandle> pinned_blocks;
	};

	//! Ensure that heap blocks correspond to row blocks
	static void AlignHeapBlocks(RowDataCollection &dst_block_collection, RowDataCollection &dst_string_heap,
	                            RowDataCollection &src_block_collection, RowDataCollection &src_string_heap,
	                            const RowLayout &layout);

	RowDataCollectionScanner(RowDataCollection &rows, RowDataCollection &heap, const RowLayout &layout, bool external,
	                         bool flush = true);

	//	Single block scan
	RowDataCollectionScanner(RowDataCollection &rows, RowDataCollection &heap, const RowLayout &layout, bool external,
	                         idx_t block_idx, bool flush);

	//! The type layout of the payload
	inline const vector<LogicalType> &GetTypes() const {
		return layout.GetTypes();
	}

	//! The number of rows in the collection
	inline idx_t Count() const {
		return total_count;
	}

	//! The number of rows scanned so far
	inline idx_t Scanned() const {
		return total_scanned;
	}

	//! The number of remaining rows
	inline idx_t Remaining() const {
		return total_count - total_scanned;
	}

	//! The number of remaining rows
	inline idx_t BlockIndex() const {
		return read_state.block_idx;
	}

	//! Swizzle the blocks for external scanning
	//! Swizzling is all or nothing, so if we have scanned previously,
	//! we need to re-swizzle.
	void ReSwizzle();

	void SwizzleBlock(RowDataBlock &data_block, RowDataBlock &heap_block);

	//! Scans the next data chunk from the sorted data
	void Scan(DataChunk &chunk);

	//! Resets to the start and updates the flush flag
	void Reset(bool flush = true);

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
	idx_t total_count;
	//! The number of rows scanned so far
	idx_t total_scanned;
	//! Addresses used to gather from the sorted data
	Vector addresses = Vector(LogicalType::POINTER);
	//! Whether the blocks can be flushed to disk
	const bool external;
	//! Whether to flush the blocks after scanning
	bool flush;
	//! Whether we are unswizzling the blocks
	const bool unswizzling;

	//! Checks that the newest block is valid
	void ValidateUnscannedBlock() const;
};

} // namespace duckdb
