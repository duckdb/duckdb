//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct HeapMetaData {
	//! Meta data for 'row_count' rows starting at row 'row_offset' of the TupleRowDataBlock this belongs to
	uint32_t row_offset;
	uint32_t row_count;
	//! Index of the heap block that stores the heap data for the rows
	uint32_t heap_block_id;
	//! Offset into the heap block
	uint32_t heap_block_offset;
};

//! TupleRowDataBlock stores the fixed-size rows of a TupleDataCollection
class TupleRowDataBlock {
public:
	TupleRowDataBlock(shared_ptr<BlockHandle> &&handle_p, const RowLayout &layout_p)
	    : handle(move(handle_p)), layout(layout_p), count(0) {
		D_ASSERT(handle->GetMemoryUsage() == Storage::BLOCK_ALLOC_SIZE);
	}

	//! How many more rows can fit in this block
	uint32_t RemainingCapacity() const {
		uint32_t capacity = Storage::BLOCK_ALLOC_SIZE / layout.GetRowWidth();
		D_ASSERT(count <= capacity);
		return capacity - count;
	}

public:
	//! The underlying block handle
	shared_ptr<BlockHandle> handle;
	//! The layout of the rows stored in this block
	const RowLayout &layout;
	//! The number of rows stored in this block
	uint32_t count;
	//! Meta data about the heap data corresponding to the rows in this block
	vector<HeapMetaData> heap_data;
};

//! TupleHeapDataBlock stores the variable-size data of the rows in a TupleRowDataBlock
class TupleHeapDataBlock {
public:
	TupleHeapDataBlock(shared_ptr<BlockHandle> &&handle_p, uint32_t capacity_p)
	    : handle(move(handle_p)), capacity(capacity_p), size(0) {
		D_ASSERT(handle->GetMemoryUsage() >= capacity_p);
	}

	//! How much space there is left to write to in this block
	uint32_t RemainingCapacity() const {
		D_ASSERT(size <= capacity);
		return capacity - size;
	}

public:
	//! The underlying block handle
	shared_ptr<BlockHandle> handle;
	//! How much space is available in the block
	uint32_t capacity;
	//! How much space is currently used within the block
	uint32_t size;
};

} // namespace duckdb
