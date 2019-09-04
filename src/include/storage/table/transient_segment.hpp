//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/transient_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/table/column_segment.hpp"
#include "storage/block.hpp"
#include "storage/buffer_manager.hpp"
namespace duckdb {


class TransientSegment : public ColumnSegment {
public:
	TransientSegment(BufferManager &manager, TypeId type, index_t start);

	//! The buffer manager
	BufferManager &manager;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The amount of vectors stored in this transient segment
	index_t vector_count;
public:
	void Scan(ColumnPointer &pointer, Vector &result, index_t count) override;
	void Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector, index_t sel_count) override;
	void Fetch(Vector &result, index_t row_id) override;
	//! Appends a (part of) vector to the transient segment, returns the amount of entries successfully appended
	index_t Append(Vector &data, index_t offset, index_t count);
	//! Updates the value of the segment at the specified row_id
	void Update(index_t row_id, data_ptr_t data);
};

} // namespace duckdb
