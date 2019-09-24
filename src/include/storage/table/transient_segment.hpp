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
#include "storage/uncompressed_segment.hpp"

namespace duckdb {
struct TransientAppendState;

class TransientSegment : public ColumnSegment {
public:
	TransientSegment(BufferManager &manager, TypeId type, index_t start);

	//! The buffer manager
	BufferManager &manager;
	//! The uncompressed segment holding the data
	UncompressedSegment data;
public:
	//! Initialize a scan of this transient segment
	void InitializeScan(TransientScanState &state);
	//! Scan one vector from this transient segment
	void Scan(Transaction &transaction, TransientScanState &state, Vector &result);

	//! Initialize an append of this transient segment
	void InitializeAppend(TransientAppendState &state);
	//! Appends a (part of) vector to the transient segment, returns the amount of entries successfully appended
	index_t Append(TransientAppendState &state, Vector &data, index_t offset, index_t count);


	// void Fetch(Vector &result, index_t row_id) override;
	// //! Updates the value of the segment at the specified row_id
	// void Update(index_t row_id, data_ptr_t data);
};

} // namespace duckdb
