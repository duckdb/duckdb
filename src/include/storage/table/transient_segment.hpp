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
class ColumnData;

class TransientSegment : public ColumnSegment {
public:
	TransientSegment(ColumnData& column_data, BufferManager &manager, TypeId type, index_t start);
	//! The column this segment belongs to
	ColumnData &column_data;
	//! The buffer manager
	BufferManager &manager;
	//! The uncompressed segment holding the data
	unique_ptr<UncompressedSegment> data;
public:
	void InitializeScan(TransientScanState &state);
	//! Scan one vector from this transient segment
	void Scan(Transaction &transaction, TransientScanState &state, index_t vector_index, Vector &result);
	//! Scan one vector from this transient segment, throwing an exception if there are any outstanding updates
	void IndexScan(TransientScanState &state, Vector &result);

	//! Fetch the base table vector index that belongs to this row
	void Fetch(TransientScanState &state, index_t vector_index, Vector &result);
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(FetchState &state, Transaction &transaction, row_t row_id, Vector &result);

	//! Initialize an append of this transient segment
	void InitializeAppend(TransientAppendState &state);
	//! Appends a (part of) vector to the transient segment, returns the amount of entries successfully appended
	index_t Append(TransientAppendState &state, Vector &data, index_t offset, index_t count);

	//! Perform an update within the transient segment
	void Update(Transaction &transaction, Vector &updates, row_t *ids);
};

} // namespace duckdb
