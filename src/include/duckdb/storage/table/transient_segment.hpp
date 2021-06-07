//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/transient_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {
struct ColumnAppendState;
class DatabaseInstance;
class PersistentSegment;

class TransientSegment : public ColumnSegment {
public:
	TransientSegment(DatabaseInstance &db, const LogicalType &type, idx_t start);

public:
	//! Initialize an append of this transient segment
	void InitializeAppend(ColumnAppendState &state);
	//! Appends a (part of) vector to the transient segment, returns the amount of entries successfully appended
	idx_t Append(ColumnAppendState &state, VectorData &data, idx_t offset, idx_t count);
	//! Revert an append made to this transient segment
	void RevertAppend(idx_t start_row);
};

} // namespace duckdb
