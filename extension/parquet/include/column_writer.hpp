//===----------------------------------------------------------------------===//
//                         DuckDB
//
// column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ColumnWriter {
public:
	ColumnWriter();
	virtual ~ColumnWriter();

public:
	virtual void WriteVector(Serializer &temp_writer, DataChunk &input, idx_t col_idx, idx_t chunk_start,
	            idx_t chunk_end) = 0;

	static unique_ptr<ColumnWriter> CreateWriterRecursive(const LogicalType &type);
};

} // namespace duckdb
