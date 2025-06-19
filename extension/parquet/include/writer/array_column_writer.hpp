//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/array_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "writer/list_column_writer.hpp"

namespace duckdb {

class ArrayColumnWriter : public ListColumnWriter {
public:
	ArrayColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                  unique_ptr<ColumnWriter> child_writer_p, bool can_have_nulls)
	    : ListColumnWriter(writer, column_schema, std::move(schema_path_p), std::move(child_writer_p), can_have_nulls) {
	}
	~ArrayColumnWriter() override = default;

public:
	void Analyze(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;
	void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;
	void Write(ColumnWriterState &state, Vector &vector, idx_t count) override;
};

} // namespace duckdb
