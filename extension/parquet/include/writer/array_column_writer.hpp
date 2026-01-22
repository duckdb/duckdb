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
	ArrayColumnWriter(ParquetWriter &writer, ParquetColumnSchema &&column_schema, vector<string> schema_path_p,
	                  unique_ptr<ColumnWriter> child_writer_p)
	    : ListColumnWriter(writer, std::move(column_schema), std::move(schema_path_p), std::move(child_writer_p)) {
	}
	~ArrayColumnWriter() override = default;

public:
	void Analyze(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;
	void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count,
	             bool vector_can_span_multiple_pages) override;
	void Write(ColumnWriterState &state, Vector &vector, idx_t count) override;

protected:
	void WriteArrayState(ListColumnWriterState &state, idx_t array_size, uint16_t first_repeat_level,
	                     idx_t define_value, const bool is_empty = false);
};

} // namespace duckdb
