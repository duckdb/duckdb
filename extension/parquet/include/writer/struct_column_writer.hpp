//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/struct_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"

namespace duckdb {

class StructColumnWriter : public ColumnWriter {
public:
	StructColumnWriter(ParquetWriter &writer, ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                   vector<unique_ptr<ColumnWriter>> child_writers_p)
	    : ColumnWriter(writer, column_schema, std::move(schema_path_p)) {
		child_writers = std::move(child_writers_p);
	}
	~StructColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::RowGroup &row_group) override;
	bool HasAnalyze() override;
	void Analyze(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;
	void FinalizeAnalyze(ColumnWriterState &state) override;
	void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count,
	             bool vector_can_span_multiple_pages) override;

	void BeginWrite(ColumnWriterState &state) override;
	void Write(ColumnWriterState &state, Vector &vector, idx_t count) override;
	void FinalizeWrite(ColumnWriterState &state) override;
	void FinalizeSchema(vector<duckdb_parquet::SchemaElement> &schemas) override;
};

} // namespace duckdb
