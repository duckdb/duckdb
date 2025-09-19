//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/variant_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"

namespace duckdb {

class VariantColumnWriterState : public ColumnWriterState {
public:
	VariantColumnWriterState(duckdb_parquet::RowGroup &row_group, idx_t col_idx)
	    : row_group(row_group), col_idx(col_idx) {
	}
	~VariantColumnWriterState() override = default;

	duckdb_parquet::RowGroup &row_group;
	idx_t col_idx;
	unique_ptr<ColumnWriterState> child_state;
	idx_t parent_index = 0;
};

class VariantColumnWriter : public ColumnWriter {
public:
	VariantColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                    vector<unique_ptr<ColumnWriter>> child_writers_p, bool can_have_nulls)
	    : ColumnWriter(writer, column_schema, std::move(schema_path_p), can_have_nulls),
	      child_writers(std::move(child_writers_p)) {
	}
	~VariantColumnWriter() override = default;

	vector<unique_ptr<ColumnWriter>> child_writers;

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
};

} // namespace duckdb
