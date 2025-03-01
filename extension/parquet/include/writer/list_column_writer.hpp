//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/list_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"

namespace duckdb {

class ListColumnWriterState : public ColumnWriterState {
public:
	ListColumnWriterState(duckdb_parquet::RowGroup &row_group, idx_t col_idx) : row_group(row_group), col_idx(col_idx) {
	}
	~ListColumnWriterState() override = default;

	duckdb_parquet::RowGroup &row_group;
	idx_t col_idx;
	unique_ptr<ColumnWriterState> child_state;
	idx_t parent_index = 0;
};

class ListColumnWriter : public ColumnWriter {
public:
	ListColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p, idx_t max_repeat,
	                 idx_t max_define, unique_ptr<ColumnWriter> child_writer_p, bool can_have_nulls)
	    : ColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls),
	      child_writer(std::move(child_writer_p)) {
	}
	~ListColumnWriter() override = default;

	unique_ptr<ColumnWriter> child_writer;

public:
	unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::RowGroup &row_group) override;
	bool HasAnalyze() override;
	void Analyze(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;
	void FinalizeAnalyze(ColumnWriterState &state) override;
	void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;

	void BeginWrite(ColumnWriterState &state) override;
	void Write(ColumnWriterState &state, Vector &vector, idx_t count) override;
	void FinalizeWrite(ColumnWriterState &state) override;
};

} // namespace duckdb
