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
	StructColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p, idx_t max_repeat,
	                   idx_t max_define, vector<unique_ptr<ColumnWriter>> child_writers_p, bool can_have_nulls)
	    : ColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls),
	      child_writers(std::move(child_writers_p)) {
	}
	~StructColumnWriter() override = default;

	vector<unique_ptr<ColumnWriter>> child_writers;

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
