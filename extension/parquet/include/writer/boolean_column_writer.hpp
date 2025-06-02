//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/boolean_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "writer/primitive_column_writer.hpp"

namespace duckdb {

class BooleanColumnWriter : public PrimitiveColumnWriter {
public:
	BooleanColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                    bool can_have_nulls);
	~BooleanColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override;

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p, ColumnWriterPageState *state_p,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override;

	unique_ptr<ColumnWriterPageState> InitializePageState(PrimitiveColumnWriterState &state, idx_t page_idx) override;
	void FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) override;

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state) const override;
};

} // namespace duckdb
