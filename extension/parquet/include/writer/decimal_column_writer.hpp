//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/decimal_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "writer/primitive_column_writer.hpp"

namespace duckdb {

class FixedDecimalColumnWriter : public PrimitiveColumnWriter {
public:
	FixedDecimalColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema,
	                         vector<string> schema_path_p, bool can_have_nulls);
	~FixedDecimalColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override;

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p, ColumnWriterPageState *page_state,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override;

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state) const override;
};

} // namespace duckdb
