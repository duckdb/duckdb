//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/enum_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "writer/primitive_column_writer.hpp"

namespace duckdb {
class EnumWriterPageState;

class EnumColumnWriter : public PrimitiveColumnWriter {
public:
	EnumColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                 bool can_have_nulls);
	~EnumColumnWriter() override = default;

	uint32_t bit_width;

public:
	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override;

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p, ColumnWriterPageState *page_state_p,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override;

	unique_ptr<ColumnWriterPageState> InitializePageState(PrimitiveColumnWriterState &state, idx_t page_idx) override;

	void FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) override;

	duckdb_parquet::Encoding::type GetEncoding(PrimitiveColumnWriterState &state) override;

	bool HasDictionary(PrimitiveColumnWriterState &state) override;

	idx_t DictionarySize(PrimitiveColumnWriterState &state_p) override;

	void FlushDictionary(PrimitiveColumnWriterState &state, ColumnWriterStatistics *stats_p) override;

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state) const override;

private:
	template <class T>
	void WriteEnumInternal(WriteStream &temp_writer, Vector &input_column, idx_t chunk_start, idx_t chunk_end,
	                       EnumWriterPageState &page_state);
};

} // namespace duckdb
