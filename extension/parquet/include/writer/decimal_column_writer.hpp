//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/decimal_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "writer/primitive_column_writer.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "writer/parquet_write_stats.hpp"

namespace duckdb {
class ColumnWriterPageState;
class ParquetWriter;
class Vector;
class WriteStream;
struct ParquetColumnSchema;

class FixedDecimalColumnWriter : public PrimitiveColumnWriter {
public:
	FixedDecimalColumnWriter(ParquetWriter &writer, ParquetColumnSchema &&column_schema, vector<string> schema_path_p);
	~FixedDecimalColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override;

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p, ColumnWriterPageState *page_state,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override;

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state) const override;
};

} // namespace duckdb
