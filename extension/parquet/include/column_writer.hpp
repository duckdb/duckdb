//===----------------------------------------------------------------------===//
//                         DuckDB
//
// column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_types.h"

namespace duckdb {
class BufferedSerializer;
class ParquetWriter;

class ColumnWriterState {
public:
	static constexpr const uint16_t DEFINE_VALID = 65535;

public:
	virtual ~ColumnWriterState();

	vector<uint16_t> definition_levels;
	vector<uint16_t> repetition_levels;
};

class ColumnWriter {
	//! We limit uncompressed pages to 1B bytes
	//! This is because Parquet limits pages to 2^31 bytes as they use an int32 to represent page size
	//! Since compressed page size can theoretically be larger than uncompressed page size
	//! We conservatively choose to limit it to around half of this
	static constexpr const idx_t MAX_UNCOMPRESSED_PAGE_SIZE = 1000000000;

public:
	ColumnWriter(ParquetWriter &writer, idx_t schema_idx, idx_t max_repeat, idx_t max_define);
	virtual ~ColumnWriter();

	ParquetWriter &writer;
	idx_t schema_idx;
	idx_t max_repeat;
	idx_t max_define;

public:
	static unique_ptr<ColumnWriter> CreateWriterRecursive(vector<duckdb_parquet::format::SchemaElement> &schemas,
	                                                      ParquetWriter &writer, const LogicalType &type,
	                                                      const string &name, idx_t max_repeat = 0,
	                                                      idx_t max_define = 1);

	virtual unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::format::RowGroup &row_group,
	                                                           vector<string> schema_path);
	virtual void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count);

	virtual void BeginWrite(ColumnWriterState &state);
	virtual void Write(ColumnWriterState &state, Vector &vector, idx_t count);
	virtual void FinalizeWrite(ColumnWriterState &state);

protected:
	void HandleDefineLevels(ColumnWriterState &state, ColumnWriterState *parent, ValidityMask &validity, idx_t count,
	                        uint16_t define_value, uint16_t null_value);
	void WriteLevels(Serializer &temp_writer, const vector<uint16_t> &levels, idx_t max_value, idx_t start_offset,
	                 idx_t count);

	void NextPage(ColumnWriterState &state_p);
	void FlushPage(ColumnWriterState &state_p);

	virtual idx_t GetRowSize(Vector &vector, idx_t index) = 0;
	virtual void WriteVector(Serializer &temp_writer, Vector &vector, idx_t chunk_start, idx_t chunk_end) = 0;

	void CompressPage(BufferedSerializer &temp_writer, size_t &compressed_size, data_ptr_t &compressed_data,
	                  unique_ptr<data_t[]> &compressed_buf);
};

} // namespace duckdb
