//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "resizable_buffer.hpp"

#include "parquet_file_metadata_cache.hpp"
#include "parquet_types.h"
#include "parquet_rle_bp_decoder.hpp"

#include <exception>

namespace parquet {
namespace format {
class FileMetaData;
}
} // namespace parquet

namespace duckdb {
class ClientContext;
class ChunkCollection;
class BaseStatistics;
struct TableFilterSet;

struct ParquetReaderColumnData {
	~ParquetReaderColumnData();

	idx_t chunk_offset;

	idx_t page_offset;
	idx_t page_value_count = 0;

	idx_t dict_size;

	uint8_t byte_pos = 0; // to decode plain booleans from bit fields

	ResizeableBuffer buf;
	ResizeableBuffer decompressed_buf; // only used for compressed files
	ResizeableBuffer dict;
	ResizeableBuffer offset_buf;
	ResizeableBuffer defined_buf;

	ByteBuffer payload;

	parquet::format::Encoding::type page_encoding;
	// these point into buf or decompressed_buf
	unique_ptr<RleBpDecoder> defined_decoder;
	unique_ptr<RleBpDecoder> dict_decoder;

	unique_ptr<ChunkCollection> string_collection;

	bool has_nulls;
};

struct ParquetReaderScanState {
	vector<idx_t> group_idx_list;
	int64_t current_group;
	vector<column_t> column_ids;
	idx_t group_offset;
	vector<unique_ptr<ParquetReaderColumnData>> column_data;
	bool finished;
	TableFilterSet *filters;
	SelectionVector sel;
};

typedef nullmask_t parquet_filter_t;

class ParquetReader {
public:
	ParquetReader(ClientContext &context, string file_name, vector<LogicalType> expected_types,
	              string initial_filename = string());
	ParquetReader(ClientContext &context, string file_name) : ParquetReader(context, file_name, vector<LogicalType>()) {
	}
	~ParquetReader();

	string file_name;
	vector<LogicalType> return_types;
	vector<string> names;

	shared_ptr<ParquetFileMetadataCache> metadata;

public:
	void Initialize(ParquetReaderScanState &state, vector<column_t> column_ids, vector<idx_t> groups_to_read,
	                TableFilterSet *table_filters);
	void Scan(ParquetReaderScanState &state, DataChunk &output);

	idx_t NumRows();
	idx_t NumRowGroups();

	const parquet::format::FileMetaData *GetFileMetadata();

	static unique_ptr<BaseStatistics> ReadStatistics(LogicalType &type, column_t column_index,
	                                                 const parquet::format::FileMetaData *file_meta_data);

private:
	void ScanColumn(ParquetReaderScanState &state, parquet_filter_t &filter_mask, idx_t count, idx_t out_col_idx,
	                Vector &out);
	bool ScanInternal(ParquetReaderScanState &state, DataChunk &output);

	const parquet::format::RowGroup &GetGroup(ParquetReaderScanState &state);
	void PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t col_idx, LogicalType &type);
	bool PreparePageBuffers(ParquetReaderScanState &state, idx_t col_idx);
	void VerifyString(LogicalTypeId id, const char *str_data, idx_t str_len);

	template <typename... Args> std::runtime_error FormatException(const string fmt_str, Args... params) {
		return std::runtime_error("Failed to read Parquet file \"" + file_name +
		                          "\": " + StringUtil::Format(fmt_str, params...));
	}

private:
	ClientContext &context;
};

} // namespace duckdb
