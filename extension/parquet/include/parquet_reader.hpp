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

#include <exception>

namespace duckdb {
class ClientContext;
class RleBpDecoder;
class ChunkCollection;

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
};

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
	void Initialize(ParquetReaderScanState &state, vector<column_t> column_ids, vector<idx_t> groups_to_read);
	void ReadChunk(ParquetReaderScanState &state, DataChunk &output);

	idx_t NumRows();
	idx_t NumRowGroups();

	const parquet::format::FileMetaData *GetFileMetadata();

private:
	const parquet::format::RowGroup &GetGroup(ParquetReaderScanState &state);
	void PrepareChunkBuffer(ParquetReaderScanState &state, idx_t col_idx);
	bool PreparePageBuffers(ParquetReaderScanState &state, idx_t col_idx);
	void VerifyString(LogicalTypeId id, const char *str_data, idx_t str_len);

	template <typename... Args> std::runtime_error FormatException(const string fmt_str, Args... params) {
		return std::runtime_error("Failed to read Parquet file \"" + file_name +
		                          "\": " + StringUtil::Format(fmt_str, params...));
	}

	template <class T>
	void fill_from_dict(ParquetReaderColumnData &col_data, idx_t count, Vector &target, idx_t target_offset);
	template <class T>
	void fill_from_plain(ParquetReaderColumnData &col_data, idx_t count, Vector &target, idx_t target_offset);

private:
	ClientContext &context;
};

} // namespace duckdb
