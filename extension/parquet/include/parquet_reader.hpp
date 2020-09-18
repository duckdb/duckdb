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
};

struct ParquetReaderScanState {
	vector<idx_t> group_idx_list;
	int64_t current_group;
	idx_t group_offset;
	vector<unique_ptr<ParquetReaderColumnData>> column_data;
	bool finished;
};

class ParquetReader {
public:
	ParquetReader(ClientContext &context, string file_name, vector<LogicalType> expected_types);
	ParquetReader(ClientContext &context, string file_name) :
		ParquetReader(context, file_name, vector<LogicalType>()) {}
	~ParquetReader();

	string file_name;
	vector<LogicalType> return_types;
	vector<string> names;
	vector<column_t> column_ids;
public:
	void Initialize(ParquetReaderScanState &state, vector<idx_t> groups_to_read);
	void ReadChunk(ParquetReaderScanState &state, DataChunk &output);

	idx_t NumRows();
	idx_t NumRowGroups();
private:
	parquet::format::RowGroup &GetGroup(ParquetReaderScanState &state);
	void PrepareChunkBuffer(ParquetReaderScanState &state, idx_t col_idx);
	bool PreparePageBuffers(ParquetReaderScanState &state, idx_t col_idx);

	template <typename... Args> std::runtime_error FormatException(const string fmt_str, Args... params) {
		return std::runtime_error("Failed to read Parquet file \"" + file_name +
		                     "\": " + StringUtil::Format(fmt_str, params...));
	}

	template <class T>
	void fill_from_dict(ParquetReaderColumnData &col_data, idx_t count, Vector &target, idx_t target_offset);
	template <class T>
	void fill_from_plain(ParquetReaderColumnData &col_data, idx_t count, Vector &target, idx_t target_offset);
private:
	static constexpr uint8_t GZIP_HEADER_MINSIZE = 10;
	static constexpr uint8_t GZIP_COMPRESSION_DEFLATE = 0x08;
	static constexpr unsigned char GZIP_FLAG_UNSUPPORTED = 0x1 | 0x2 | 0x4 | 0x10 | 0x20;

	ClientContext &context;
	parquet::format::FileMetaData file_meta_data;
};

}
