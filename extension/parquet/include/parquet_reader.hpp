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
#include "column_reader.hpp"

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

struct ParquetReaderScanState {
	vector<idx_t> group_idx_list;
	int64_t current_group;
	vector<column_t> column_ids;
	idx_t group_offset;
	vector<unique_ptr<ColumnReader>> column_readers;

	bool finished;
	TableFilterSet *filters;
	SelectionVector sel;
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

	unique_ptr<apache::thrift::protocol::TProtocol> thrift_file_proto;
	unique_ptr<apache::thrift::protocol::TProtocol> thrift_memory_proto;

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
	bool ScanInternal(ParquetReaderScanState &state, DataChunk &output);

	const parquet::format::RowGroup &GetGroup(ParquetReaderScanState &state);
	void PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t col_idx, LogicalType &type);

	template <typename... Args> std::runtime_error FormatException(const string fmt_str, Args... params) {
		return std::runtime_error("Failed to read Parquet file \"" + file_name +
		                          "\": " + StringUtil::Format(fmt_str, params...));
	}

private:
	ClientContext &context;
};

} // namespace duckdb
