//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/copy_function.hpp"

#include "parquet_statistics.hpp"
#include "column_writer.hpp"
#include "parquet_field_id.hpp"
#include "parquet_shredding.hpp"
#include "parquet_types.h"
#include "parquet_geometry.hpp"
#include "writer/parquet_write_stats.hpp"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {
class FileSystem;
class FileOpener;
class ParquetEncryptionConfig;
class ParquetStatsAccumulator;

class Serializer;
class Deserializer;

class ColumnWriterStatistics;
struct CopyFunctionFileStatistics;

struct PreparedRowGroup {
	duckdb_parquet::RowGroup row_group;
	vector<unique_ptr<ColumnWriterState>> states;
};

struct ParquetBloomFilterEntry {
	unique_ptr<ParquetBloomFilter> bloom_filter;
	idx_t row_group_idx;
	idx_t column_idx;
};

enum class ParquetVersion : uint8_t {
	V1 = 1, //! Excludes DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, BYTE_STREAM_SPLIT
	V2 = 2, //! Includes the encodings above
};

class ParquetWriter {
public:
	ParquetWriter(ClientContext &context, FileSystem &fs, string file_name, vector<LogicalType> types,
	              vector<string> names, duckdb_parquet::CompressionCodec::type codec, ChildFieldIDs field_ids,
	              ShreddingType shredding_types, const vector<pair<string, string>> &kv_metadata,
	              shared_ptr<ParquetEncryptionConfig> encryption_config, optional_idx dictionary_size_limit,
	              idx_t string_dictionary_page_size_limit, bool enable_bloom_filters,
	              double bloom_filter_false_positive_ratio, int64_t compression_level, bool debug_use_openssl,
	              ParquetVersion parquet_version, GeoParquetVersion geoparquet_version);
	~ParquetWriter();

public:
	void PrepareRowGroup(ColumnDataCollection &buffer, PreparedRowGroup &result);
	void FlushRowGroup(PreparedRowGroup &row_group);
	void Flush(ColumnDataCollection &buffer);
	void Finalize();

	static duckdb_parquet::Type::type DuckDBTypeToParquetType(const LogicalType &duckdb_type);
	static void SetSchemaProperties(const LogicalType &duckdb_type, duckdb_parquet::SchemaElement &schema_ele,
	                                bool allow_geometry);

	ClientContext &GetContext() {
		return context;
	}
	duckdb_apache::thrift::protocol::TProtocol *GetProtocol() {
		return protocol.get();
	}
	duckdb_parquet::CompressionCodec::type GetCodec() {
		return codec;
	}
	duckdb_parquet::Type::type GetType(idx_t schema_idx) {
		return file_meta_data.schema[schema_idx].type;
	}
	LogicalType GetSQLType(idx_t schema_idx) const {
		return sql_types[schema_idx];
	}
	BufferedFileWriter &GetWriter() {
		return *writer;
	}
	idx_t FileSize() {
		return total_written;
	}
	optional_idx DictionarySizeLimit() const {
		return dictionary_size_limit;
	}
	idx_t StringDictionaryPageSizeLimit() const {
		return string_dictionary_page_size_limit;
	}
	double EnableBloomFilters() const {
		return enable_bloom_filters;
	}
	double BloomFilterFalsePositiveRatio() const {
		return bloom_filter_false_positive_ratio;
	}
	int64_t CompressionLevel() const {
		return compression_level;
	}
	idx_t NumberOfRowGroups() {
		return num_row_groups;
	}
	ParquetVersion GetParquetVersion() const {
		return parquet_version;
	}
	GeoParquetVersion GetGeoParquetVersion() const {
		return geoparquet_version;
	}
	const string &GetFileName() const {
		return file_name;
	}

	uint32_t Write(const duckdb_apache::thrift::TBase &object);
	uint32_t WriteData(const const_data_ptr_t buffer, const uint32_t buffer_size);

	GeoParquetFileMetadata &GetGeoParquetData();

	static bool TryGetParquetType(const LogicalType &duckdb_type,
	                              optional_ptr<duckdb_parquet::Type::type> type = nullptr);

	void BufferBloomFilter(idx_t col_idx, unique_ptr<ParquetBloomFilter> bloom_filter);
	void SetWrittenStatistics(CopyFunctionFileStatistics &written_stats);
	void FlushColumnStats(idx_t col_idx, duckdb_parquet::ColumnChunk &chunk,
	                      optional_ptr<ColumnWriterStatistics> writer_stats);

private:
	void GatherWrittenStatistics();

private:
	ClientContext &context;
	string file_name;
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::CompressionCodec::type codec;
	ChildFieldIDs field_ids;
	ShreddingType shredding_types;
	shared_ptr<ParquetEncryptionConfig> encryption_config;
	optional_idx dictionary_size_limit;
	idx_t string_dictionary_page_size_limit;
	bool enable_bloom_filters;
	double bloom_filter_false_positive_ratio;
	int64_t compression_level;
	bool debug_use_openssl;
	shared_ptr<EncryptionUtil> encryption_util;
	ParquetVersion parquet_version;
	GeoParquetVersion geoparquet_version;
	vector<ParquetColumnSchema> column_schemas;

	unique_ptr<BufferedFileWriter> writer;
	//! Atomics to reduce contention when rotating writes to multiple Parquet files
	atomic<idx_t> total_written;
	atomic<idx_t> num_row_groups;
	std::shared_ptr<duckdb_apache::thrift::protocol::TProtocol> protocol;
	duckdb_parquet::FileMetaData file_meta_data;
	std::mutex lock;

	vector<unique_ptr<ColumnWriter>> column_writers;

	unique_ptr<GeoParquetFileMetadata> geoparquet_data;
	vector<ParquetBloomFilterEntry> bloom_filters;

	optional_ptr<CopyFunctionFileStatistics> written_stats;
	unique_ptr<ParquetStatsAccumulator> stats_accumulator;
};

} // namespace duckdb
