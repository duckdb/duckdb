//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/common.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#endif

#include "parquet_statistics.hpp"
#include "column_writer.hpp"
#include "parquet_types.h"
#include "geo_parquet.hpp"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {
class FileSystem;
class FileOpener;
class ParquetEncryptionConfig;

class Serializer;
class Deserializer;

struct CopyFunctionFileStatistics;

struct PreparedRowGroup {
	duckdb_parquet::RowGroup row_group;
	vector<unique_ptr<ColumnWriterState>> states;
};

struct FieldID;
struct ChildFieldIDs {
	ChildFieldIDs();
	ChildFieldIDs Copy() const;
	unique_ptr<case_insensitive_map_t<FieldID>> ids;

	void Serialize(Serializer &serializer) const;
	static ChildFieldIDs Deserialize(Deserializer &source);
};

struct FieldID {
	static constexpr const auto DUCKDB_FIELD_ID = "__duckdb_field_id";
	FieldID();
	explicit FieldID(int32_t field_id);
	FieldID Copy() const;
	bool set;
	int32_t field_id;
	ChildFieldIDs child_field_ids;

	void Serialize(Serializer &serializer) const;
	static FieldID Deserialize(Deserializer &source);
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
	              const vector<pair<string, string>> &kv_metadata,
	              shared_ptr<ParquetEncryptionConfig> encryption_config, idx_t dictionary_size_limit,
	              idx_t string_dictionary_page_size_limit, double bloom_filter_false_positive_ratio,
	              int64_t compression_level, bool debug_use_openssl, ParquetVersion parquet_version);

public:
	void PrepareRowGroup(ColumnDataCollection &buffer, PreparedRowGroup &result);
	void FlushRowGroup(PreparedRowGroup &row_group);
	void Flush(ColumnDataCollection &buffer);
	void Finalize();

	static duckdb_parquet::Type::type DuckDBTypeToParquetType(const LogicalType &duckdb_type);
	static void SetSchemaProperties(const LogicalType &duckdb_type, duckdb_parquet::SchemaElement &schema_ele);

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
		lock_guard<mutex> glock(lock);
		return writer->total_written;
	}
	idx_t DictionarySizeLimit() const {
		return dictionary_size_limit;
	}
	idx_t StringDictionaryPageSizeLimit() const {
		return string_dictionary_page_size_limit;
	}
	double BloomFilterFalsePositiveRatio() const {
		return bloom_filter_false_positive_ratio;
	}
	int64_t CompressionLevel() const {
		return compression_level;
	}
	idx_t NumberOfRowGroups() {
		lock_guard<mutex> glock(lock);
		return file_meta_data.row_groups.size();
	}
	ParquetVersion GetParquetVersion() const {
		return parquet_version;
	}

	uint32_t Write(const duckdb_apache::thrift::TBase &object);
	uint32_t WriteData(const const_data_ptr_t buffer, const uint32_t buffer_size);

	GeoParquetFileMetadata &GetGeoParquetData();

	static bool TryGetParquetType(const LogicalType &duckdb_type,
	                              optional_ptr<duckdb_parquet::Type::type> type = nullptr);

	void BufferBloomFilter(idx_t col_idx, unique_ptr<ParquetBloomFilter> bloom_filter);
	void SetWrittenStatistics(CopyFunctionFileStatistics &written_stats);

private:
	void GatherWrittenStatistics();

private:
	ClientContext &context;
	string file_name;
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::CompressionCodec::type codec;
	ChildFieldIDs field_ids;
	shared_ptr<ParquetEncryptionConfig> encryption_config;
	idx_t dictionary_size_limit;
	idx_t string_dictionary_page_size_limit;
	double bloom_filter_false_positive_ratio;
	int64_t compression_level;
	bool debug_use_openssl;
	shared_ptr<EncryptionUtil> encryption_util;
	ParquetVersion parquet_version;
	vector<ParquetColumnSchema> column_schemas;

	unique_ptr<BufferedFileWriter> writer;
	std::shared_ptr<duckdb_apache::thrift::protocol::TProtocol> protocol;
	duckdb_parquet::FileMetaData file_meta_data;
	std::mutex lock;

	vector<unique_ptr<ColumnWriter>> column_writers;

	unique_ptr<GeoParquetFileMetadata> geoparquet_data;
	vector<ParquetBloomFilterEntry> bloom_filters;

	optional_ptr<CopyFunctionFileStatistics> written_stats;
};

} // namespace duckdb
