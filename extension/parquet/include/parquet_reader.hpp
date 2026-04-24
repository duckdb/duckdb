//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <exception>
#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "duckdb.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_adaptive_filter_cache.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "column_reader.hpp"
#include "parquet_file_metadata_cache.hpp"
#include "parquet_rle_bp_decoder.hpp"
#include "parquet_types.h"
#include "resizable_buffer.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_column_schema.hpp"
#include "thrift/protocol/TProtocol.h"

namespace duckdb_apache {
namespace thrift {
class TBase;
} // namespace thrift
} // namespace duckdb_apache

namespace duckdb_parquet {
class EncryptionAlgorithm;
class FileMetaData;
class RowGroup;
class SchemaElement;

namespace format {
class FileMetaData;
}
} // namespace duckdb_parquet

namespace duckdb {
class Allocator;
class ClientContext;
class BaseStatistics;
class TableFilterSet;
class ParquetEncryptionConfig;
class ParquetReader;
class DataChunk;
class Deserializer;
class EncryptionUtil;
class PhysicalOperator;
class Serializer;
class TableFilter;
struct CryptoMetaData;
struct GlobalTableFunctionState;
struct LocalTableFunctionState;
struct PartitionStatistics;
struct TableFilterState;

struct ParquetReaderPrefetchConfig {
	//! Percentage of data in a row group span that should be scanned for enabling whole group prefetch
	static constexpr double WHOLE_GROUP_PREFETCH_MINIMUM_SCAN = 0.95;
	//! How many row groups need to produce at least one surviving row (from filtering)
	static constexpr double WHOLE_GROUP_PREFETCH_MINIMUM_MATCH_RATIO = 0.9;
};

struct ParquetScanFilter {
	ParquetScanFilter(ClientContext &context, ProjectionIndex filter_idx, TableFilter &filter);
	~ParquetScanFilter();
	ParquetScanFilter(ParquetScanFilter &&) = default;

	ProjectionIndex filter_idx;
	TableFilter &filter;
	unique_ptr<TableFilterState> filter_state;
};

struct ParquetReaderScanState {
	vector<idx_t> group_idx_list;
	int64_t current_group;
	idx_t offset_in_group;
	idx_t group_offset;
	unique_ptr<CachingFileHandle> file_handle;
	unique_ptr<ColumnReader> root_reader;
	duckdb_base_std::unique_ptr<duckdb_apache::thrift::protocol::TProtocol> thrift_file_proto;

	bool finished;
	SelectionVector sel;

	ResizeableBuffer define_buf;
	ResizeableBuffer repeat_buf;

	bool prefetch_mode = false;
	bool current_group_prefetched = false;


	bool current_group_filter_ran = false;
	bool current_group_had_match = false;

	idx_t row_groups_executed = 0;
	idx_t row_groups_with_matches = 0;

	void FinalizeRowGroupSelectivity() {
		if (current_group_filter_ran) {
			row_groups_executed++;
			if (current_group_had_match) {
				row_groups_with_matches++;
			}
		}
		current_group_filter_ran = false;
		current_group_had_match = false;
	}

	//! Per-thread adaptive filter cache
	MultiFileAdaptiveFilterCache adaptive_filter_cache;
	//! Table filter list
	vector<ParquetScanFilter> scan_filters;

	//! (optional) pointer to the PhysicalOperator for logging
	optional_ptr<const PhysicalOperator> op;
};

struct ParquetColumnDefinition {
public:
	static ParquetColumnDefinition FromSchemaValue(ClientContext &context, const Value &column_value);

public:
	// DEPRECATED, use 'identifier' instead
	int32_t field_id;
	string name;
	LogicalType type;
	Value default_value;
	Value identifier;

public:
	void Serialize(Serializer &serializer) const;
	static ParquetColumnDefinition Deserialize(Deserializer &deserializer);
};

struct ParquetOptions {
	explicit ParquetOptions() {
	}
	explicit ParquetOptions(ClientContext &context);

	bool binary_as_string = false;
	bool variant_legacy_encoding = false;
	bool file_row_number = false;
	shared_ptr<ParquetEncryptionConfig> encryption_config;

	vector<ParquetColumnDefinition> schema;
	idx_t explicit_cardinality = 0;
	bool can_have_nan = false; // if floats or doubles can contain NaN values
};

struct ParquetOptionsSerialization {
	ParquetOptionsSerialization() = default;
	ParquetOptionsSerialization(ParquetOptions parquet_options_p, MultiFileOptions file_options_p)
	    : parquet_options(std::move(parquet_options_p)), file_options(std::move(file_options_p)) {
	}

	ParquetOptions parquet_options;
	MultiFileOptions file_options;

public:
	void Serialize(Serializer &serializer) const;
	static ParquetOptionsSerialization Deserialize(Deserializer &deserializer);
};

struct ParquetUnionData : public BaseUnionData {
	explicit ParquetUnionData(OpenFileInfo file_p) : BaseUnionData(std::move(file_p)) {
	}
	~ParquetUnionData() override;

	optional_idx TryGetCardinalityEstimate() const override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, const string &name) override;

	ParquetOptions options;
	shared_ptr<ParquetFileMetadataCache> metadata;
	unique_ptr<ParquetColumnSchema> root_schema;
};

class ParquetReader : public BaseFileReader {
public:
	ParquetReader(ClientContext &context, OpenFileInfo file, ParquetOptions parquet_options,
	              shared_ptr<ParquetFileMetadataCache> metadata = nullptr);
	~ParquetReader() override;

	mutable CachingFileSystem fs;
	Allocator &allocator;
	shared_ptr<ParquetFileMetadataCache> metadata;
	ParquetOptions parquet_options;
	unique_ptr<ParquetColumnSchema> root_schema;
	shared_ptr<EncryptionUtil> encryption_util;
	//! How many rows have been read from this file
	atomic<idx_t> rows_read;

public:
	string GetReaderType() const override {
		return "Parquet";
	}

	shared_ptr<BaseUnionData> GetUnionData(idx_t file_idx) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, const string &name) override;

	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	void PrepareScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
	                 LocalTableFunctionState &lstate_p) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;
	void FinishFile(ClientContext &context, GlobalTableFunctionState &gstate_p) override;
	double GetProgressInFile(ClientContext &context) override;

public:
	void InitializeScan(ClientContext &context, ParquetReaderScanState &state, vector<idx_t> groups_to_read) const;
	AsyncResult Scan(ClientContext &context, ParquetReaderScanState &state, DataChunk &output);

	idx_t NumRows() const;
	idx_t NumRowGroups() const;
	idx_t GetFileSize() const;
	idx_t GetDataSize() const;

	const duckdb_parquet::FileMetaData *GetFileMetadata() const;
	string static GetUniqueFileIdentifier(const duckdb_parquet::EncryptionAlgorithm &encryption_algorithm);

	uint32_t Read(duckdb_apache::thrift::TBase &object, TProtocol &iprot) const;
	uint32_t ReadEncrypted(duckdb_apache::thrift::TBase &object, TProtocol &iprot,
	                       CryptoMetaData &aad_crypto_metadata) const;
	uint32_t ReadData(duckdb_apache::thrift::protocol::TProtocol &iprot, const data_ptr_t buffer,
	                  const uint32_t buffer_size) const;
	uint32_t ReadDataEncrypted(duckdb_apache::thrift::protocol::TProtocol &iprot, const data_ptr_t buffer,
	                           const uint32_t buffer_size, CryptoMetaData &aad_crypto_metadata) const;

	unique_ptr<BaseStatistics> ReadStatistics(const string &name);

	CachingFileHandle &GetHandle() {
		return *file_handle;
	}

	static unique_ptr<BaseStatistics> ReadStatistics(ClientContext &context, ParquetOptions parquet_options,
	                                                 shared_ptr<ParquetFileMetadataCache> metadata, const string &name);
	static unique_ptr<BaseStatistics> ReadStatistics(const ParquetUnionData &union_data, const string &name);

	LogicalType DeriveLogicalType(const SchemaElement &s_ele, ParquetColumnSchema &schema) const;
	static LogicalType DeriveLogicalType(const SchemaElement &s_ele, const ParquetOptions &options,
	                                     ParquetColumnSchema &schema);

	void AddVirtualColumn(column_t virtual_column_id) override;

	void GetPartitionStats(vector<PartitionStatistics> &result);
	static void GetPartitionStats(const duckdb_parquet::FileMetaData &metadata, vector<PartitionStatistics> &result,
	                              optional_ptr<ParquetColumnSchema> root_schema = nullptr,
	                              optional_ptr<ParquetOptions> parquet_options = nullptr);
	static bool MetadataCacheEnabled(ClientContext &context);
	static shared_ptr<ParquetFileMetadataCache> GetMetadataCacheEntry(ClientContext &context, const OpenFileInfo &file);

private:
	//! Construct a parquet reader but **do not** open a file, used in ReadStatistics only
	ParquetReader(ClientContext &context, ParquetOptions parquet_options,
	              shared_ptr<ParquetFileMetadataCache> metadata);

	void InitializeSchema(ClientContext &context);
	//! Parse the schema of the file
	unique_ptr<ParquetColumnSchema> ParseSchema(ClientContext &context);
	ParquetColumnSchema ParseSchemaRecursive(idx_t depth, idx_t max_define, idx_t max_repeat, idx_t &next_schema_idx,
	                                         idx_t &next_file_idx, ClientContext &context);

	unique_ptr<ColumnReader> CreateReader(ClientContext &context) const;
	unique_ptr<ColumnReader> CreateReaderRecursive(ClientContext &context, const vector<ColumnIndex> &indexes,
	                                               const ParquetColumnSchema &schema) const;
	const duckdb_parquet::RowGroup &GetGroup(ParquetReaderScanState &state);
	uint64_t GetGroupCompressedSize(ParquetReaderScanState &state);
	idx_t GetGroupOffset(ParquetReaderScanState &state);
	// Group span is the distance between the min page offset and the max page offset plus the max page compressed size
	uint64_t GetGroupSpan(ParquetReaderScanState &state);
	void PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t out_col_idx);
	ParquetColumnSchema ParseColumnSchema(const SchemaElement &s_ele, idx_t max_define, idx_t max_repeat,
	                                      idx_t schema_index, idx_t column_index,
	                                      ParquetColumnSchemaType type = ParquetColumnSchemaType::COLUMN);

	MultiFileColumnDefinition ParseColumnDefinition(const duckdb_parquet::FileMetaData &file_meta_data,
	                                                ParquetColumnSchema &element);
	unique_ptr<AdditionalAuthenticatedData> GenerateAAD(uint8_t module_type, uint16_t row_group_ordinal,
	                                                    uint16_t column_ordinal, uint16_t page_ordinal) const;

private:
	unique_ptr<CachingFileHandle> file_handle;
};

} // namespace duckdb
