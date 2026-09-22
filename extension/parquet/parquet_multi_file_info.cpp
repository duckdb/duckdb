#include "parquet_multi_file_info.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/main/client_context.hpp"

#include <stdint.h>
#include <unordered_map>

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "parquet_crypto.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "parquet_column_schema.hpp"
#include "parquet_file_metadata_cache.hpp"
#include "parquet_reader.hpp"
#include "parquet_types.h"

namespace duckdb {
class BaseStatistics;
class ClientContext;
class DataChunk;
class ExecutionContext;
class Expression;
class LogicalGet;
class PhysicalOperator;

struct ParquetMetadataCacheEntry {
	ParquetMetadataCacheEntry(shared_ptr<ParquetFileMetadataCache> metadata, ParquetCacheValidity validity,
	                          bool has_deletes);

	shared_ptr<ParquetFileMetadataCache> metadata;
	ParquetCacheValidity validity;
	bool has_deletes;
};

struct ParquetReadBindData : public TableFunctionData {
	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;
	idx_t initial_file_size = 0;
	idx_t initial_file_data_size = 0;
	idx_t explicit_cardinality = 0; // can be set to inject exterior cardinality knowledge (e.g. from a data lake)
	unique_ptr<ParquetFileReaderOptions> options;
	unordered_map<idx_t, ParquetReaderProjectionExpression> projection_expressions;

	ParquetOptions &GetParquetOptions() {
		return options->options;
	}
	const ParquetOptions &GetParquetOptions() const {
		return options->options;
	}

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ParquetReadBindData>();
		result->initial_file_cardinality = initial_file_cardinality;
		result->initial_file_row_groups = initial_file_row_groups;
		result->initial_file_size = initial_file_size;
		result->initial_file_data_size = initial_file_data_size;
		result->explicit_cardinality = explicit_cardinality;
		result->options = make_uniq<ParquetFileReaderOptions>(options->options);
		result->projection_expressions = projection_expressions;
		return std::move(result);
	}

	const vector<ParquetMetadataCacheEntry> &TryLoadCaches(const MultiFileBindData &bind_data, ClientContext &context);

private:
	vector<ParquetMetadataCacheEntry> caches;
	bool attempted_to_load_caches = false;
};

unique_ptr<MultiFileReaderInterface> ParquetMultiFileInfo::CreateInterface(ClientContext &context) {
	return make_uniq<ParquetMultiFileInfo>();
}

void ParquetMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types,
                                      vector<Identifier> &names, MultiFileBindData &bind_data) {
	auto &parquet_bind = bind_data.bind_data->Cast<ParquetReadBindData>();
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
	    context, return_types, names, *bind_data.file_list, bind_data, *parquet_bind.options, bind_data.file_options);
}

static bool GetBooleanArgument(const Identifier &key, const vector<Value> &option_values) {
	if (option_values.empty()) {
		return true;
	}
	string error_message;
	auto boolean_value = option_values[0].DefaultTryCastAs(LogicalType::BOOLEAN, &error_message);
	if (!boolean_value) {
		throw InvalidInputException("Unable to cast \"%s\" to BOOLEAN for Parquet option %s",
		                            option_values[0].ToString(), key);
	}
	return BooleanValue::Get(*boolean_value);
}

static bool ParquetScanPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	return true;
}

static bool ParquetScanSupportPushdownExtract(const FunctionData &bind_data_p, const LogicalIndex &col_idx) {
	auto &bind_data = bind_data_p.Cast<MultiFileBindData>();

	auto &column = bind_data.columns[col_idx.index];
	auto &column_type = column.type;
	return column_type.id() == LogicalTypeId::STRUCT || column_type.id() == LogicalTypeId::VARIANT;
}

static void ParquetScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                 const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto &parquet_data = bind_data.bind_data->Cast<ParquetReadBindData>();

	vector<string> files;
	for (auto &file : bind_data.file_list->GetAllFiles()) {
		files.emplace_back(file.path);
	}
	serializer.WriteProperty(100, "files", files);
	serializer.WriteProperty(101, "types", bind_data.types);
	serializer.WriteProperty(102, "names", bind_data.names);
	ParquetOptionsSerialization serialization(parquet_data.GetParquetOptions(), bind_data.file_options);
	serializer.WriteProperty(103, "parquet_options", serialization);
	// previously serialization version 3
	if (serializer.ShouldSerialize(StorageVersion::V1_2_0)) {
		serializer.WriteProperty(104, "table_columns", bind_data.table_columns);
	}
	// Old clients won't be able to read the plan even if we
	// don't push this field. If projection expression worked,
	// it modified "types" which we serialize ultimately
	serializer.WriteProperty(105, "projection_expressions", parquet_data.projection_expressions);
}

static unique_ptr<FunctionData> ParquetScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();
	auto files = deserializer.ReadProperty<vector<string>>(100, "files");
	auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
	auto names = StringsToIdentifiers(deserializer.ReadProperty<vector<string>>(102, "names"));
	auto serialization = deserializer.ReadProperty<ParquetOptionsSerialization>(103, "parquet_options");
	auto table_columns =
	    deserializer.ReadPropertyWithExplicitDefault<vector<string>>(104, "table_columns", vector<string> {});
	auto projection_expressions =
	    deserializer.ReadPropertyWithExplicitDefault<unordered_map<idx_t, ParquetReaderProjectionExpression>>(
	        105, "projection_expressions", unordered_map<idx_t, ParquetReaderProjectionExpression> {});

	vector<Value> file_path;
	for (auto &path : files) {
		file_path.emplace_back(path);
	}
	FileGlobInput input(FileGlobOptions::FALLBACK_GLOB, "parquet");
	// we are restoring an already bound file list rather than globbing user input - it is legitimately empty
	// when filter pushdown pruned every file away, and rejecting that makes the plan impossible to deserialize
	input.allow_empty = true;

	auto multi_file_reader = MultiFileReader::Create(function);
	auto file_list = multi_file_reader->CreateFileList(context, Value::LIST(LogicalType::VARCHAR, file_path), input);
	// plans serialized before "file_row_number" and "schema" became options of every multi-file reader keep them with
	// the parquet options
	if (serialization.legacy_file_row_number) {
		serialization.file_options.file_row_number = true;
	}
	if (serialization.file_options.schema.empty()) {
		for (auto &column : serialization.legacy_schema) {
			serialization.file_options.schema.push_back(column.ToMultiFileColumnDefinition());
		}
	}
	auto parquet_options = make_uniq<ParquetFileReaderOptions>(std::move(serialization.parquet_options));
	auto interface = make_uniq<ParquetMultiFileInfo>();
	auto bind_data = MultiFileFunction<ParquetMultiFileInfo>::MultiFileBindInternal(
	    context, std::move(multi_file_reader), std::move(file_list), types, names,
	    std::move(serialization.file_options), std::move(parquet_options), std::move(interface));
	auto &inner_bind_data = bind_data->Cast<MultiFileBindData>();
	inner_bind_data.table_columns = std::move(table_columns);
	auto &parquet_bind_data = inner_bind_data.bind_data->Cast<ParquetReadBindData>();
	parquet_bind_data.projection_expressions = std::move(projection_expressions);

	for (const auto &[idx, expr] : parquet_bind_data.projection_expressions) {
		if (idx < inner_bind_data.columns.size()) {
			inner_bind_data.columns[idx].type = expr.return_type;
		}
		if (idx < inner_bind_data.types.size()) {
			inner_bind_data.types[idx] = expr.return_type;
		}
		if (auto &schema = inner_bind_data.reader_bind.schema; !schema.empty() && idx < schema.size()) {
			schema[idx].type = expr.return_type;
		}
		if (!inner_bind_data.initial_reader) {
			continue;
		}
		auto &reader = inner_bind_data.initial_reader->Cast<ParquetReader>();
		reader.projection_expressions[idx] = expr;
		if (idx < reader.columns.size()) {
			reader.columns[idx].type = expr.return_type;
		}
		if (idx < reader.root_schema->children.size()) {
			reader.root_schema->children[idx].type = expr.return_type;
		}
	}
	return bind_data;
}

static vector<column_t> ParquetGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	vector<column_t> result;
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX);
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

static void ParquetScanGetMetrics(TableFunctionGetMetricsInput &input) {
	// emit the shared multi-file metrics (files read, filenames, rows scanned)
	MultiFileFunction<ParquetMultiFileInfo>::MultiFileGetMetrics(input);
	if (!input.global_state) {
		return;
	}
	auto &gstate = input.global_state->Cast<MultiFileGlobalState>();
	if (!gstate.global_state) {
		return;
	}
	auto &parquet_gstate = gstate.global_state->Cast<ParquetReadGlobalState>();
	// each local state drains the not-yet-reported count
	input.operator_metrics.row_groups_scanned = parquet_gstate.row_groups_scanned_unreported.exchange(0);
	input.operator_metrics.total_row_groups_to_scan = parquet_gstate.total_row_groups_to_scan.load();
}

static bool ParquetProjectionExpressionPushdown(ClientContext &context,
                                                const TableFunctionProjectionExpressionInput &input) {
	if (input.expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	const auto &fn = input.expr.Cast<BoundFunctionExpression>();
	if (const Identifier &name = fn.Function().GetName(); name != "strlen" && name != "octet_length") {
		return false;
	}

	auto &bind_data = input.get.bind_data->Cast<MultiFileBindData>();
	// Don't do pushdown on custom schema users like Ducklake.
	// We may support it and UNION readers in the future
	if (!bind_data.reader_bind.schema.empty() || !bind_data.union_readers.empty()) {
		return false;
	}

	const auto &column_id = input.get.GetColumnIds()[input.column_index];
	// Pushdown extract columns i.e. SELECT x.y.z have a complex nested type update
	if (column_id.IsPushdownExtract()) {
		return false;
	}
	const idx_t idx = column_id.GetPrimaryIndex();

	// Don't do pushdown of strlen to hive and filename columns.
	// See src/function/table/read_csv.cpp : PushdownProjectionExpression
	for (const auto &partition : bind_data.reader_bind.hive_partitioning_indexes) {
		if (partition.index == idx) {
			return false;
		}
	}
	if (bind_data.reader_bind.filename_idx.IsValid() && bind_data.reader_bind.filename_idx.GetIndex() == idx) {
		return false;
	}

	// We run scalar function pushdown after filter pushdown. If a filter was
	// pushed into get.table_filters, we don't see it here and can't
	// proceed with pushdown
	if (input.get.table_filters.HasFilter(input.column_index)) {
		return false;
	}

	auto &parquet_bind_data = bind_data.bind_data->Cast<ParquetReadBindData>();
	// Don't do pushdown with multiple files. As metadata for all but first file is not
	// available upfront, we need to TryLoadCaches and check cache validity which complicates
	// things. Another complication is that different files may have separate schemas:
	// if we're reading file 1 (int, str) and file 2 (str, int) and trying to push down
	// strlen for column 2, query will fail on file 2 as it compares column indices and
	// not names
	if (bind_data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
		return false;
	}
	if (!bind_data.initial_reader) {
		return false;
	}

	auto &reader = bind_data.initial_reader->Cast<ParquetReader>();
	auto &children = reader.root_schema->children;
	if (idx >= children.size()) {
		return false;
	}

	const idx_t column_flat_idx = children[idx].column_index;
	for (const auto &group : reader.GetFileMetadata()->row_groups) {
		D_ASSERT(column_flat_idx < group.columns.size());
		for (const Encoding::type type : group.columns[column_flat_idx].meta_data.encodings) {
			if (type == duckdb_parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY) {
				return false;
			}
		}
	}

	const LogicalType type = LogicalType::BIGINT;
	bind_data.types[idx] = type;
	bind_data.columns[idx].type = type;

	const ParquetReaderProjectionExpression expression {ParquetReaderProjectionExpressionType::BYTE_LENGTH, type};

	parquet_bind_data.projection_expressions[idx] = expression;

	// initial reader was created before scalar function pushdown optimizer pass, update its types.
	reader.projection_expressions[idx] = expression;
	if (idx < reader.columns.size()) {
		reader.columns[idx].type = type;
	}
	children[idx].type = type;
	return true;
}

ParquetMetadataCacheEntry::ParquetMetadataCacheEntry(shared_ptr<ParquetFileMetadataCache> metadata_p,
                                                     ParquetCacheValidity validity_p, bool has_deletes_p)
    : metadata(std::move(metadata_p)), validity(validity_p), has_deletes(has_deletes_p) {
}

//! The cached metadata of every file of the scan - empty unless the metadata of all of them is cached
static vector<ParquetMetadataCacheEntry> LoadMetadataCaches(ClientContext &context, MultiFileList &file_list) {
	vector<ParquetMetadataCacheEntry> result;
	// if we are reading multiple files - we check if we have caching enabled
	if (!ParquetReader::MetadataCacheEnabled(context)) {
		// no caching - bail
		return result;
	}
	// caching is enabled - check if we have ALL of the metadata cached
	for (auto &file : file_list.Files()) {
		auto metadata_entry = ParquetReader::GetMetadataCacheEntry(context, file);
		if (!metadata_entry) {
			// no cache entry found for this file
			return vector<ParquetMetadataCacheEntry>();
		}
		// check if the file has any deletes
		// if it has, skip emitting partition stats
		// FIXME: we could emit partition stats but set count to `COUNT_APPROXIMATE` instead of `COUNT_EXACT`
		bool has_deletes = false;
		if (file.extended_info) {
			file.extended_info->TryGetOption("has_deletes", has_deletes);
		}

		// check if the cache is valid based ONLY on the OpenFileInfo (do not do any file system requests here)
		const auto is_valid = metadata_entry->IsValid(file, context);
		result.emplace_back(std::move(metadata_entry), is_valid, has_deletes);
	}
	return result;
}

const vector<ParquetMetadataCacheEntry> &ParquetReadBindData::TryLoadCaches(const MultiFileBindData &bind_data,
                                                                            ClientContext &context) {
	if (attempted_to_load_caches) {
		return caches;
	}
	// only attempt to load the caches once
	attempted_to_load_caches = true;
	caches = LoadMetadataCaches(context, *bind_data.file_list);
	return caches;
}

//! The row groups of every file of the scan, taken from their cached metadata - empty unless all of it can be used
static vector<PartitionStatistics>
GetCachedPartitionStats(ClientContext &context, const vector<ParquetMetadataCacheEntry> &cached_metadata,
                        const shared_ptr<ParquetEncryptionConfig> &encryption_config) {
	vector<PartitionStatistics> result;
	if (cached_metadata.empty()) {
		// no cached metadata - bail
		return result;
	}
	string encryption_key_hash;
	optional_ptr<const string> encryption_key_hash_ptr;
	// first check if all caches are valid and there are no deletes
	for (auto &cache : cached_metadata) {
		if (cache.metadata->IsEncrypted() && encryption_config && !encryption_key_hash_ptr) {
			auto hash_util = context.db->GetMbedTLSUtil(false);
			encryption_key_hash = ParquetFileMetadataCache::CreateEncryptionKeyHash(*encryption_config, *hash_util);
			encryption_key_hash_ptr = encryption_key_hash;
		}
		if (!cache.metadata->CanUseMetadataStatistics(encryption_config, encryption_key_hash_ptr)) {
			return result;
		}
		if (cache.has_deletes) {
			// we have deletes - don't return any partition stats
			// FIXME: we could return with count approximate
			return result;
		}
		if (cache.validity != ParquetCacheValidity::VALID) {
			// we don't know for sure if this cache entry is valid - we can't use these stats
			return result;
		}
	}

	// all caches are valid! we can return the partition stats
	for (auto &cache : cached_metadata) {
		ParquetReader::GetPartitionStats(*cache.metadata->metadata, result);
	}
	return result;
}

static vector<PartitionStatistics> ParquetGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	vector<PartitionStatistics> result;
	if (bind_data.file_list->GetExpandResult() == FileExpandResult::SINGLE_FILE && bind_data.initial_reader) {
		// we have read the metadata - get the partitions for this reader
		auto &reader = bind_data.initial_reader->Cast<ParquetReader>();
		reader.GetPartitionStats(result);
		return result;
	}
	auto &parquet_data = bind_data.bind_data->Cast<ParquetReadBindData>();
	auto &cached_metadata = parquet_data.TryLoadCaches(bind_data, context);
	return GetCachedPartitionStats(context, cached_metadata, parquet_data.GetParquetOptions().encryption_config);
}

//! The row groups of a multi-file parquet scan - those of a single file are read from the file, those of several files
//! only when the metadata of every one of them is cached
static vector<PartitionStatistics> ParquetMultiFileGetPartitionStats(ClientContext &context,
                                                                     GetPartitionStatsInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	if (bind_data.file_list->GetExpandResult() == FileExpandResult::SINGLE_FILE) {
		return TableFunctionMultiFileWrapper::GetPartitionStats(context, input);
	}
	// without the bind of a file we do not know the key the files are encrypted with - encrypted files are then not
	// answered from their metadata
	shared_ptr<ParquetEncryptionConfig> encryption_config;
	auto &file_bind_data = bind_data.bind_data->Cast<TableFunctionMultiFileData>().options.schema_bind_data;
	if (file_bind_data) {
		encryption_config = ParquetScanFunction::GetFileOptions(*file_bind_data).encryption_config;
	}
	return GetCachedPartitionStats(context, LoadMetadataCaches(context, *bind_data.file_list), encryption_config);
}

void ParquetScanFunction::AddNamedParameters(TableFunction &table_function) {
	table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
	table_function.named_parameters["debug_use_openssl"] = LogicalType::BOOLEAN;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;
	table_function.named_parameters["explicit_cardinality"] = LogicalType::UBIGINT;
	table_function.named_parameters["encryption_config"] = LogicalTypeId::ANY;
	table_function.named_parameters["parquet_version"] = LogicalType::VARCHAR;
	table_function.named_parameters["can_have_nan"] = LogicalType::BOOLEAN;
	table_function.named_parameters["prefetch_strategy"] = LogicalType::VARCHAR;
	table_function.named_parameters["utf8_validation"] = LogicalType::VARCHAR;
}

//! Bind a parquet scan. The wrapped single-file function is resolved here rather than taken from the function that
//! is being bound - callers that manage parquet files themselves (e.g. DuckLake) bind this through a TableFunction
//! they construct, which carries none of our info
static unique_ptr<FunctionData> ParquetMultiFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	return TableFunctionMultiFileWrapper::MultiFileBindWith(context, input, return_types, names,
	                                                        ParquetScanFunction::GetSingleFileFunction(),
	                                                        ParquetScanFunction::GetMultiFileSettings());
}

TableFunction ParquetScanFunction::GetMultiFileFunction(Identifier name) {
	// the multi-file parquet reader is the single-file parquet reader wrapped into a multi-file function
	auto result = TableFunctionMultiFileWrapper::CreateFunction(GetSingleFileFunction(), std::move(name),
	                                                            GetMultiFileSettings(), ParquetMultiFileBind);
	// the callbacks below describe the scan rather than one of its files, so they are set on the wrapper
	result.get_row_id_columns = ParquetGetRowIdColumns;
	result.supports_pushdown_extract = ParquetScanSupportPushdownExtract;
	result.pushdown_expression = ParquetScanPushdownExpression;
	result.get_partition_stats = ParquetMultiFileGetPartitionStats;
	result.projection_expression_pushdown = ParquetScanFunction::ProjectionExpressionPushdown;
	return result;
}

TableFunctionSet ParquetScanFunction::GetFunctionSet() {
	MultiFileFunction<ParquetMultiFileInfo> table_function("parquet_scan");
	AddNamedParameters(table_function);
	table_function.statistics_extended = MultiFileFunction<ParquetMultiFileInfo>::MultiFileScanStatsExtended;
	table_function.get_metrics = ParquetScanGetMetrics;
	table_function.projection_expression_pushdown = ParquetProjectionExpressionPushdown;
	table_function.supports_pushdown_extract = ParquetScanSupportPushdownExtract;
	table_function.serialize = ParquetScanSerialize;
	table_function.deserialize = ParquetScanDeserialize;
	table_function.get_row_id_columns = ParquetGetRowIdColumns;
	table_function.pushdown_expression = ParquetScanPushdownExpression;
	table_function.get_partition_stats = ParquetGetPartitionStats;
	table_function.filter_pushdown = true;
	table_function.filter_prune = true;
	table_function.late_materialization = true;

	return MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(table_function));
}

unique_ptr<BaseFileReaderOptions> ParquetMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                          optional_ptr<TableFunctionInfo> info) {
	return make_uniq<ParquetFileReaderOptions>(context);
}

bool ParquetMultiFileInfo::ParseCopyOption(ClientContext &context, const Identifier &key, const vector<Value> &values,
                                           BaseFileReaderOptions &file_options, vector<Identifier> &expected_names,
                                           vector<LogicalType> &expected_types) {
	auto &parquet_options = file_options.Cast<ParquetFileReaderOptions>();
	auto &options = parquet_options.options;
	if (key == "compression" || key == "codec" || key == "row_group_size") {
		// CODEC/COMPRESSION and ROW_GROUP_SIZE options have no effect on parquet read.
		// These options are determined from the file.
		return true;
	}
	if (key == "binary_as_string") {
		options.binary_as_string = GetBooleanArgument(key, values);
		return true;
	}
	if (key == "debug_use_openssl") {
		return true; // deprecated
	}
	if (key == "encryption_config") {
		if (values.size() != 1) {
			throw BinderException("Parquet encryption_config cannot be empty!");
		}
		options.encryption_config = ParquetEncryptionConfig::Create(context, values[0]);
		return true;
	}
	if (key == "can_have_nan") {
		if (values.size() != 1) {
			throw BinderException("Parquet can_have_nan cannot be empty!");
		}
		options.can_have_nan = GetBooleanArgument(key, values);
		return true;
	}
	if (key == "prefetch_strategy") {
		if (values.size() != 1) {
			throw BinderException("Parquet prefetch_strategy cannot be empty!");
		}
		options.prefetch_strategy = ParquetPrefetchStrategyOptionFromString(StringValue::Get(values[0]));
		return true;
	}
	if (key == "utf8_validation") {
		if (values.size() != 1) {
			throw BinderException("Parquet utf8_validation cannot be empty!");
		}
		options.utf8_validation_option = StringColumnReader::GetUtf8ValidationOption(StringValue::Get(values[0]));
		return true;
	}
	return false;
}

bool ParquetMultiFileInfo::ParseOption(ClientContext &context, const Identifier &key, const Value &val,
                                       MultiFileOptions &file_options, BaseFileReaderOptions &base_options) {
	auto &parquet_options = base_options.Cast<ParquetFileReaderOptions>();
	auto &options = parquet_options.options;
	if (val.IsNull()) {
		throw BinderException("Cannot use NULL as argument to %s", key);
	}
	if (key == "compression") {
		// COMPRESSION has no effect on parquet read.
		// These options are determined from the file.
		return true;
	}
	if (key == "binary_as_string") {
		options.binary_as_string = BooleanValue::Get(val);
		return true;
	}
	if (key == "debug_use_openssl") {
		return true; // deprecated
	}
	if (key == "can_have_nan") {
		options.can_have_nan = BooleanValue::Get(val);
		return true;
	}
	if (key == "explicit_cardinality") {
		options.explicit_cardinality = UBigIntValue::Get(val);
		return true;
	}
	if (key == "encryption_config") {
		options.encryption_config = ParquetEncryptionConfig::Create(context, val);
		return true;
	}
	if (key == "prefetch_strategy") {
		options.prefetch_strategy = ParquetPrefetchStrategyOptionFromString(StringValue::Get(val));
		return true;
	}
	if (key == "utf8_validation") {
		options.utf8_validation_option = StringColumnReader::GetUtf8ValidationOption(StringValue::Get(val));
		return true;
	}
	return false;
}

unique_ptr<TableFunctionData> ParquetMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                       unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<ParquetReadBindData>();
	// Set the explicit cardinality if requested
	result->options = unique_ptr_cast<BaseFileReaderOptions, ParquetFileReaderOptions>(std::move(options_p));
	auto &parquet_options = result->GetParquetOptions();
	if (parquet_options.explicit_cardinality) {
		auto file_count = multi_file_data.file_list->GetTotalFileCount();
		result->explicit_cardinality = parquet_options.explicit_cardinality;
		result->initial_file_cardinality = result->explicit_cardinality / (file_count ? file_count : 1);
	}
	return std::move(result);
}

void ParquetMultiFileInfo::GetBindInfo(const TableFunctionData &bind_data_p, BindInfo &info) {
	auto &bind_data = bind_data_p.Cast<ParquetReadBindData>();
	auto &parquet_options = bind_data.GetParquetOptions();
	info.type = ScanType::PARQUET;
	info.InsertOption("binary_as_string", Value::BOOLEAN(parquet_options.binary_as_string));
}

optional_idx ParquetMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                              const MultiFileGlobalState &global_state,
                                              FileExpandResult expand_result) {
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		// always launch max threads if we are reading multiple files
		return optional_idx();
	}
	auto &bind_data = bind_data_p.bind_data->Cast<ParquetReadBindData>();
	return MaxValue(bind_data.initial_file_row_groups, static_cast<idx_t>(1));
}

void ParquetMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
	auto &bind_data = multi_file_data.bind_data->Cast<ParquetReadBindData>();
	if (multi_file_data.initial_reader) {
		auto &initial_reader = multi_file_data.initial_reader->Cast<ParquetReader>();
		bind_data.initial_file_cardinality = initial_reader.NumRows();
		bind_data.initial_file_row_groups = initial_reader.NumRowGroups();
		bind_data.initial_file_size = initial_reader.GetFileSize();
		bind_data.initial_file_data_size = initial_reader.GetDataSize();
		if (bind_data.initial_file_data_size >= bind_data.initial_file_size) {
			// this should not be possible - we need at least some metadata in the file
			// FIXME: should this throw an error?
			bind_data.initial_file_data_size = bind_data.initial_file_size - 1;
		}
		bind_data.options->options = initial_reader.parquet_options;
	}
}

unique_ptr<NodeStatistics> ParquetMultiFileInfo::GetCardinality(ClientContext &context,
                                                                const MultiFileBindData &bind_data, idx_t file_count) {
	auto &parquet_data = bind_data.bind_data->Cast<ParquetReadBindData>();
	if (parquet_data.explicit_cardinality) {
		return make_uniq<NodeStatistics>(parquet_data.explicit_cardinality);
	}
	if (file_count == 1) {
		// if we have one file we can just use the cardinality of that file
		return make_uniq<NodeStatistics>(parquet_data.initial_file_cardinality);
	}
	// multiple parquet files
	// check if we have cached Parquet metadata we can use to get the estimate
	auto &caches = parquet_data.TryLoadCaches(bind_data, context);
	if (!caches.empty()) {
		// we have cached Parquet data - use it to get the cardinality estimate
		idx_t cardinality = 0;
		for (auto &cache : caches) {
			// note: since this is just an estimate we don't need to look at whether or not the cache is valid
			cardinality += cache.metadata->metadata->num_rows;
		}
		return make_uniq<NodeStatistics>(cardinality);
	}
	// we don't have any scan data - check if we can get some intel from the file list
	// in particular - if we have file sizes, we try to estimate rows per file from the file size
	MultiFileListScanData scan_data;
	scan_data.scan_type = MultiFileListScanType::FETCH_IF_AVAILABLE;
	bind_data.file_list->InitializeScan(scan_data);
	OpenFileInfo file;
	idx_t initial_cardinality = MaxValue<idx_t>(parquet_data.initial_file_cardinality, 1ULL);
	idx_t estimated_bytes_per_row = parquet_data.initial_file_data_size / initial_cardinality;
	// for very small cardinalities, compression doesn't really work well
	// to compensate if initial_cardinality is small we decrease estimated_bytes_per_row
	// for 1 row we divide by 10, for 2 rows we divide by 9, etc
	if (initial_cardinality < 10) {
		estimated_bytes_per_row /= 11 - initial_cardinality;
	}
	estimated_bytes_per_row = MaxValue<idx_t>(estimated_bytes_per_row, 10ULL);

	idx_t files_with_sizes = 0;
	idx_t estimated_file_row_count = 0;
	while (bind_data.file_list->Scan(scan_data, file)) {
		if (!file.extended_info) {
			// no extended info
			estimated_file_row_count = 0;
			break;
		}
		idx_t current_file_size;
		if (!file.extended_info->TryGetOption("file_size", current_file_size)) {
			// no file size available
			estimated_file_row_count = 0;
			break;
		}
		// we have the file size - estimate row count based on estimated bytes per row
		files_with_sizes++;
		idx_t rows_in_this_file = MaxValue<idx_t>(current_file_size / estimated_bytes_per_row, 1ULL);
		estimated_file_row_count += rows_in_this_file;
	}
	idx_t per_file_cardinality;
	if (estimated_file_row_count > 0) {
		// use estimate based on file sizes
		per_file_cardinality = estimated_file_row_count / files_with_sizes;
	} else {
		// no estimate based on file sizes - use initial file cardinality
		per_file_cardinality = parquet_data.initial_file_cardinality;
	}
	// if we have several files, our cardinality estimate can be way off if our initial file is ~empty
	// we set the minimum per file cardinality to 1000 just to avoid greatly underestimating
	idx_t min_per_file_cardinality = 1000ULL;
	if (per_file_cardinality < min_per_file_cardinality) {
		per_file_cardinality = min_per_file_cardinality;
	}
	return make_uniq<NodeStatistics>(per_file_cardinality * file_count);
}

unique_ptr<BaseStatistics> ParquetReader::GetStatistics(ClientContext &context, const Identifier &name) {
	return ReadStatistics(name);
}

double ParquetReader::GetProgressInFile(ClientContext &context) {
	auto read_rows = rows_read.load();
	return 100.0 * (static_cast<double>(read_rows) / static_cast<double>(NumRows()));
}

void ParquetMultiFileInfo::GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &result) {
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	result.insert(make_pair(ParquetReader::COLUMN_IDENTIFIER_FILE_ROW_GROUP_NUMBER,
	                        TableColumn("file_row_group_number", LogicalType::UBIGINT)));
}

shared_ptr<BaseFileReader> ParquetMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &,
                                                              BaseUnionData &union_data_p,
                                                              const MultiFileBindData &bind_data_p) {
	auto &union_data = union_data_p.Cast<ParquetUnionData>();
	return make_shared_ptr<ParquetReader>(context, union_data.file, union_data.options, union_data.metadata);
}

shared_ptr<BaseFileReader> ParquetMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &,
                                                              const OpenFileInfo &file, idx_t file_idx,
                                                              const MultiFileBindData &multi_bind_data) {
	auto &bind_data = multi_bind_data.bind_data->Cast<ParquetReadBindData>();
	return make_shared_ptr<ParquetReader>(context, file, bind_data.GetParquetOptions(), nullptr,
	                                      bind_data.projection_expressions);
}

shared_ptr<BaseFileReader> ParquetMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                              BaseFileReaderOptions &options_p,
                                                              const MultiFileOptions &) {
	auto &options = options_p.Cast<ParquetFileReaderOptions>();
	return make_shared_ptr<ParquetReader>(context, file, options.options);
}

shared_ptr<BaseUnionData> ParquetReader::GetUnionData(idx_t file_idx) {
	auto result = make_uniq<ParquetUnionData>(file);
	result->names.reserve(columns.size());
	result->types.reserve(columns.size());
	for (auto &column : columns) {
		result->names.push_back(column.name.GetIdentifierName());
		result->types.push_back(column.type);
	}

	result->options = parquet_options;
	result->metadata = metadata;
	if (file_idx == 0) {
		result->reader = shared_from_this();
	} else {
		result->root_schema = std::move(root_schema);
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ParquetMultiFileInfo::InitializeGlobalState(ClientContext &, MultiFileBindData &,
                                                                                 MultiFileGlobalState &global_state) {
	return make_uniq<ParquetReadGlobalState>(global_state.op);
}

unique_ptr<LocalTableFunctionState> ParquetMultiFileInfo::InitializeLocalState(ClientContext &,
                                                                               GlobalTableFunctionState &) {
	return make_uniq<ParquetReadLocalState>();
}

bool ParquetReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                      LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<ParquetReadGlobalState>();
	auto &lstate = lstate_p.Cast<ParquetReadLocalState>();
	if (gstate.row_group_index >= NumRowGroups()) {
		// scanned all row groups in this file
		return false;
	}
	// The current reader has rowgroups left to be scanned
	lstate.group_index = gstate.row_group_index;
	gstate.row_group_index++;
	return true;
}

void ParquetReader::PrepareScan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<ParquetReadGlobalState>();
	auto &lstate = lstate_p.Cast<ParquetReadLocalState>();
	lstate.scan_state.op = gstate.op;
	InitializeScan(context, lstate.scan_state, lstate.group_index);
}

AsyncResult ParquetReader::ScheduleIO(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                      LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<ParquetReadGlobalState>();
	auto &lstate = lstate_p.Cast<ParquetReadLocalState>();
	auto &scan_state = lstate.scan_state;
	auto read_before = scan_state.row_groups_read;
	auto skipped_before = scan_state.row_groups_skipped;
	auto strategy = RegisterRowGroupReads(context, scan_state);
	auto read = scan_state.row_groups_read - read_before;
	auto skipped = scan_state.row_groups_skipped - skipped_before;
	gstate.row_groups_scanned_unreported += read;
	gstate.total_row_groups_to_scan += read + skipped;
	return ScheduleRowGroupReads(scan_state, strategy);
}

void ParquetReader::FinishFile(ClientContext &context, GlobalTableFunctionState &gstate_p) {
	auto &gstate = gstate_p.Cast<ParquetReadGlobalState>();
	gstate.row_group_index = 0;
}

AsyncResult ParquetReader::Scan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                LocalTableFunctionState &local_state_p, DataChunk &chunk) {
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	{
		AsyncResult test_result;
		if (AsyncResult::TryGenerateTestResult(test_result)) {
			return test_result;
		}
	}
#endif
	auto &local_state = local_state_p.Cast<ParquetReadLocalState>();
	return Process(context, local_state.scan_state, chunk);
}

unique_ptr<MultiFileReaderInterface> ParquetMultiFileInfo::Copy() {
	return make_uniq<ParquetMultiFileInfo>();
}

FileGlobInput ParquetMultiFileInfo::GetGlobInput() {
	return FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "parquet");
}

} // namespace duckdb
