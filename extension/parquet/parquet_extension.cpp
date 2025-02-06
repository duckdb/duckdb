#define DUCKDB_EXTENSION_MAIN

#include "parquet_extension.hpp"

#include "cast_column_reader.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "geo_parquet.hpp"
#include "parquet_crypto.hpp"
#include "parquet_metadata.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "struct_column_reader.hpp"
#include "zstd_file_system.hpp"

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table/row_group.hpp"
#endif

namespace duckdb {

struct ParquetReadBindData : public TableFunctionData {
	shared_ptr<MultiFileList> file_list;
	unique_ptr<MultiFileReader> multi_file_reader;

	shared_ptr<ParquetReader> initial_reader;
	atomic<idx_t> chunk_count;
	vector<string> names;
	vector<LogicalType> types;
	vector<MultiFileReaderColumnDefinition> columns;
	//! Table column names - set when using COPY tbl FROM file.parquet
	vector<string> table_columns;

	// The union readers are created (when parquet union_by_name option is on) during binding
	// Those readers can be re-used during ParquetParallelStateNext
	vector<unique_ptr<ParquetUnionData>> union_readers;

	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;
	idx_t explicit_cardinality = 0; // can be set to inject exterior cardinality knowledge (e.g. from a data lake)
	ParquetOptions parquet_options;
	MultiFileReaderBindData reader_bind;

	void Initialize(shared_ptr<ParquetReader> reader) {
		initial_reader = std::move(reader);
		initial_file_cardinality = initial_reader->NumRows();
		initial_file_row_groups = initial_reader->NumRowGroups();
		parquet_options = initial_reader->parquet_options;
	}
	void Initialize(ClientContext &, unique_ptr<ParquetUnionData> &union_data) {
		Initialize(std::move(union_data->reader));
	}
};

struct ParquetReadLocalState : public LocalTableFunctionState {
	shared_ptr<ParquetReader> reader;
	ParquetReaderScanState scan_state;
	bool is_parallel;
	idx_t batch_index;
	idx_t file_index;
	//! The DataChunk containing all read columns (even columns that are immediately removed)
	DataChunk all_columns;
};

enum class ParquetFileState : uint8_t { UNOPENED, OPENING, OPEN, CLOSED };

struct ParquetFileReaderData {
	// Create data for an unopened file
	explicit ParquetFileReaderData(const string &file_to_be_opened)
	    : reader(nullptr), file_state(ParquetFileState::UNOPENED), file_mutex(make_uniq<mutex>()),
	      file_to_be_opened(file_to_be_opened) {
	}
	// Create data for an existing reader
	explicit ParquetFileReaderData(shared_ptr<ParquetReader> reader_p)
	    : reader(std::move(reader_p)), file_state(ParquetFileState::OPEN), file_mutex(make_uniq<mutex>()) {
	}
	// Create data for an existing reader
	explicit ParquetFileReaderData(unique_ptr<ParquetUnionData> union_data_p) : file_mutex(make_uniq<mutex>()) {
		if (union_data_p->reader) {
			reader = std::move(union_data_p->reader);
			file_state = ParquetFileState::OPEN;
		} else {
			union_data = std::move(union_data_p);
			file_state = ParquetFileState::UNOPENED;
		}
	}

	//! Currently opened reader for the file
	shared_ptr<ParquetReader> reader;
	//! Flag to indicate the file is being opened
	ParquetFileState file_state;
	//! Mutexes to wait for the file when it is being opened
	unique_ptr<mutex> file_mutex;
	//! Parquet options for opening the file
	unique_ptr<ParquetUnionData> union_data;

	//! (only set when file_state is UNOPENED) the file to be opened
	string file_to_be_opened;
};

struct ParquetReadGlobalState : public GlobalTableFunctionState {
	explicit ParquetReadGlobalState(MultiFileList &file_list_p) : file_list(file_list_p) {
	}
	explicit ParquetReadGlobalState(unique_ptr<MultiFileList> owned_file_list_p)
	    : file_list(*owned_file_list_p), owned_file_list(std::move(owned_file_list_p)) {
	}

	//! The file list to scan
	MultiFileList &file_list;
	//! The scan over the file_list
	MultiFileListScanData file_list_scan;
	//! Owned multi file list - if filters have been dynamically pushed into the reader
	unique_ptr<MultiFileList> owned_file_list;

	unique_ptr<MultiFileReaderGlobalState> multi_file_reader_state;

	mutex lock;

	//! The current set of parquet readers
	vector<unique_ptr<ParquetFileReaderData>> readers;

	//! Signal to other threads that a file failed to open, letting every thread abort.
	bool error_opening_file = false;

	//! Index of file currently up for scanning
	atomic<idx_t> file_index;
	//! Index of row group within file currently up for scanning
	idx_t row_group_index;
	//! Batch index of the next row group to be scanned
	idx_t batch_index;

	idx_t max_threads;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;
	vector<ColumnIndex> column_indexes;
	optional_ptr<TableFilterSet> filters;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveColumns() const {
		return !projection_ids.empty();
	}
};

struct ParquetWriteBindData : public TableFunctionData {
	vector<LogicalType> sql_types;
	vector<string> column_names;
	duckdb_parquet::CompressionCodec::type codec = duckdb_parquet::CompressionCodec::SNAPPY;
	vector<pair<string, string>> kv_metadata;
	idx_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
	idx_t row_group_size_bytes = NumericLimits<idx_t>::Maximum();

	//! How/Whether to encrypt the data
	shared_ptr<ParquetEncryptionConfig> encryption_config;
	bool debug_use_openssl = true;

	//! After how many distinct values should we abandon dictionary compression and bloom filters?
	idx_t dictionary_size_limit = row_group_size / 100;

	//! What false positive rate are we willing to accept for bloom filters
	double bloom_filter_false_positive_ratio = 0.01;

	//! After how many row groups to rotate to a new file
	optional_idx row_groups_per_file;

	ChildFieldIDs field_ids;
	//! The compression level, higher value is more
	int64_t compression_level = ZStdFileSystem::DefaultCompressionLevel();

	//! Which encodings to include when writing
	ParquetVersion parquet_version = ParquetVersion::V1;
};

struct ParquetWriteGlobalState : public GlobalFunctionData {
	unique_ptr<ParquetWriter> writer;
};

struct ParquetWriteLocalState : public LocalFunctionData {
	explicit ParquetWriteLocalState(ClientContext &context, const vector<LogicalType> &types)
	    : buffer(BufferAllocator::Get(context), types) {
		buffer.InitializeAppend(append_state);
	}

	ColumnDataCollection buffer;
	ColumnDataAppendState append_state;
};

BindInfo ParquetGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto bind_info = BindInfo(ScanType::PARQUET);
	auto &parquet_bind = bind_data->Cast<ParquetReadBindData>();

	vector<Value> file_path;
	for (const auto &file : parquet_bind.file_list->Files()) {
		file_path.emplace_back(file);
	}

	// LCOV_EXCL_START
	bind_info.InsertOption("file_path", Value::LIST(LogicalType::VARCHAR, file_path));
	bind_info.InsertOption("binary_as_string", Value::BOOLEAN(parquet_bind.parquet_options.binary_as_string));
	bind_info.InsertOption("file_row_number", Value::BOOLEAN(parquet_bind.parquet_options.file_row_number));
	bind_info.InsertOption("debug_use_openssl", Value::BOOLEAN(parquet_bind.parquet_options.debug_use_openssl));
	parquet_bind.parquet_options.file_options.AddBatchInfo(bind_info);
	// LCOV_EXCL_STOP
	return bind_info;
}

static void ParseFileRowNumberOption(MultiFileReaderBindData &bind_data, ParquetOptions &options,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (options.file_row_number) {
		if (StringUtil::CIFind(names, "file_row_number") != DConstants::INVALID_INDEX) {
			throw BinderException(
			    "Using file_row_number option on file with column named file_row_number is not supported");
		}

		bind_data.file_row_number_idx = names.size();
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("file_row_number");
	}
}

static MultiFileReaderBindData BindSchema(ClientContext &context, vector<LogicalType> &return_types,
                                          vector<string> &names, ParquetReadBindData &result, ParquetOptions &options) {
	D_ASSERT(!options.schema.empty());

	options.file_options.AutoDetectHivePartitioning(*result.file_list, context);

	auto &file_options = options.file_options;
	if (file_options.union_by_name || file_options.hive_partitioning) {
		throw BinderException("Parquet schema cannot be combined with union_by_name=true or hive_partitioning=true");
	}

	vector<string> schema_col_names;
	vector<LogicalType> schema_col_types;
	MultiFileReaderBindData bind_data;
	schema_col_names.reserve(options.schema.size());
	schema_col_types.reserve(options.schema.size());
	bool match_by_field_id;
	if (!options.schema.empty()) {
		auto &column = options.schema[0];
		if (column.identifier.type().id() == LogicalTypeId::INTEGER) {
			match_by_field_id = true;
		} else {
			match_by_field_id = false;
		}
	} else {
		match_by_field_id = false;
	}

	for (idx_t i = 0; i < options.schema.size(); i++) {
		const auto &column = options.schema[i];
		schema_col_names.push_back(column.name);
		schema_col_types.push_back(column.type);

		auto res = MultiFileReaderColumnDefinition(column.name, column.type);
		res.identifier = column.identifier;
#ifdef DEBUG
		if (match_by_field_id) {
			D_ASSERT(res.identifier.type().id() == LogicalTypeId::INTEGER);
		} else {
			D_ASSERT(res.identifier.type().id() == LogicalTypeId::VARCHAR);
		}
#endif

		res.default_expression = make_uniq<ConstantExpression>(column.default_value);
		bind_data.schema.emplace_back(std::move(res));
	}

	if (match_by_field_id) {
		bind_data.mapping = MultiFileReaderColumnMappingMode::BY_FIELD_ID;
	} else {
		bind_data.mapping = MultiFileReaderColumnMappingMode::BY_NAME;
	}

	// perform the binding on the obtained set of names + types
	result.multi_file_reader->BindOptions(options.file_options, *result.file_list, schema_col_types, schema_col_names,
	                                      bind_data);

	names = schema_col_names;
	return_types = schema_col_types;
	D_ASSERT(names.size() == return_types.size());

	ParseFileRowNumberOption(bind_data, options, return_types, names);

	return bind_data;
}

static void InitializeParquetReader(ParquetReader &reader, const ParquetReadBindData &bind_data,
                                    const vector<ColumnIndex> &global_column_ids,
                                    optional_ptr<TableFilterSet> table_filters, ClientContext &context,
                                    optional_idx file_idx, optional_ptr<MultiFileReaderGlobalState> reader_state) {
	auto &parquet_options = bind_data.parquet_options;
	auto &reader_data = reader.reader_data;

	reader.table_columns = bind_data.table_columns;
	// Mark the file in the file list we are scanning here
	reader_data.file_list_idx = file_idx;

	// 'reader_bind.schema' could be set explicitly by:
	// 1. The MultiFileReader::Bind call
	// 2. The 'schema' parquet option
	auto &global_columns = bind_data.reader_bind.schema.empty() ? bind_data.columns : bind_data.reader_bind.schema;
	bind_data.multi_file_reader->InitializeReader(reader, parquet_options.file_options, bind_data.reader_bind,
	                                              global_columns, global_column_ids, table_filters,
	                                              bind_data.file_list->GetFirstFile(), context, reader_state);
}

static bool GetBooleanArgument(const pair<string, vector<Value>> &option) {
	if (option.second.empty()) {
		return true;
	}
	Value boolean_value;
	string error_message;
	if (!option.second[0].DefaultTryCastAs(LogicalType::BOOLEAN, boolean_value, &error_message)) {
		throw InvalidInputException("Unable to cast \"%s\" to BOOLEAN for Parquet option \"%s\"",
		                            option.second[0].ToString(), option.first);
	}
	return BooleanValue::Get(boolean_value);
}

TablePartitionInfo ParquetGetPartitionInfo(ClientContext &context, TableFunctionPartitionInput &input) {
	auto &parquet_bind = input.bind_data->Cast<ParquetReadBindData>();
	return parquet_bind.multi_file_reader->GetPartitionInfo(context, parquet_bind.reader_bind, input);
}

class ParquetScanFunction {
public:
	static TableFunctionSet GetFunctionSet() {
		TableFunction table_function("parquet_scan", {LogicalType::VARCHAR}, ParquetScanImplementation, ParquetScanBind,
		                             ParquetScanInitGlobal, ParquetScanInitLocal);
		table_function.statistics = ParquetScanStats;
		table_function.cardinality = ParquetCardinality;
		table_function.table_scan_progress = ParquetProgress;
		table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
		table_function.named_parameters["file_row_number"] = LogicalType::BOOLEAN;
		table_function.named_parameters["debug_use_openssl"] = LogicalType::BOOLEAN;
		table_function.named_parameters["compression"] = LogicalType::VARCHAR;
		table_function.named_parameters["explicit_cardinality"] = LogicalType::UBIGINT;
		table_function.named_parameters["schema"] = LogicalTypeId::ANY;
		table_function.named_parameters["encryption_config"] = LogicalTypeId::ANY;
		table_function.named_parameters["parquet_version"] = LogicalType::VARCHAR;
		table_function.get_partition_data = ParquetScanGetPartitionData;
		table_function.serialize = ParquetScanSerialize;
		table_function.deserialize = ParquetScanDeserialize;
		table_function.get_bind_info = ParquetGetBindInfo;
		table_function.projection_pushdown = true;
		table_function.filter_pushdown = true;
		table_function.filter_prune = true;
		table_function.pushdown_complex_filter = ParquetComplexFilterPushdown;
		table_function.get_partition_info = ParquetGetPartitionInfo;

		MultiFileReader::AddParameters(table_function);

		return MultiFileReader::CreateFunctionSet(table_function);
	}

	static unique_ptr<FunctionData> ParquetReadBind(ClientContext &context, CopyInfo &info,
	                                                vector<string> &expected_names,
	                                                vector<LogicalType> &expected_types) {
		D_ASSERT(expected_names.size() == expected_types.size());
		ParquetOptions parquet_options(context);

		for (auto &option : info.options) {
			auto loption = StringUtil::Lower(option.first);
			if (loption == "compression" || loption == "codec" || loption == "row_group_size") {
				// CODEC/COMPRESSION and ROW_GROUP_SIZE options have no effect on parquet read.
				// These options are determined from the file.
				continue;
			} else if (loption == "binary_as_string") {
				parquet_options.binary_as_string = GetBooleanArgument(option);
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = GetBooleanArgument(option);
			} else if (loption == "debug_use_openssl") {
				parquet_options.debug_use_openssl = GetBooleanArgument(option);
			} else if (loption == "encryption_config") {
				if (option.second.size() != 1) {
					throw BinderException("Parquet encryption_config cannot be empty!");
				}
				parquet_options.encryption_config = ParquetEncryptionConfig::Create(context, option.second[0]);
			} else {
				throw NotImplementedException("Unsupported option for COPY FROM parquet: %s", option.first);
			}
		}

		// TODO: Allow overriding the MultiFileReader for COPY FROM?
		auto multi_file_reader = MultiFileReader::CreateDefault("ParquetCopy");
		vector<string> paths = {info.file_path};
		auto file_list = multi_file_reader->CreateFileList(context, paths);

		return ParquetScanBindInternal(context, std::move(multi_file_reader), std::move(file_list), expected_types,
		                               expected_names, parquet_options);
	}

	static unique_ptr<BaseStatistics> ParquetScanStats(ClientContext &context, const FunctionData *bind_data_p,
	                                                   column_t column_index) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();

		if (IsRowIdColumnId(column_index)) {
			return nullptr;
		}

		// NOTE: we do not want to parse the Parquet metadata for the sole purpose of getting column statistics
		if (bind_data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
			// multiple files, no luck!
			return nullptr;
		}
		if (!bind_data.initial_reader) {
			// no reader
			return nullptr;
		}
		// scanning single parquet file and we have the metadata read already
		return bind_data.initial_reader->ReadStatistics(bind_data.names[column_index]);

		return nullptr;
	}

	static unique_ptr<FunctionData> ParquetScanBindInternal(ClientContext &context,
	                                                        unique_ptr<MultiFileReader> multi_file_reader,
	                                                        shared_ptr<MultiFileList> file_list,
	                                                        vector<LogicalType> &return_types, vector<string> &names,
	                                                        ParquetOptions parquet_options) {
		auto result = make_uniq<ParquetReadBindData>();
		result->multi_file_reader = std::move(multi_file_reader);
		result->file_list = std::move(file_list);

		bool bound_on_first_file = true;
		if (result->multi_file_reader->Bind(parquet_options.file_options, *result->file_list, result->types,
		                                    result->names, result->reader_bind)) {
			result->multi_file_reader->BindOptions(parquet_options.file_options, *result->file_list, result->types,
			                                       result->names, result->reader_bind);
			// Enable the parquet file_row_number on the parquet options if the file_row_number_idx was set
			if (result->reader_bind.file_row_number_idx != DConstants::INVALID_INDEX) {
				parquet_options.file_row_number = true;
			}
			bound_on_first_file = false;
		} else if (!parquet_options.schema.empty()) {
			// A schema was supplied: use the schema for binding
			result->reader_bind = BindSchema(context, result->types, result->names, *result, parquet_options);
		} else {
			parquet_options.file_options.AutoDetectHivePartitioning(*result->file_list, context);
			// Default bind
			result->reader_bind = result->multi_file_reader->BindReader<ParquetReader>(
			    context, result->types, result->names, *result->file_list, *result, parquet_options);
		}

		// Set the explicit cardinality if requested
		if (parquet_options.explicit_cardinality) {
			auto file_count = result->file_list->GetTotalFileCount();
			result->explicit_cardinality = parquet_options.explicit_cardinality;
			result->initial_file_cardinality = result->explicit_cardinality / (file_count ? file_count : 1);
		}

		if (return_types.empty()) {
			// no expected types - just copy the types
			return_types = result->types;
			names = result->names;
		} else {
			// We're deserializing from a previously successful bind call
			// verify that the amount of columns still matches
			if (return_types.size() != result->types.size()) {
				auto file_string = bound_on_first_file ? result->file_list->GetFirstFile()
				                                       : StringUtil::Join(result->file_list->GetPaths(), ",");
				string extended_error;
				extended_error = "Table schema: ";
				for (idx_t col_idx = 0; col_idx < return_types.size(); col_idx++) {
					if (col_idx > 0) {
						extended_error += ", ";
					}
					extended_error += names[col_idx] + " " + return_types[col_idx].ToString();
				}
				extended_error += "\nParquet schema: ";
				for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
					if (col_idx > 0) {
						extended_error += ", ";
					}
					extended_error += result->names[col_idx] + " " + result->types[col_idx].ToString();
				}
				extended_error += "\n\nPossible solutions:";
				extended_error += "\n* Manually specify which columns to insert using \"INSERT INTO tbl SELECT ... "
				                  "FROM read_parquet(...)\"";
				throw ConversionException(
				    "Failed to read file(s) \"%s\" - column count mismatch: expected %d columns but found %d\n%s",
				    file_string, return_types.size(), result->types.size(), extended_error);
			}
			// expected types - overwrite the types we want to read instead
			result->types = return_types;
			result->table_columns = names;
		}
		result->parquet_options = parquet_options;
		result->columns = MultiFileReaderColumnDefinition::ColumnsFromNamesAndTypes(result->names, result->types);
		return std::move(result);
	}

	static void VerifyParquetSchemaParameter(const Value &schema) {
		LogicalType::MAP(LogicalType::BLOB, LogicalType::STRUCT({{{"name", LogicalType::VARCHAR},
		                                                          {"type", LogicalType::VARCHAR},
		                                                          {"default_value", LogicalType::VARCHAR}}}));
		auto &map_type = schema.type();
		if (map_type.id() != LogicalTypeId::MAP) {
			throw InvalidInputException("'schema' expects a value of type MAP, not %s",
			                            LogicalTypeIdToString(map_type.id()));
		}
		auto &key_type = MapType::KeyType(map_type);
		auto &value_type = MapType::ValueType(map_type);

		if (value_type.id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("'schema' expects a STRUCT as the value type of the map");
		}
		auto &children = StructType::GetChildTypes(value_type);
		if (children.size() < 3) {
			throw InvalidInputException(
			    "'schema' expects the STRUCT to have 3 children, 'name', 'type' and 'default_value");
		}
		if (!StringUtil::CIEquals(children[0].first, "name")) {
			throw InvalidInputException("'schema' expects the first field of the struct to be called 'name'");
		}
		if (children[0].second.id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException("'schema' expects the 'name' field to be of type VARCHAR, not %s",
			                            LogicalTypeIdToString(children[0].second.id()));
		}
		if (!StringUtil::CIEquals(children[1].first, "type")) {
			throw InvalidInputException("'schema' expects the second field of the struct to be called 'type'");
		}
		if (children[1].second.id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException("'schema' expects the 'type' field to be of type VARCHAR, not %s",
			                            LogicalTypeIdToString(children[1].second.id()));
		}
		if (!StringUtil::CIEquals(children[2].first, "default_value")) {
			throw InvalidInputException("'schema' expects the third field of the struct to be called 'default_value'");
		}
		//! NOTE: default_value can be any type

		if (key_type.id() != LogicalTypeId::INTEGER && key_type.id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException(
			    "'schema' expects the value type of the map to be either INTEGER or VARCHAR, not %s",
			    LogicalTypeIdToString(key_type.id()));
		}
	}

	static unique_ptr<FunctionData> ParquetScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names) {
		auto multi_file_reader = MultiFileReader::Create(input.table_function);

		ParquetOptions parquet_options(context);
		for (auto &kv : input.named_parameters) {
			if (kv.second.IsNull()) {
				throw BinderException("Cannot use NULL as function argument");
			}
			auto loption = StringUtil::Lower(kv.first);
			if (multi_file_reader->ParseOption(kv.first, kv.second, parquet_options.file_options, context)) {
				continue;
			}
			if (loption == "binary_as_string") {
				parquet_options.binary_as_string = BooleanValue::Get(kv.second);
			} else if (loption == "file_row_number") {
				parquet_options.file_row_number = BooleanValue::Get(kv.second);
			} else if (loption == "debug_use_openssl") {
				parquet_options.debug_use_openssl = BooleanValue::Get(kv.second);
			} else if (loption == "schema") {
				// Argument is a map that defines the schema
				const auto &schema_value = kv.second;
				VerifyParquetSchemaParameter(schema_value);
				const auto column_values = ListValue::GetChildren(schema_value);
				if (column_values.empty()) {
					throw BinderException("Parquet schema cannot be empty");
				}
				parquet_options.schema.reserve(column_values.size());
				for (idx_t i = 0; i < column_values.size(); i++) {
					parquet_options.schema.emplace_back(
					    ParquetColumnDefinition::FromSchemaValue(context, column_values[i]));
				}

				// cannot be combined with hive_partitioning=true, so we disable auto-detection
				parquet_options.file_options.auto_detect_hive_partitioning = false;
			} else if (loption == "explicit_cardinality") {
				parquet_options.explicit_cardinality = UBigIntValue::Get(kv.second);
			} else if (loption == "encryption_config") {
				parquet_options.encryption_config = ParquetEncryptionConfig::Create(context, kv.second);
			}
		}

		auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0]);
		return ParquetScanBindInternal(context, std::move(multi_file_reader), std::move(file_list), return_types, names,
		                               parquet_options);
	}

	static double ParquetProgress(ClientContext &context, const FunctionData *bind_data_p,
	                              const GlobalTableFunctionState *global_state) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();
		auto &gstate = global_state->Cast<ParquetReadGlobalState>();

		auto total_count = gstate.file_list.GetTotalFileCount();
		if (total_count == 0) {
			return 100.0;
		}
		if (bind_data.initial_file_cardinality == 0) {
			return (100.0 * (static_cast<double>(gstate.file_index) + 1.0)) / static_cast<double>(total_count);
		}
		auto percentage = MinValue<double>(100.0, (static_cast<double>(bind_data.chunk_count) * STANDARD_VECTOR_SIZE *
		                                           100.0 / static_cast<double>(bind_data.initial_file_cardinality)));
		return (percentage + 100.0 * static_cast<double>(gstate.file_index)) / static_cast<double>(total_count);
	}

	static unique_ptr<LocalTableFunctionState>
	ParquetScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p) {
		auto &bind_data = input.bind_data->Cast<ParquetReadBindData>();
		auto &gstate = gstate_p->Cast<ParquetReadGlobalState>();

		auto result = make_uniq<ParquetReadLocalState>();
		result->is_parallel = true;
		result->batch_index = 0;

		if (gstate.CanRemoveColumns()) {
			result->all_columns.Initialize(context.client, gstate.scanned_types);
		}
		if (!ParquetParallelStateNext(context.client, bind_data, *result, gstate)) {
			return nullptr;
		}
		return std::move(result);
	}

	static unique_ptr<MultiFileList> ParquetDynamicFilterPushdown(ClientContext &context,
	                                                              const ParquetReadBindData &data,
	                                                              const vector<column_t> &column_ids,
	                                                              optional_ptr<TableFilterSet> filters) {
		if (!filters) {
			return nullptr;
		}
		auto new_list = data.multi_file_reader->DynamicFilterPushdown(
		    context, *data.file_list, data.parquet_options.file_options, data.names, data.types, column_ids, *filters);
		return new_list;
	}

	static unique_ptr<GlobalTableFunctionState> ParquetScanInitGlobal(ClientContext &context,
	                                                                  TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->CastNoConst<ParquetReadBindData>();
		unique_ptr<ParquetReadGlobalState> result;

		// before instantiating a scan trigger a dynamic filter pushdown if possible
		auto new_list = ParquetDynamicFilterPushdown(context, bind_data, input.column_ids, input.filters);
		if (new_list) {
			result = make_uniq<ParquetReadGlobalState>(std::move(new_list));
		} else {
			result = make_uniq<ParquetReadGlobalState>(*bind_data.file_list);
		}
		auto &file_list = result->file_list;
		file_list.InitializeScan(result->file_list_scan);

		auto &global_columns = bind_data.reader_bind.schema.empty() ? bind_data.columns : bind_data.reader_bind.schema;
		result->multi_file_reader_state = bind_data.multi_file_reader->InitializeGlobalState(
		    context, bind_data.parquet_options.file_options, bind_data.reader_bind, file_list, global_columns,
		    input.column_indexes);

		if (file_list.IsEmpty()) {
			result->readers = {};
		} else if (!bind_data.union_readers.empty()) {
			// TODO: confirm we are not changing behaviour by modifying the order here?
			for (auto &reader : bind_data.union_readers) {
				if (!reader) {
					break;
				}
				result->readers.push_back(make_uniq<ParquetFileReaderData>(std::move(reader)));
			}
			if (result->readers.size() != file_list.GetTotalFileCount()) {
				// This case happens with recursive CTEs: the first execution the readers have already
				// been moved out of the bind data.
				// FIXME: clean up this process and make it more explicit
				result->readers = {};
			}
		} else if (bind_data.initial_reader) {
			// we can only use the initial reader if it was constructed from the first file
			if (bind_data.initial_reader->file_name == file_list.GetFirstFile()) {
				result->readers.push_back(make_uniq<ParquetFileReaderData>(std::move(bind_data.initial_reader)));
			}
		}

		// Ensure all readers are initialized and FileListScan is sync with readers list
		for (auto &reader_data : result->readers) {
			string file_name;
			idx_t file_idx = result->file_list_scan.current_file_idx;
			file_list.Scan(result->file_list_scan, file_name);
			if (reader_data->union_data) {
				if (file_name != reader_data->union_data->GetFileName()) {
					throw InternalException("Mismatch in filename order and union reader order in parquet scan");
				}
			} else {
				D_ASSERT(reader_data->reader);
				if (file_name != reader_data->reader->file_name) {
					throw InternalException("Mismatch in filename order and reader order in parquet scan");
				}
				InitializeParquetReader(*reader_data->reader, bind_data, input.column_indexes, input.filters, context,
				                        file_idx, result->multi_file_reader_state);
			}
		}

		result->column_indexes = input.column_indexes;
		result->filters = input.filters.get();
		result->row_group_index = 0;
		result->file_index = 0;
		result->batch_index = 0;
		result->max_threads = ParquetScanMaxThreads(context, input.bind_data.get());

		bool require_extra_columns =
		    result->multi_file_reader_state && result->multi_file_reader_state->RequiresExtraColumns();
		if (input.CanRemoveFilterColumns() || require_extra_columns) {
			if (!input.projection_ids.empty()) {
				result->projection_ids = input.projection_ids;
			} else {
				result->projection_ids.resize(input.column_indexes.size());
				iota(begin(result->projection_ids), end(result->projection_ids), 0);
			}

			const auto table_types = bind_data.types;
			for (const auto &col_idx : input.column_indexes) {
				if (col_idx.IsRowIdColumn()) {
					result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
				} else {
					result->scanned_types.push_back(table_types[col_idx.GetPrimaryIndex()]);
				}
			}
		}

		if (require_extra_columns) {
			for (const auto &column_type : result->multi_file_reader_state->extra_columns) {
				result->scanned_types.push_back(column_type);
			}
		}

		return std::move(result);
	}

	static OperatorPartitionData ParquetScanGetPartitionData(ClientContext &context,
	                                                         TableFunctionGetPartitionInput &input) {
		auto &bind_data = input.bind_data->CastNoConst<ParquetReadBindData>();
		auto &data = input.local_state->Cast<ParquetReadLocalState>();
		auto &gstate = input.global_state->Cast<ParquetReadGlobalState>();
		OperatorPartitionData partition_data(data.batch_index);
		bind_data.multi_file_reader->GetPartitionData(context, bind_data.reader_bind, data.reader->reader_data,
		                                              gstate.multi_file_reader_state, input.partition_info,
		                                              partition_data);
		return partition_data;
	}

	static void ParquetScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                 const TableFunction &function) {
		auto &bind_data = bind_data_p->Cast<ParquetReadBindData>();

		serializer.WriteProperty(100, "files", bind_data.file_list->GetAllFiles());
		serializer.WriteProperty(101, "types", bind_data.types);
		serializer.WriteProperty(102, "names", bind_data.names);
		serializer.WriteProperty(103, "parquet_options", bind_data.parquet_options);
		if (serializer.ShouldSerialize(3)) {
			serializer.WriteProperty(104, "table_columns", bind_data.table_columns);
		}
	}

	static unique_ptr<FunctionData> ParquetScanDeserialize(Deserializer &deserializer, TableFunction &function) {
		auto &context = deserializer.Get<ClientContext &>();
		auto files = deserializer.ReadProperty<vector<string>>(100, "files");
		auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
		auto names = deserializer.ReadProperty<vector<string>>(102, "names");
		auto parquet_options = deserializer.ReadProperty<ParquetOptions>(103, "parquet_options");
		auto table_columns =
		    deserializer.ReadPropertyWithExplicitDefault<vector<string>>(104, "table_columns", vector<string> {});

		vector<Value> file_path;
		for (auto &path : files) {
			file_path.emplace_back(path);
		}

		auto multi_file_reader = MultiFileReader::Create(function);
		auto file_list = multi_file_reader->CreateFileList(context, Value::LIST(LogicalType::VARCHAR, file_path),
		                                                   FileGlobOptions::DISALLOW_EMPTY);
		auto bind_data = ParquetScanBindInternal(context, std::move(multi_file_reader), std::move(file_list), types,
		                                         names, parquet_options);
		bind_data->Cast<ParquetReadBindData>().table_columns = std::move(table_columns);
		return bind_data;
	}

	static void ParquetScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		if (!data_p.local_state) {
			return;
		}
		auto &data = data_p.local_state->Cast<ParquetReadLocalState>();
		auto &gstate = data_p.global_state->Cast<ParquetReadGlobalState>();
		auto &bind_data = data_p.bind_data->CastNoConst<ParquetReadBindData>();

		bool rowgroup_finished;
		do {
			if (gstate.CanRemoveColumns()) {
				data.all_columns.Reset();
				data.reader->Scan(data.scan_state, data.all_columns);
				rowgroup_finished = data.all_columns.size() == 0;
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                           data.all_columns, gstate.multi_file_reader_state);
				output.ReferenceColumns(data.all_columns, gstate.projection_ids);
			} else {
				data.reader->Scan(data.scan_state, output);
				rowgroup_finished = output.size() == 0;
				bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind, data.reader->reader_data,
				                                           output, gstate.multi_file_reader_state);
			}

			bind_data.chunk_count++;
			if (output.size() > 0) {
				return;
			}
			if (rowgroup_finished && !ParquetParallelStateNext(context, bind_data, data, gstate)) {
				return;
			}
		} while (true);
	}

	static unique_ptr<NodeStatistics> ParquetCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<ParquetReadBindData>();
		if (data.explicit_cardinality) {
			return make_uniq<NodeStatistics>(data.explicit_cardinality);
		}
		auto file_list_cardinality_estimate = data.file_list->GetCardinality(context);
		if (file_list_cardinality_estimate) {
			return file_list_cardinality_estimate;
		}
		return make_uniq<NodeStatistics>(MaxValue(data.initial_file_cardinality, (idx_t)1) *
		                                 data.file_list->GetTotalFileCount());
	}

	static idx_t ParquetScanMaxThreads(ClientContext &context, const FunctionData *bind_data) {
		auto &data = bind_data->Cast<ParquetReadBindData>();

		if (data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
			return TaskScheduler::GetScheduler(context).NumberOfThreads();
		}

		return MaxValue(data.initial_file_row_groups, (idx_t)1);
	}

	// Queries the metadataprovider for another file to scan, updating the files/reader lists in the process.
	// Returns true if resized
	static bool ResizeFiles(ParquetReadGlobalState &parallel_state) {
		string scanned_file;
		if (!parallel_state.file_list.Scan(parallel_state.file_list_scan, scanned_file)) {
			return false;
		}

		// Push the file in the reader data, to be opened later
		parallel_state.readers.push_back(make_uniq<ParquetFileReaderData>(scanned_file));

		return true;
	}

	// This function looks for the next available row group. If not available, it will open files from bind_data.files
	// until there is a row group available for scanning or the files runs out
	static bool ParquetParallelStateNext(ClientContext &context, const ParquetReadBindData &bind_data,
	                                     ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state) {
		unique_lock<mutex> parallel_lock(parallel_state.lock);

		while (true) {
			if (parallel_state.error_opening_file) {
				return false;
			}

			if (parallel_state.file_index >= parallel_state.readers.size() && !ResizeFiles(parallel_state)) {
				return false;
			}

			auto &current_reader_data = *parallel_state.readers[parallel_state.file_index];
			if (current_reader_data.file_state == ParquetFileState::OPEN) {
				if (parallel_state.row_group_index < current_reader_data.reader->NumRowGroups()) {
					// The current reader has rowgroups left to be scanned
					scan_data.reader = current_reader_data.reader;
					vector<idx_t> group_indexes {parallel_state.row_group_index};
					scan_data.reader->InitializeScan(context, scan_data.scan_state, group_indexes);
					scan_data.batch_index = parallel_state.batch_index++;
					scan_data.file_index = parallel_state.file_index;
					parallel_state.row_group_index++;
					return true;
				} else {
					// Close current file
					current_reader_data.file_state = ParquetFileState::CLOSED;
					current_reader_data.reader = nullptr;

					// Set state to the next file
					parallel_state.file_index++;
					parallel_state.row_group_index = 0;

					continue;
				}
			}

			if (TryOpenNextFile(context, bind_data, scan_data, parallel_state, parallel_lock)) {
				continue;
			}

			// Check if the current file is being opened, in that case we need to wait for it.
			if (current_reader_data.file_state == ParquetFileState::OPENING) {
				WaitForFile(parallel_state.file_index, parallel_state, parallel_lock);
			}
		}
	}

	static void ParquetComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                         vector<unique_ptr<Expression>> &filters) {
		auto &data = bind_data_p->Cast<ParquetReadBindData>();

		MultiFilePushdownInfo info(get);
		auto new_list = data.multi_file_reader->ComplexFilterPushdown(context, *data.file_list,
		                                                              data.parquet_options.file_options, info, filters);

		if (new_list) {
			data.file_list = std::move(new_list);
			MultiFileReader::PruneReaders(data, *data.file_list);
		}
	}

	//! Wait for a file to become available. Parallel lock should be locked when calling.
	static void WaitForFile(idx_t file_index, ParquetReadGlobalState &parallel_state,
	                        unique_lock<mutex> &parallel_lock) {
		while (true) {
			// Get pointer to file mutex before unlocking
			auto &file_mutex = *parallel_state.readers[file_index]->file_mutex;

			// To get the file lock, we first need to release the parallel_lock to prevent deadlocking. Note that this
			// requires getting the ref to the file mutex pointer with the lock stil held: readers get be resized
			parallel_lock.unlock();
			unique_lock<mutex> current_file_lock(file_mutex);
			parallel_lock.lock();

			// Here we have both locks which means we can stop waiting if:
			// - the thread opening the file is done and the file is available
			// - the thread opening the file has failed
			// - the file was somehow scanned till the end while we were waiting
			if (parallel_state.file_index >= parallel_state.readers.size() ||
			    parallel_state.readers[parallel_state.file_index]->file_state != ParquetFileState::OPENING ||
			    parallel_state.error_opening_file) {
				return;
			}
		}
	}

	//! Helper function that try to start opening a next file. Parallel lock should be locked when calling.
	static bool TryOpenNextFile(ClientContext &context, const ParquetReadBindData &bind_data,
	                            ParquetReadLocalState &scan_data, ParquetReadGlobalState &parallel_state,
	                            unique_lock<mutex> &parallel_lock) {
		const auto file_index_limit =
		    parallel_state.file_index + TaskScheduler::GetScheduler(context).NumberOfThreads();

		for (idx_t i = parallel_state.file_index; i < file_index_limit; i++) {
			// We check if we can resize files in this loop too otherwise we will only ever open 1 file ahead
			if (i >= parallel_state.readers.size() && !ResizeFiles(parallel_state)) {
				return false;
			}

			auto &current_reader_data = *parallel_state.readers[i];
			if (current_reader_data.file_state == ParquetFileState::UNOPENED) {
				current_reader_data.file_state = ParquetFileState::OPENING;
				auto pq_options = bind_data.parquet_options;

				// Get pointer to file mutex before unlocking
				auto &current_file_lock = *current_reader_data.file_mutex;

				// Now we switch which lock we are holding, instead of locking the global state, we grab the lock on
				// the file we are opening. This file lock allows threads to wait for a file to be opened.
				parallel_lock.unlock();
				unique_lock<mutex> file_lock(current_file_lock);

				shared_ptr<ParquetReader> reader;
				try {
					if (current_reader_data.union_data) {
						auto &union_data = *current_reader_data.union_data;
						reader = make_shared_ptr<ParquetReader>(context, union_data.file_name, union_data.options,
						                                        union_data.metadata);
					} else {
						reader =
						    make_shared_ptr<ParquetReader>(context, current_reader_data.file_to_be_opened, pq_options);
					}
					InitializeParquetReader(*reader, bind_data, parallel_state.column_indexes, parallel_state.filters,
					                        context, i, parallel_state.multi_file_reader_state);
				} catch (...) {
					parallel_lock.lock();
					parallel_state.error_opening_file = true;
					throw;
				}

				// Now re-lock the state and add the reader
				parallel_lock.lock();
				current_reader_data.reader = std::move(reader);
				current_reader_data.file_state = ParquetFileState::OPEN;

				return true;
			}
		}

		return false;
	}
};

static case_insensitive_map_t<LogicalType> GetChildNameToTypeMap(const LogicalType &type) {
	case_insensitive_map_t<LogicalType> name_to_type_map;
	switch (type.id()) {
	case LogicalTypeId::LIST:
		name_to_type_map.emplace("element", ListType::GetChildType(type));
		break;
	case LogicalTypeId::MAP:
		name_to_type_map.emplace("key", MapType::KeyType(type));
		name_to_type_map.emplace("value", MapType::ValueType(type));
		break;
	case LogicalTypeId::STRUCT:
		for (auto &child_type : StructType::GetChildTypes(type)) {
			if (child_type.first == FieldID::DUCKDB_FIELD_ID) {
				throw BinderException("Cannot have column named \"%s\" with FIELD_IDS", FieldID::DUCKDB_FIELD_ID);
			}
			name_to_type_map.emplace(child_type);
		}
		break;
	default: // LCOV_EXCL_START
		throw InternalException("Unexpected type in GetChildNameToTypeMap");
	} // LCOV_EXCL_STOP
	return name_to_type_map;
}

static void GetChildNamesAndTypes(const LogicalType &type, vector<string> &child_names,
                                  vector<LogicalType> &child_types) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
		child_names.emplace_back("element");
		child_types.emplace_back(ListType::GetChildType(type));
		break;
	case LogicalTypeId::MAP:
		child_names.emplace_back("key");
		child_names.emplace_back("value");
		child_types.emplace_back(MapType::KeyType(type));
		child_types.emplace_back(MapType::ValueType(type));
		break;
	case LogicalTypeId::STRUCT:
		for (auto &child_type : StructType::GetChildTypes(type)) {
			child_names.emplace_back(child_type.first);
			child_types.emplace_back(child_type.second);
		}
		break;
	default: // LCOV_EXCL_START
		throw InternalException("Unexpected type in GetChildNamesAndTypes");
	} // LCOV_EXCL_STOP
}

static void GenerateFieldIDs(ChildFieldIDs &field_ids, idx_t &field_id, const vector<string> &names,
                             const vector<LogicalType> &sql_types) {
	D_ASSERT(names.size() == sql_types.size());
	for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
		const auto &col_name = names[col_idx];
		auto inserted = field_ids.ids->insert(make_pair(col_name, FieldID(UnsafeNumericCast<int32_t>(field_id++))));
		D_ASSERT(inserted.second);

		const auto &col_type = sql_types[col_idx];
		if (col_type.id() != LogicalTypeId::LIST && col_type.id() != LogicalTypeId::MAP &&
		    col_type.id() != LogicalTypeId::STRUCT) {
			continue;
		}

		// Cannot use GetChildNameToTypeMap here because we lose order, and we want to generate depth-first
		vector<string> child_names;
		vector<LogicalType> child_types;
		GetChildNamesAndTypes(col_type, child_names, child_types);

		GenerateFieldIDs(inserted.first->second.child_field_ids, field_id, child_names, child_types);
	}
}

static void GetFieldIDs(const Value &field_ids_value, ChildFieldIDs &field_ids,
                        unordered_set<uint32_t> &unique_field_ids,
                        const case_insensitive_map_t<LogicalType> &name_to_type_map) {
	const auto &struct_type = field_ids_value.type();
	if (struct_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException(
		    "Expected FIELD_IDS to be a STRUCT, e.g., {col1: 42, col2: {%s: 43, nested_col: 44}, col3: 44}",
		    FieldID::DUCKDB_FIELD_ID);
	}
	const auto &struct_children = StructValue::GetChildren(field_ids_value);
	D_ASSERT(StructType::GetChildTypes(struct_type).size() == struct_children.size());
	for (idx_t i = 0; i < struct_children.size(); i++) {
		const auto &col_name = StringUtil::Lower(StructType::GetChildName(struct_type, i));
		if (col_name == FieldID::DUCKDB_FIELD_ID) {
			continue;
		}

		auto it = name_to_type_map.find(col_name);
		if (it == name_to_type_map.end()) {
			string names;
			for (const auto &name : name_to_type_map) {
				if (!names.empty()) {
					names += ", ";
				}
				names += name.first;
			}
			throw BinderException(
			    "Column name \"%s\" specified in FIELD_IDS not found. Consider using WRITE_PARTITION_COLUMNS if this "
			    "column is a partition column. Available column names: [%s]",
			    col_name, names);
		}
		D_ASSERT(field_ids.ids->find(col_name) == field_ids.ids->end()); // Caught by STRUCT - deduplicates keys

		const auto &child_value = struct_children[i];
		const auto &child_type = child_value.type();
		optional_ptr<const Value> field_id_value;
		optional_ptr<const Value> child_field_ids_value;

		if (child_type.id() == LogicalTypeId::STRUCT) {
			const auto &nested_children = StructValue::GetChildren(child_value);
			D_ASSERT(StructType::GetChildTypes(child_type).size() == nested_children.size());
			for (idx_t nested_i = 0; nested_i < nested_children.size(); nested_i++) {
				const auto &field_id_or_nested_col = StructType::GetChildName(child_type, nested_i);
				if (field_id_or_nested_col == FieldID::DUCKDB_FIELD_ID) {
					field_id_value = &nested_children[nested_i];
				} else {
					child_field_ids_value = &child_value;
				}
			}
		} else {
			field_id_value = &child_value;
		}

		FieldID field_id;
		if (field_id_value) {
			Value field_id_integer_value = field_id_value->DefaultCastAs(LogicalType::INTEGER);
			const uint32_t field_id_int = IntegerValue::Get(field_id_integer_value);
			if (!unique_field_ids.insert(field_id_int).second) {
				throw BinderException("Duplicate field_id %s found in FIELD_IDS", field_id_integer_value.ToString());
			}
			field_id = FieldID(UnsafeNumericCast<int32_t>(field_id_int));
		}
		auto inserted = field_ids.ids->insert(make_pair(col_name, std::move(field_id)));
		D_ASSERT(inserted.second);

		if (child_field_ids_value) {
			const auto &col_type = it->second;
			if (col_type.id() != LogicalTypeId::LIST && col_type.id() != LogicalTypeId::MAP &&
			    col_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("Column \"%s\" with type \"%s\" cannot have a nested FIELD_IDS specification",
				                      col_name, LogicalTypeIdToString(col_type.id()));
			}

			GetFieldIDs(*child_field_ids_value, inserted.first->second.child_field_ids, unique_field_ids,
			            GetChildNameToTypeMap(col_type));
		}
	}
}

unique_ptr<FunctionData> ParquetWriteBind(ClientContext &context, CopyFunctionBindInput &input,
                                          const vector<string> &names, const vector<LogicalType> &sql_types) {
	D_ASSERT(names.size() == sql_types.size());
	bool row_group_size_bytes_set = false;
	bool compression_level_set = false;
	auto bind_data = make_uniq<ParquetWriteBindData>();
	for (auto &option : input.info.options) {
		const auto loption = StringUtil::Lower(option.first);
		if (option.second.size() != 1) {
			// All parquet write options require exactly one argument
			throw BinderException("%s requires exactly one argument", StringUtil::Upper(loption));
		}
		if (loption == "row_group_size" || loption == "chunk_size") {
			bind_data->row_group_size = option.second[0].GetValue<uint64_t>();
		} else if (loption == "row_group_size_bytes") {
			auto roption = option.second[0];
			if (roption.GetTypeMutable().id() == LogicalTypeId::VARCHAR) {
				bind_data->row_group_size_bytes = DBConfig::ParseMemoryLimit(roption.ToString());
			} else {
				bind_data->row_group_size_bytes = option.second[0].GetValue<uint64_t>();
			}
			row_group_size_bytes_set = true;
		} else if (loption == "row_groups_per_file") {
			bind_data->row_groups_per_file = option.second[0].GetValue<uint64_t>();
		} else if (loption == "compression" || loption == "codec") {
			const auto roption = StringUtil::Lower(option.second[0].ToString());
			if (roption == "uncompressed") {
				bind_data->codec = duckdb_parquet::CompressionCodec::UNCOMPRESSED;
			} else if (roption == "snappy") {
				bind_data->codec = duckdb_parquet::CompressionCodec::SNAPPY;
			} else if (roption == "gzip") {
				bind_data->codec = duckdb_parquet::CompressionCodec::GZIP;
			} else if (roption == "zstd") {
				bind_data->codec = duckdb_parquet::CompressionCodec::ZSTD;
			} else if (roption == "brotli") {
				bind_data->codec = duckdb_parquet::CompressionCodec::BROTLI;
			} else if (roption == "lz4" || roption == "lz4_raw") {
				/* LZ4 is technically another compression scheme, but deprecated and arrow also uses them
				 * interchangeably */
				bind_data->codec = duckdb_parquet::CompressionCodec::LZ4_RAW;
			} else {
				throw BinderException("Expected %s argument to be either [uncompressed, brotli, gzip, snappy, or zstd]",
				                      loption);
			}
		} else if (loption == "field_ids") {
			if (option.second[0].type().id() == LogicalTypeId::VARCHAR &&
			    StringUtil::Lower(StringValue::Get(option.second[0])) == "auto") {
				idx_t field_id = 0;
				GenerateFieldIDs(bind_data->field_ids, field_id, names, sql_types);
			} else {
				unordered_set<uint32_t> unique_field_ids;
				case_insensitive_map_t<LogicalType> name_to_type_map;
				for (idx_t col_idx = 0; col_idx < names.size(); col_idx++) {
					if (names[col_idx] == FieldID::DUCKDB_FIELD_ID) {
						throw BinderException("Cannot have a column named \"%s\" when writing FIELD_IDS",
						                      FieldID::DUCKDB_FIELD_ID);
					}
					name_to_type_map.emplace(names[col_idx], sql_types[col_idx]);
				}
				GetFieldIDs(option.second[0], bind_data->field_ids, unique_field_ids, name_to_type_map);
			}
		} else if (loption == "kv_metadata") {
			auto &kv_struct = option.second[0];
			auto &kv_struct_type = kv_struct.type();
			if (kv_struct_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("Expected kv_metadata argument to be a STRUCT");
			}
			auto values = StructValue::GetChildren(kv_struct);
			for (idx_t i = 0; i < values.size(); i++) {
				auto value = values[i];
				auto key = StructType::GetChildName(kv_struct_type, i);
				// If the value is a blob, write the raw blob bytes
				// otherwise, cast to string
				if (value.type().id() == LogicalTypeId::BLOB) {
					bind_data->kv_metadata.emplace_back(key, StringValue::Get(value));
				} else {
					bind_data->kv_metadata.emplace_back(key, value.ToString());
				}
			}
		} else if (loption == "encryption_config") {
			bind_data->encryption_config = ParquetEncryptionConfig::Create(context, option.second[0]);
		} else if (loption == "dictionary_compression_ratio_threshold") {
			// deprecated, ignore setting
		} else if (loption == "dictionary_size_limit") {
			auto val = option.second[0].GetValue<int64_t>();
			if (val < 0) {
				throw BinderException("dictionary_size_limit must be greater than 0 or 0 to disable");
			}
			bind_data->dictionary_size_limit = val;
		} else if (loption == "bloom_filter_false_positive_ratio") {
			auto val = option.second[0].GetValue<double>();
			if (val <= 0) {
				throw BinderException("bloom_filter_false_positive_ratio must be greater than 0");
			}
			bind_data->bloom_filter_false_positive_ratio = val;
		} else if (loption == "debug_use_openssl") {
			auto val = StringUtil::Lower(option.second[0].GetValue<std::string>());
			if (val == "false") {
				bind_data->debug_use_openssl = false;
			} else if (val == "true") {
				bind_data->debug_use_openssl = true;
			} else {
				throw BinderException("Expected debug_use_openssl to be a BOOLEAN");
			}
		} else if (loption == "compression_level") {
			const auto val = option.second[0].GetValue<int64_t>();
			if (val < ZStdFileSystem::MinimumCompressionLevel() || val > ZStdFileSystem::MaximumCompressionLevel()) {
				throw BinderException("Compression level must be between %lld and %lld",
				                      ZStdFileSystem::MinimumCompressionLevel(),
				                      ZStdFileSystem::MaximumCompressionLevel());
			}
			bind_data->compression_level = val;
			compression_level_set = true;
		} else if (loption == "parquet_version") {
			const auto roption = StringUtil::Upper(option.second[0].ToString());
			if (roption == "V1") {
				bind_data->parquet_version = ParquetVersion::V1;
			} else if (roption == "V2") {
				bind_data->parquet_version = ParquetVersion::V2;
			} else {
				throw BinderException("Expected parquet_version 'V1' or 'V2'");
			}
		} else {
			throw NotImplementedException("Unrecognized option for PARQUET: %s", option.first.c_str());
		}
	}
	if (row_group_size_bytes_set) {
		if (DBConfig::GetConfig(context).options.preserve_insertion_order) {
			throw BinderException("ROW_GROUP_SIZE_BYTES does not work while preserving insertion order. Use \"SET "
			                      "preserve_insertion_order=false;\" to disable preserving insertion order.");
		}
	}

	if (compression_level_set && bind_data->codec != CompressionCodec::ZSTD) {
		throw BinderException("Compression level is only supported for the ZSTD compression codec");
	}

	bind_data->sql_types = sql_types;
	bind_data->column_names = names;
	return std::move(bind_data);
}

unique_ptr<GlobalFunctionData> ParquetWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                            const string &file_path) {
	auto global_state = make_uniq<ParquetWriteGlobalState>();
	auto &parquet_bind = bind_data.Cast<ParquetWriteBindData>();

	auto &fs = FileSystem::GetFileSystem(context);
	global_state->writer = make_uniq<ParquetWriter>(
	    context, fs, file_path, parquet_bind.sql_types, parquet_bind.column_names, parquet_bind.codec,
	    parquet_bind.field_ids.Copy(), parquet_bind.kv_metadata, parquet_bind.encryption_config,
	    parquet_bind.dictionary_size_limit, parquet_bind.bloom_filter_false_positive_ratio,
	    parquet_bind.compression_level, parquet_bind.debug_use_openssl, parquet_bind.parquet_version);
	return std::move(global_state);
}

void ParquetWriteSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto &local_state = lstate.Cast<ParquetWriteLocalState>();

	// append data to the local (buffered) chunk collection
	local_state.buffer.Append(local_state.append_state, input);

	if (local_state.buffer.Count() >= bind_data.row_group_size ||
	    local_state.buffer.SizeInBytes() >= bind_data.row_group_size_bytes) {
		// if the chunk collection exceeds a certain size (rows/bytes) we flush it to the parquet file
		local_state.append_state.current_chunk_state.handles.clear();
		global_state.writer->Flush(local_state.buffer);
		local_state.buffer.InitializeAppend(local_state.append_state);
	}
}

void ParquetWriteCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                         LocalFunctionData &lstate) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto &local_state = lstate.Cast<ParquetWriteLocalState>();
	// flush any data left in the local state to the file
	global_state.writer->Flush(local_state.buffer);
}

void ParquetWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	// finalize: write any additional metadata to the file here
	global_state.writer->Finalize();
}

unique_ptr<LocalFunctionData> ParquetWriteInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	return make_uniq<ParquetWriteLocalState>(context.client, bind_data.sql_types);
}

// LCOV_EXCL_START

// FIXME: Have these be generated instead
template <>
const char *EnumUtil::ToChars<duckdb_parquet::CompressionCodec::type>(duckdb_parquet::CompressionCodec::type value) {
	switch (value) {
	case CompressionCodec::UNCOMPRESSED:
		return "UNCOMPRESSED";
		break;
	case CompressionCodec::SNAPPY:
		return "SNAPPY";
		break;
	case CompressionCodec::GZIP:
		return "GZIP";
		break;
	case CompressionCodec::LZO:
		return "LZO";
		break;
	case CompressionCodec::BROTLI:
		return "BROTLI";
		break;
	case CompressionCodec::LZ4:
		return "LZ4";
		break;
	case CompressionCodec::LZ4_RAW:
		return "LZ4_RAW";
		break;
	case CompressionCodec::ZSTD:
		return "ZSTD";
		break;
	default:
		throw NotImplementedException(StringUtil::Format("Enum value: '%s' not implemented", value));
	}
}

template <>
duckdb_parquet::CompressionCodec::type EnumUtil::FromString<duckdb_parquet::CompressionCodec::type>(const char *value) {
	if (StringUtil::Equals(value, "UNCOMPRESSED")) {
		return CompressionCodec::UNCOMPRESSED;
	}
	if (StringUtil::Equals(value, "SNAPPY")) {
		return CompressionCodec::SNAPPY;
	}
	if (StringUtil::Equals(value, "GZIP")) {
		return CompressionCodec::GZIP;
	}
	if (StringUtil::Equals(value, "LZO")) {
		return CompressionCodec::LZO;
	}
	if (StringUtil::Equals(value, "BROTLI")) {
		return CompressionCodec::BROTLI;
	}
	if (StringUtil::Equals(value, "LZ4")) {
		return CompressionCodec::LZ4;
	}
	if (StringUtil::Equals(value, "LZ4_RAW")) {
		return CompressionCodec::LZ4_RAW;
	}
	if (StringUtil::Equals(value, "ZSTD")) {
		return CompressionCodec::ZSTD;
	}
	throw NotImplementedException(StringUtil::Format("Enum value: '%s' not implemented", value));
}

template <>
const char *EnumUtil::ToChars<ParquetVersion>(ParquetVersion value) {
	switch (value) {
	case ParquetVersion::V1:
		return "V1";
	case ParquetVersion::V2:
		return "V2";
	default:
		throw NotImplementedException(StringUtil::Format("Enum value: '%s' not implemented", value));
	}
}

template <>
ParquetVersion EnumUtil::FromString<ParquetVersion>(const char *value) {
	if (StringUtil::Equals(value, "V1")) {
		return ParquetVersion::V1;
	}
	if (StringUtil::Equals(value, "V2")) {
		return ParquetVersion::V2;
	}
	throw NotImplementedException(StringUtil::Format("Enum value: '%s' not implemented", value));
}

static optional_idx SerializeCompressionLevel(const int64_t compression_level) {
	return compression_level < 0 ? NumericLimits<idx_t>::Maximum() - NumericCast<idx_t>(AbsValue(compression_level))
	                             : NumericCast<idx_t>(compression_level);
}

static int64_t DeserializeCompressionLevel(const optional_idx compression_level) {
	// Was originally an optional_idx, now int64_t, so we still serialize as such
	if (!compression_level.IsValid()) {
		return ZStdFileSystem::DefaultCompressionLevel();
	}
	if (compression_level.GetIndex() > NumericCast<idx_t>(ZStdFileSystem::MaximumCompressionLevel())) {
		// restore the negative compression level
		return -NumericCast<int64_t>(NumericLimits<idx_t>::Maximum() - compression_level.GetIndex());
	}
	return NumericCast<int64_t>(compression_level.GetIndex());
}

static void ParquetCopySerialize(Serializer &serializer, const FunctionData &bind_data_p,
                                 const CopyFunction &function) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	serializer.WriteProperty(100, "sql_types", bind_data.sql_types);
	serializer.WriteProperty(101, "column_names", bind_data.column_names);
	serializer.WriteProperty(102, "codec", bind_data.codec);
	serializer.WriteProperty(103, "row_group_size", bind_data.row_group_size);
	serializer.WriteProperty(104, "row_group_size_bytes", bind_data.row_group_size_bytes);
	serializer.WriteProperty(105, "kv_metadata", bind_data.kv_metadata);
	serializer.WriteProperty(106, "field_ids", bind_data.field_ids);
	serializer.WritePropertyWithDefault<shared_ptr<ParquetEncryptionConfig>>(107, "encryption_config",
	                                                                         bind_data.encryption_config, nullptr);

	// 108 was dictionary_compression_ratio_threshold, but was deleted

	// To avoid doubly defining the default values in both ParquetWriteBindData and here,
	// and possibly making a mistake, we just get the values from ParquetWriteBindData.
	// We have to std::move them, otherwise MSVC will complain that it's not a "const T &&"
	const auto compression_level = SerializeCompressionLevel(bind_data.compression_level);
	D_ASSERT(DeserializeCompressionLevel(compression_level) == bind_data.compression_level);
	ParquetWriteBindData default_value;
	serializer.WritePropertyWithDefault(109, "compression_level", compression_level);
	serializer.WritePropertyWithDefault(110, "row_groups_per_file", bind_data.row_groups_per_file,
	                                    default_value.row_groups_per_file);
	serializer.WritePropertyWithDefault(111, "debug_use_openssl", bind_data.debug_use_openssl,
	                                    default_value.debug_use_openssl);
	serializer.WritePropertyWithDefault(112, "dictionary_size_limit", bind_data.dictionary_size_limit,
	                                    default_value.dictionary_size_limit);
	serializer.WritePropertyWithDefault(113, "bloom_filter_false_positive_ratio",
	                                    bind_data.bloom_filter_false_positive_ratio,
	                                    default_value.bloom_filter_false_positive_ratio);
	serializer.WritePropertyWithDefault(114, "parquet_version", bind_data.parquet_version,
	                                    default_value.parquet_version);
}

static unique_ptr<FunctionData> ParquetCopyDeserialize(Deserializer &deserializer, CopyFunction &function) {
	auto data = make_uniq<ParquetWriteBindData>();
	data->sql_types = deserializer.ReadProperty<vector<LogicalType>>(100, "sql_types");
	data->column_names = deserializer.ReadProperty<vector<string>>(101, "column_names");
	data->codec = deserializer.ReadProperty<duckdb_parquet::CompressionCodec::type>(102, "codec");
	data->row_group_size = deserializer.ReadProperty<idx_t>(103, "row_group_size");
	data->row_group_size_bytes = deserializer.ReadProperty<idx_t>(104, "row_group_size_bytes");
	data->kv_metadata = deserializer.ReadProperty<vector<pair<string, string>>>(105, "kv_metadata");
	data->field_ids = deserializer.ReadProperty<ChildFieldIDs>(106, "field_ids");
	deserializer.ReadPropertyWithExplicitDefault<shared_ptr<ParquetEncryptionConfig>>(
	    107, "encryption_config", data->encryption_config, std::move(ParquetWriteBindData().encryption_config));
	deserializer.ReadDeletedProperty<double>(108, "dictionary_compression_ratio_threshold");

	optional_idx compression_level;
	deserializer.ReadPropertyWithDefault<optional_idx>(109, "compression_level", compression_level);
	data->compression_level = DeserializeCompressionLevel(compression_level);
	D_ASSERT(SerializeCompressionLevel(data->compression_level) == compression_level);
	ParquetWriteBindData default_value;
	data->row_groups_per_file = deserializer.ReadPropertyWithExplicitDefault<optional_idx>(
	    110, "row_groups_per_file", default_value.row_groups_per_file);
	data->debug_use_openssl =
	    deserializer.ReadPropertyWithExplicitDefault<bool>(111, "debug_use_openssl", default_value.debug_use_openssl);
	data->dictionary_size_limit = deserializer.ReadPropertyWithExplicitDefault<idx_t>(
	    112, "dictionary_size_limit", default_value.dictionary_size_limit);
	data->bloom_filter_false_positive_ratio = deserializer.ReadPropertyWithExplicitDefault<double>(
	    113, "bloom_filter_false_positive_ratio", default_value.bloom_filter_false_positive_ratio);
	data->parquet_version =
	    deserializer.ReadPropertyWithExplicitDefault(114, "parquet_version", default_value.parquet_version);

	return std::move(data);
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Execution Mode
//===--------------------------------------------------------------------===//
CopyFunctionExecutionMode ParquetWriteExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	if (!preserve_insertion_order) {
		return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}
//===--------------------------------------------------------------------===//
// Prepare Batch
//===--------------------------------------------------------------------===//
struct ParquetWriteBatchData : public PreparedBatchData {
	PreparedRowGroup prepared_row_group;
};

unique_ptr<PreparedBatchData> ParquetWritePrepareBatch(ClientContext &context, FunctionData &bind_data,
                                                       GlobalFunctionData &gstate,
                                                       unique_ptr<ColumnDataCollection> collection) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto result = make_uniq<ParquetWriteBatchData>();
	global_state.writer->PrepareRowGroup(*collection, result->prepared_row_group);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Flush Batch
//===--------------------------------------------------------------------===//
void ParquetWriteFlushBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                            PreparedBatchData &batch_p) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto &batch = batch_p.Cast<ParquetWriteBatchData>();
	global_state.writer->FlushRowGroup(batch.prepared_row_group);
}

//===--------------------------------------------------------------------===//
// Desired Batch Size
//===--------------------------------------------------------------------===//
idx_t ParquetWriteDesiredBatchSize(ClientContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	return bind_data.row_group_size;
}

//===--------------------------------------------------------------------===//
// File rotation
//===--------------------------------------------------------------------===//
bool ParquetWriteRotateFiles(FunctionData &bind_data_p, const optional_idx &file_size_bytes) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	return file_size_bytes.IsValid() || bind_data.row_groups_per_file.IsValid();
}

bool ParquetWriteRotateNextFile(GlobalFunctionData &gstate, FunctionData &bind_data_p,
                                const optional_idx &file_size_bytes) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	if (file_size_bytes.IsValid() && global_state.writer->FileSize() > file_size_bytes.GetIndex()) {
		return true;
	}
	if (bind_data.row_groups_per_file.IsValid() &&
	    global_state.writer->NumberOfRowGroups() >= bind_data.row_groups_per_file.GetIndex()) {
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Scan Replacement
//===--------------------------------------------------------------------===//
unique_ptr<TableRef> ParquetScanReplacement(ClientContext &context, ReplacementScanInput &input,
                                            optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!ReplacementScan::CanReplace(table_name, {"parquet"})) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("parquet_scan", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
// Helper predicates for ParquetWriteSelect
static bool IsTypeNotSupported(const LogicalType &type) {
	if (type.IsNested()) {
		return false;
	}
	return !ParquetWriter::TryGetParquetType(type);
}

static bool IsTypeLossy(const LogicalType &type) {
	return type.id() == LogicalTypeId::HUGEINT || type.id() == LogicalTypeId::UHUGEINT;
}

static vector<unique_ptr<Expression>> ParquetWriteSelect(CopyToSelectInput &input) {

	auto &context = input.context;

	vector<unique_ptr<Expression>> result;

	bool any_change = false;

	for (auto &expr : input.select_list) {

		const auto &type = expr->return_type;
		const auto &name = expr->GetAlias();

		// Spatial types need to be encoded into WKB when writing GeoParquet.
		// But dont perform this conversion if this is a EXPORT DATABASE statement
		if (input.copy_to_type == CopyToType::COPY_TO_FILE && type.id() == LogicalTypeId::BLOB && type.HasAlias() &&
		    type.GetAlias() == "GEOMETRY" && GeoParquetFileMetadata::IsGeoParquetConversionEnabled(context)) {

			LogicalType wkb_blob_type(LogicalTypeId::BLOB);
			wkb_blob_type.SetAlias("WKB_BLOB");

			auto cast_expr = BoundCastExpression::AddCastToType(context, std::move(expr), wkb_blob_type, false);
			cast_expr->SetAlias(name);
			result.push_back(std::move(cast_expr));
			any_change = true;
		}
		// If this is an EXPORT DATABASE statement, we dont want to write "lossy" types, instead cast them to VARCHAR
		else if (input.copy_to_type == CopyToType::EXPORT_DATABASE && TypeVisitor::Contains(type, IsTypeLossy)) {
			// Replace all lossy types with VARCHAR
			auto new_type = TypeVisitor::VisitReplace(
			    type, [](const LogicalType &ty) -> LogicalType { return IsTypeLossy(ty) ? LogicalType::VARCHAR : ty; });

			// Cast the column to the new type
			auto cast_expr = BoundCastExpression::AddCastToType(context, std::move(expr), new_type, false);
			cast_expr->SetAlias(name);
			result.push_back(std::move(cast_expr));
			any_change = true;
		}
		// Else look if there is any unsupported type
		else if (TypeVisitor::Contains(type, IsTypeNotSupported)) {
			// If there is at least one unsupported type, replace all unsupported types with varchar
			// and perform a CAST
			auto new_type = TypeVisitor::VisitReplace(type, [](const LogicalType &ty) -> LogicalType {
				return IsTypeNotSupported(ty) ? LogicalType::VARCHAR : ty;
			});

			auto cast_expr = BoundCastExpression::AddCastToType(context, std::move(expr), new_type, false);
			cast_expr->SetAlias(name);
			result.push_back(std::move(cast_expr));
			any_change = true;
		}
		// Otherwise, just reference the input column
		else {
			result.push_back(std::move(expr));
		}
	}

	// If any change was made, return the new expressions
	// otherwise, return an empty vector to indicate no change and avoid pushing another projection on to the plan
	if (any_change) {
		return result;
	}
	return {};
}

void ParquetExtension::Load(DuckDB &db) {
	auto &db_instance = *db.instance;
	auto &fs = db.GetFileSystem();
	fs.RegisterSubSystem(FileCompressionType::ZSTD, make_uniq<ZStdFileSystem>());

	auto scan_fun = ParquetScanFunction::GetFunctionSet();
	scan_fun.name = "read_parquet";
	ExtensionUtil::RegisterFunction(db_instance, scan_fun);
	scan_fun.name = "parquet_scan";
	ExtensionUtil::RegisterFunction(db_instance, scan_fun);

	// parquet_metadata
	ParquetMetaDataFunction meta_fun;
	ExtensionUtil::RegisterFunction(db_instance, MultiFileReader::CreateFunctionSet(meta_fun));

	// parquet_schema
	ParquetSchemaFunction schema_fun;
	ExtensionUtil::RegisterFunction(db_instance, MultiFileReader::CreateFunctionSet(schema_fun));

	// parquet_key_value_metadata
	ParquetKeyValueMetadataFunction kv_meta_fun;
	ExtensionUtil::RegisterFunction(db_instance, MultiFileReader::CreateFunctionSet(kv_meta_fun));

	// parquet_file_metadata
	ParquetFileMetadataFunction file_meta_fun;
	ExtensionUtil::RegisterFunction(db_instance, MultiFileReader::CreateFunctionSet(file_meta_fun));

	// parquet_bloom_probe
	ParquetBloomProbeFunction bloom_probe_fun;
	ExtensionUtil::RegisterFunction(db_instance, MultiFileReader::CreateFunctionSet(bloom_probe_fun));

	CopyFunction function("parquet");
	function.copy_to_select = ParquetWriteSelect;
	function.copy_to_bind = ParquetWriteBind;
	function.copy_to_initialize_global = ParquetWriteInitializeGlobal;
	function.copy_to_initialize_local = ParquetWriteInitializeLocal;
	function.copy_to_sink = ParquetWriteSink;
	function.copy_to_combine = ParquetWriteCombine;
	function.copy_to_finalize = ParquetWriteFinalize;
	function.execution_mode = ParquetWriteExecutionMode;
	function.copy_from_bind = ParquetScanFunction::ParquetReadBind;
	function.copy_from_function = scan_fun.functions[0];
	function.prepare_batch = ParquetWritePrepareBatch;
	function.flush_batch = ParquetWriteFlushBatch;
	function.desired_batch_size = ParquetWriteDesiredBatchSize;
	function.rotate_files = ParquetWriteRotateFiles;
	function.rotate_next_file = ParquetWriteRotateNextFile;
	function.serialize = ParquetCopySerialize;
	function.deserialize = ParquetCopyDeserialize;

	function.extension = "parquet";
	ExtensionUtil::RegisterFunction(db_instance, function);

	// parquet_key
	auto parquet_key_fun = PragmaFunction::PragmaCall("add_parquet_key", ParquetCrypto::AddKey,
	                                                  {LogicalType::VARCHAR, LogicalType::VARCHAR});
	ExtensionUtil::RegisterFunction(db_instance, parquet_key_fun);

	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(ParquetScanReplacement);
	config.AddExtensionOption("binary_as_string", "In Parquet files, interpret binary data as a string.",
	                          LogicalType::BOOLEAN);
	config.AddExtensionOption("disable_parquet_prefetching", "Disable the prefetching mechanism in Parquet",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption("prefetch_all_parquet_files",
	                          "Use the prefetching mechanism for all types of parquet files", LogicalType::BOOLEAN,
	                          Value(false));
	config.AddExtensionOption("parquet_metadata_cache",
	                          "Cache Parquet metadata - useful when reading the same files multiple times",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption(
	    "enable_geoparquet_conversion",
	    "Attempt to decode/encode geometry data in/as GeoParquet files if the spatial extension is present.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));
}

std::string ParquetExtension::Name() {
	return "parquet";
}

std::string ParquetExtension::Version() const {
#ifdef EXT_VERSION_PARQUET
	return EXT_VERSION_PARQUET;
#else
	return "";
#endif
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_EXTENSION_API void parquet_init(duckdb::DatabaseInstance &db) { // NOLINT
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ParquetExtension>();
}

DUCKDB_EXTENSION_API const char *parquet_version() { // NOLINT
	return duckdb::DuckDB::LibraryVersion();
}
}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
