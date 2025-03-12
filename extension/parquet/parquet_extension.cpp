#define DUCKDB_EXTENSION_MAIN

#include "parquet_extension.hpp"

#include "reader/cast_column_reader.hpp"
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
#include "reader/struct_column_reader.hpp"
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
#include "duckdb/common/multi_file_reader_function.hpp"
#include "duckdb/common/primitive_dictionary.hpp"
#endif

namespace duckdb {

class ParquetFileReaderOptions : public BaseFileReaderOptions {
public:
	explicit ParquetFileReaderOptions(ParquetOptions options_p) : options(std::move(options_p)) {
	}
	explicit ParquetFileReaderOptions(ClientContext &context) : options(context) {
	}

	ParquetOptions options;
};

struct ParquetReadBindData : public TableFunctionData {
	// These come from the initial_reader, but need to be stored in case the initial_reader is removed by a filter
	idx_t initial_file_cardinality;
	idx_t initial_file_row_groups;
	idx_t explicit_cardinality = 0; // can be set to inject exterior cardinality knowledge (e.g. from a data lake)
	ParquetOptions parquet_options;
};

struct ParquetReadGlobalState : public GlobalTableFunctionState {
	//! Index of row group within file currently up for scanning
	idx_t row_group_index;
	//! Batch index of the next row group to be scanned
	idx_t batch_index;
};

struct ParquetReadLocalState : public LocalTableFunctionState {
	ParquetReaderScanState scan_state;
};

struct ParquetMultiFileInfo {
	static unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                           optional_ptr<TableFunctionInfo> info);
	static bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                            BaseFileReaderOptions &options, vector<string> &expected_names,
	                            vector<LogicalType> &expected_types);
	static bool ParseOption(ClientContext &context, const string &key, const Value &val,
	                        MultiFileReaderOptions &file_options, BaseFileReaderOptions &options);
	static void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options_p,
	                             const vector<string> &expected_names, const vector<LogicalType> &expected_types);
	static void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                       MultiFileBindData &bind_data);
	static unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                        unique_ptr<BaseFileReaderOptions> options);
	static void FinalizeBindData(MultiFileBindData &multi_file_data);
	static void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info);
	static optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
	                               FileExpandResult expand_result);
	static unique_ptr<GlobalTableFunctionState>
	InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data, MultiFileGlobalState &global_state);
	static unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &, GlobalTableFunctionState &);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               BaseUnionData &union_data, const MultiFileBindData &bind_data_p);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               const string &filename, idx_t file_idx,
	                                               const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const string &filename,
	                                               ParquetOptions &options, const MultiFileReaderOptions &file_options);
	static shared_ptr<BaseUnionData> GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx);
	static void FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &);
	static void Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk);
	static bool TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader,
	                              GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate);
	static void FinishFile(ClientContext &context, GlobalTableFunctionState &global_state, BaseFileReader &reader);
	static void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                          LocalTableFunctionState &local_state);
	static unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count);
	static unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, BaseFileReader &reader, const string &name);
	static double GetProgressInFile(ClientContext &context, const BaseFileReader &reader);
	static void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result);
};

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

static void BindSchema(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                       MultiFileBindData &bind_data) {
	auto &parquet_bind = bind_data.bind_data->Cast<ParquetReadBindData>();
	auto &options = parquet_bind.parquet_options;
	D_ASSERT(!options.schema.empty());

	auto &file_options = bind_data.file_options;
	if (file_options.union_by_name || file_options.hive_partitioning) {
		throw BinderException("Parquet schema cannot be combined with union_by_name=true or hive_partitioning=true");
	}
	auto &reader_bind = bind_data.reader_bind;

	vector<string> schema_col_names;
	vector<LogicalType> schema_col_types;
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
		reader_bind.schema.emplace_back(std::move(res));
	}

	if (match_by_field_id) {
		reader_bind.mapping = MultiFileReaderColumnMappingMode::BY_FIELD_ID;
	} else {
		reader_bind.mapping = MultiFileReaderColumnMappingMode::BY_NAME;
	}

	// perform the binding on the obtained set of names + types
	bind_data.multi_file_reader->BindOptions(file_options, *bind_data.file_list, schema_col_types, schema_col_names,
	                                         reader_bind);

	names = schema_col_names;
	return_types = schema_col_types;
	D_ASSERT(names.size() == return_types.size());

	ParseFileRowNumberOption(reader_bind, options, return_types, names);
}

void ParquetMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                      MultiFileBindData &bind_data) {
	auto &parquet_bind = bind_data.bind_data->Cast<ParquetReadBindData>();
	auto &options = parquet_bind.parquet_options;
	if (!options.schema.empty()) {
		BindSchema(context, return_types, names, bind_data);
	} else {
		bind_data.reader_bind = bind_data.multi_file_reader->BindReader<ParquetMultiFileInfo>(
		    context, return_types, names, *bind_data.file_list, bind_data, options, bind_data.file_options);
	}
}

static bool GetBooleanArgument(const string &key, const vector<Value> &option_values) {
	if (option_values.empty()) {
		return true;
	}
	Value boolean_value;
	string error_message;
	if (!option_values[0].DefaultTryCastAs(LogicalType::BOOLEAN, boolean_value, &error_message)) {
		throw InvalidInputException("Unable to cast \"%s\" to BOOLEAN for Parquet option \"%s\"",
		                            option_values[0].ToString(), key);
	}
	return BooleanValue::Get(boolean_value);
}

class ParquetScanFunction {
public:
	static TableFunctionSet GetFunctionSet() {
		MultiFileReaderFunction<ParquetMultiFileInfo> table_function("parquet_scan");
		table_function.named_parameters["binary_as_string"] = LogicalType::BOOLEAN;
		table_function.named_parameters["file_row_number"] = LogicalType::BOOLEAN;
		table_function.named_parameters["debug_use_openssl"] = LogicalType::BOOLEAN;
		table_function.named_parameters["compression"] = LogicalType::VARCHAR;
		table_function.named_parameters["explicit_cardinality"] = LogicalType::UBIGINT;
		table_function.named_parameters["schema"] = LogicalTypeId::ANY;
		table_function.named_parameters["encryption_config"] = LogicalTypeId::ANY;
		table_function.named_parameters["parquet_version"] = LogicalType::VARCHAR;
		table_function.statistics = MultiFileReaderFunction<ParquetMultiFileInfo>::MultiFileScanStats;
		table_function.serialize = ParquetScanSerialize;
		table_function.deserialize = ParquetScanDeserialize;
		table_function.filter_pushdown = true;
		table_function.filter_prune = true;

		return MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(table_function));
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

	static void ParquetScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                                 const TableFunction &function) {
		auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
		auto &parquet_data = bind_data.bind_data->Cast<ParquetReadBindData>();

		serializer.WriteProperty(100, "files", bind_data.file_list->GetAllFiles());
		serializer.WriteProperty(101, "types", bind_data.types);
		serializer.WriteProperty(102, "names", bind_data.names);
		ParquetOptionsSerialization serialization(parquet_data.parquet_options, bind_data.file_options);
		serializer.WriteProperty(103, "parquet_options", serialization);
		if (serializer.ShouldSerialize(3)) {
			serializer.WriteProperty(104, "table_columns", bind_data.table_columns);
		}
	}

	static unique_ptr<FunctionData> ParquetScanDeserialize(Deserializer &deserializer, TableFunction &function) {
		auto &context = deserializer.Get<ClientContext &>();
		auto files = deserializer.ReadProperty<vector<string>>(100, "files");
		auto types = deserializer.ReadProperty<vector<LogicalType>>(101, "types");
		auto names = deserializer.ReadProperty<vector<string>>(102, "names");
		auto serialization = deserializer.ReadProperty<ParquetOptionsSerialization>(103, "parquet_options");
		auto table_columns =
		    deserializer.ReadPropertyWithExplicitDefault<vector<string>>(104, "table_columns", vector<string> {});

		vector<Value> file_path;
		for (auto &path : files) {
			file_path.emplace_back(path);
		}

		auto multi_file_reader = MultiFileReader::Create(function);
		auto file_list = multi_file_reader->CreateFileList(context, Value::LIST(LogicalType::VARCHAR, file_path),
		                                                   FileGlobOptions::DISALLOW_EMPTY);
		auto parquet_options = make_uniq<ParquetFileReaderOptions>(std::move(serialization.parquet_options));
		auto bind_data = MultiFileReaderFunction<ParquetMultiFileInfo>::MultiFileBindInternal(
		    context, std::move(multi_file_reader), std::move(file_list), types, names,
		    std::move(serialization.file_options), std::move(parquet_options));
		bind_data->Cast<MultiFileBindData>().table_columns = std::move(table_columns);
		return bind_data;
	}
};

unique_ptr<BaseFileReaderOptions> ParquetMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                          optional_ptr<TableFunctionInfo> info) {
	return make_uniq<ParquetFileReaderOptions>(context);
}

bool ParquetMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                                           BaseFileReaderOptions &file_options, vector<string> &expected_names,
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
	if (key == "file_row_number") {
		options.file_row_number = GetBooleanArgument(key, values);
		return true;
	}
	if (key == "debug_use_openssl") {
		options.debug_use_openssl = GetBooleanArgument(key, values);
		return true;
	}
	if (key == "encryption_config") {
		if (values.size() != 1) {
			throw BinderException("Parquet encryption_config cannot be empty!");
		}
		options.encryption_config = ParquetEncryptionConfig::Create(context, values[0]);
		return true;
	}
	return false;
}

void ParquetMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options_p,
                                            const vector<string> &expected_names,
                                            const vector<LogicalType> &expected_types) {
}

bool ParquetMultiFileInfo::ParseOption(ClientContext &context, const string &original_key, const Value &val,
                                       MultiFileReaderOptions &file_options, BaseFileReaderOptions &base_options) {
	auto &parquet_options = base_options.Cast<ParquetFileReaderOptions>();
	auto &options = parquet_options.options;
	auto key = StringUtil::Lower(original_key);
	if (val.IsNull()) {
		throw BinderException("Cannot use NULL as argument to %s", original_key);
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
	if (key == "file_row_number") {
		options.file_row_number = BooleanValue::Get(val);
		return true;
	}
	if (key == "debug_use_openssl") {
		options.debug_use_openssl = BooleanValue::Get(val);
		return true;
	}
	if (key == "schema") {
		// Argument is a map that defines the schema
		const auto &schema_value = val;
		ParquetScanFunction::VerifyParquetSchemaParameter(schema_value);
		const auto column_values = ListValue::GetChildren(schema_value);
		if (column_values.empty()) {
			throw BinderException("Parquet schema cannot be empty");
		}
		options.schema.reserve(column_values.size());
		for (idx_t i = 0; i < column_values.size(); i++) {
			options.schema.emplace_back(ParquetColumnDefinition::FromSchemaValue(context, column_values[i]));
		}
		file_options.auto_detect_hive_partitioning = false;
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
	return false;
}

unique_ptr<TableFunctionData> ParquetMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                       unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<ParquetReadBindData>();
	// Set the explicit cardinality if requested
	auto &options = options_p->Cast<ParquetFileReaderOptions>();
	result->parquet_options = std::move(options.options);
	if (result->parquet_options.explicit_cardinality) {
		auto file_count = multi_file_data.file_list->GetTotalFileCount();
		result->explicit_cardinality = result->parquet_options.explicit_cardinality;
		result->initial_file_cardinality = result->explicit_cardinality / (file_count ? file_count : 1);
	}
	return std::move(result);
}

void ParquetMultiFileInfo::GetBindInfo(const TableFunctionData &bind_data_p, BindInfo &info) {
	auto &bind_data = bind_data_p.Cast<ParquetReadBindData>();
	info.type = ScanType::PARQUET;
	info.InsertOption("binary_as_string", Value::BOOLEAN(bind_data.parquet_options.binary_as_string));
	info.InsertOption("file_row_number", Value::BOOLEAN(bind_data.parquet_options.file_row_number));
	info.InsertOption("debug_use_openssl", Value::BOOLEAN(bind_data.parquet_options.debug_use_openssl));
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
	// Enable the parquet file_row_number on the parquet options if the file_row_number_idx was set
	if (multi_file_data.reader_bind.file_row_number_idx != DConstants::INVALID_INDEX) {
		bind_data.parquet_options.file_row_number = true;
	}
	if (multi_file_data.initial_reader) {
		auto &initial_reader = multi_file_data.initial_reader->Cast<ParquetReader>();
		bind_data.initial_file_cardinality = initial_reader.NumRows();
		bind_data.initial_file_row_groups = initial_reader.NumRowGroups();
		bind_data.parquet_options = initial_reader.parquet_options;
	}
}

unique_ptr<NodeStatistics> ParquetMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data_p,
                                                                idx_t file_count) {
	auto &bind_data = bind_data_p.bind_data->Cast<ParquetReadBindData>();
	if (bind_data.explicit_cardinality) {
		return make_uniq<NodeStatistics>(bind_data.explicit_cardinality);
	}
	return make_uniq<NodeStatistics>(MaxValue(bind_data.initial_file_cardinality, (idx_t)1) * file_count);
}

unique_ptr<BaseStatistics> ParquetMultiFileInfo::GetStatistics(ClientContext &context, BaseFileReader &reader_p,
                                                               const string &name) {
	auto &reader = reader_p.Cast<ParquetReader>();
	return reader.ReadStatistics(name);
}

double ParquetMultiFileInfo::GetProgressInFile(ClientContext &context, const BaseFileReader &reader) {
	auto &parquet_reader = reader.Cast<ParquetReader>();
	auto read_rows = parquet_reader.rows_read.load();
	return 100.0 * (static_cast<double>(read_rows) / static_cast<double>(parquet_reader.NumRows()));
}

void ParquetMultiFileInfo::GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &) {
}

shared_ptr<BaseFileReader> ParquetMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &,
                                                              BaseUnionData &union_data_p,
                                                              const MultiFileBindData &bind_data_p) {
	auto &union_data = union_data_p.Cast<ParquetUnionData>();
	return make_shared_ptr<ParquetReader>(context, union_data.file_name, union_data.options, union_data.metadata);
}

shared_ptr<BaseFileReader> ParquetMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &,
                                                              const string &filename, idx_t file_idx,
                                                              const MultiFileBindData &multi_bind_data) {
	auto &bind_data = multi_bind_data.bind_data->Cast<ParquetReadBindData>();
	return make_shared_ptr<ParquetReader>(context, filename, bind_data.parquet_options);
}

shared_ptr<BaseFileReader> ParquetMultiFileInfo::CreateReader(ClientContext &context, const string &filename,
                                                              ParquetOptions &options, const MultiFileReaderOptions &) {
	return make_shared_ptr<ParquetReader>(context, filename, options);
}

shared_ptr<BaseUnionData> ParquetMultiFileInfo::GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx) {
	auto &scan = scan_p->Cast<ParquetReader>();
	auto result = make_uniq<ParquetUnionData>(scan.file_name);
	if (file_idx == 0) {
		for (auto &column : scan.columns) {
			result->names.push_back(column.name);
			result->types.push_back(column.type);
		}
		result->options = scan.parquet_options;
		result->metadata = scan.metadata;
		result->reader = std::move(scan_p);
	} else {
		for (auto &column : scan.columns) {
			result->names.push_back(column.name);
			result->types.push_back(column.type);
		}
		scan.columns.clear();
		result->options = std::move(scan.parquet_options);
		result->metadata = std::move(scan.metadata);
	}

	return result;
}

void ParquetMultiFileInfo::FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &) {
}

unique_ptr<GlobalTableFunctionState> ParquetMultiFileInfo::InitializeGlobalState(ClientContext &, MultiFileBindData &,
                                                                                 MultiFileGlobalState &) {
	return make_uniq<ParquetReadGlobalState>();
}

unique_ptr<LocalTableFunctionState> ParquetMultiFileInfo::InitializeLocalState(ExecutionContext &,
                                                                               GlobalTableFunctionState &) {
	return make_uniq<ParquetReadLocalState>();
}

bool ParquetMultiFileInfo::TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader_p,
                                             GlobalTableFunctionState &gstate_p, LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<ParquetReadGlobalState>();
	auto &lstate = lstate_p.Cast<ParquetReadLocalState>();
	auto &reader = reader_p->Cast<ParquetReader>();
	if (gstate.row_group_index >= reader.NumRowGroups()) {
		// scanned all row groups in this file
		return false;
	}
	// The current reader has rowgroups left to be scanned
	vector<idx_t> group_indexes {gstate.row_group_index};
	reader.InitializeScan(context, lstate.scan_state, group_indexes);
	gstate.row_group_index++;
	return true;
}

void ParquetMultiFileInfo::FinishFile(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                      BaseFileReader &reader) {
	auto &gstate = gstate_p.Cast<ParquetReadGlobalState>();
	gstate.row_group_index = 0;
}

void ParquetMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                         LocalTableFunctionState &local_state) {
}

void ParquetMultiFileInfo::Scan(ClientContext &context, BaseFileReader &reader_p, GlobalTableFunctionState &gstate_p,
                                LocalTableFunctionState &local_state_p, DataChunk &chunk) {
	auto &local_state = local_state_p.Cast<ParquetReadLocalState>();
	auto &reader = reader_p.Cast<ParquetReader>();
	reader.Scan(context, local_state.scan_state, chunk);
}

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
	idx_t dictionary_size_limit = row_group_size / 20;

	void SetToDefaultDictionarySizeLimit() {
		// This depends on row group size so we should "reset" if the row group size is changed
		dictionary_size_limit = row_group_size / 20;
	}

	idx_t string_dictionary_page_size_limit = 1048576;

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
	explicit ParquetWriteLocalState(ClientContext &context, const vector<LogicalType> &types) : buffer(context, types) {
		buffer.SetPartitionIndex(0); // Makes the buffer manager less likely to spill this data
		buffer.InitializeAppend(append_state);
	}

	ColumnDataCollection buffer;
	ColumnDataAppendState append_state;
};

unique_ptr<FunctionData> ParquetWriteBind(ClientContext &context, CopyFunctionBindInput &input,
                                          const vector<string> &names, const vector<LogicalType> &sql_types) {
	D_ASSERT(names.size() == sql_types.size());
	bool row_group_size_bytes_set = false;
	bool compression_level_set = false;
	bool dictionary_size_limit_set = false;
	auto bind_data = make_uniq<ParquetWriteBindData>();
	for (auto &option : input.info.options) {
		const auto loption = StringUtil::Lower(option.first);
		if (option.second.size() != 1) {
			// All parquet write options require exactly one argument
			throw BinderException("%s requires exactly one argument", StringUtil::Upper(loption));
		}
		if (loption == "row_group_size" || loption == "chunk_size") {
			bind_data->row_group_size = option.second[0].GetValue<uint64_t>();
			if (!dictionary_size_limit_set) {
				bind_data->SetToDefaultDictionarySizeLimit();
			}
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
				auto &value = values[i];
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
			dictionary_size_limit_set = true;
		} else if (loption == "string_dictionary_page_size_limit") {
			auto val = option.second[0].GetValue<uint64_t>();
			if (val > PrimitiveDictionary<uint32_t>::MAXIMUM_POSSIBLE_SIZE) {
				throw BinderException("string_dictionary_page_size_limit must be less than or equal to %llu",
				                      PrimitiveDictionary<uint32_t>::MAXIMUM_POSSIBLE_SIZE);
			}
			bind_data->string_dictionary_page_size_limit = val;
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
	    parquet_bind.dictionary_size_limit, parquet_bind.string_dictionary_page_size_limit,
	    parquet_bind.bloom_filter_false_positive_ratio, parquet_bind.compression_level, parquet_bind.debug_use_openssl,
	    parquet_bind.parquet_version);
	return std::move(global_state);
}

void ParquetWriteGetWrittenStatistics(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                      CopyFunctionFileStatistics &statistics) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	global_state.writer->SetWrittenStatistics(statistics);
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
	serializer.WritePropertyWithDefault(115, "string_dictionary_page_size_limit",
	                                    bind_data.string_dictionary_page_size_limit,
	                                    default_value.string_dictionary_page_size_limit);
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
	data->string_dictionary_page_size_limit = deserializer.ReadPropertyWithExplicitDefault(
	    115, "string_dictionary_page_size_limit", default_value.string_dictionary_page_size_limit);

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
	function.copy_to_get_written_statistics = ParquetWriteGetWrittenStatistics;
	function.copy_to_sink = ParquetWriteSink;
	function.copy_to_combine = ParquetWriteCombine;
	function.copy_to_finalize = ParquetWriteFinalize;
	function.execution_mode = ParquetWriteExecutionMode;
	function.copy_from_bind = MultiFileReaderFunction<ParquetMultiFileInfo>::MultiFileBindCopy;
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
