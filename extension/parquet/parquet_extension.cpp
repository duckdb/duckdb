#define DUCKDB_EXTENSION_MAIN

#include "parquet_extension.hpp"

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
#include "writer/primitive_column_writer.hpp"

#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
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
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/primitive_dictionary.hpp"
#include "parquet_multi_file_info.hpp"

namespace duckdb {

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

	//! This is huge but we grow it starting from 1 MB
	idx_t string_dictionary_page_size_limit = PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE;

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

	mutex lock;
	unique_ptr<ColumnDataCollection> combine_buffer;
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
				throw BinderException(
				    "Expected %s argument to be any of [uncompressed, brotli, gzip, snappy, lz4, lz4_raw or zstd]",
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
			if (val > PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE || val == 0) {
				throw BinderException(
				    "string_dictionary_page_size_limit cannot be 0 and must be less than or equal to %llu",
				    PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE);
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

void ParquetWriteCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                         LocalFunctionData &lstate) {
	auto &bind_data = bind_data_p.Cast<ParquetWriteBindData>();
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	auto &local_state = lstate.Cast<ParquetWriteLocalState>();

	if (local_state.buffer.Count() >= bind_data.row_group_size / 2 ||
	    local_state.buffer.SizeInBytes() >= bind_data.row_group_size_bytes / 2) {
		// local state buffer is more than half of the row_group_size(_bytes), just flush it
		global_state.writer->Flush(local_state.buffer);
		return;
	}

	unique_lock<mutex> guard(global_state.lock);
	if (global_state.combine_buffer) {
		// There is still some data, combine it
		global_state.combine_buffer->Combine(local_state.buffer);
		if (global_state.combine_buffer->Count() >= bind_data.row_group_size / 2 ||
		    global_state.combine_buffer->SizeInBytes() >= bind_data.row_group_size_bytes / 2) {
			// After combining, the combine buffer is more than half of the row_group_size(_bytes), so we flush
			auto owned_combine_buffer = std::move(global_state.combine_buffer);
			guard.unlock();
			// Lock free, of course
			global_state.writer->Flush(*owned_combine_buffer);
		}
		return;
	}

	global_state.combine_buffer = make_uniq<ColumnDataCollection>(context.client, local_state.buffer.Types());
	global_state.combine_buffer->Combine(local_state.buffer);
}

void ParquetWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &global_state = gstate.Cast<ParquetWriteGlobalState>();
	// flush the combine buffer (if it's there)
	if (global_state.combine_buffer) {
		global_state.writer->Flush(*global_state.combine_buffer);
	}

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
	function.copy_from_bind = MultiFileFunction<ParquetMultiFileInfo>::MultiFileBindCopy;
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
