#include "parquet_reader.hpp"

#include "reader/boolean_column_reader.hpp"
#include "reader/callback_column_reader.hpp"
#include "reader/cast_column_reader.hpp"
#include "column_reader.hpp"
#include "duckdb.hpp"
#include "reader/expression_column_reader.hpp"
#include "geo_parquet.hpp"
#include "reader/list_column_reader.hpp"
#include "parquet_crypto.hpp"
#include "parquet_file_metadata_cache.hpp"
#include "parquet_statistics.hpp"
#include "parquet_timestamp.hpp"
#include "mbedtls_wrapper.hpp"
#include "reader/row_number_column_reader.hpp"
#include "reader/string_column_reader.hpp"
#include "reader/struct_column_reader.hpp"
#include "reader/templated_column_reader.hpp"
#include "thrift_tools.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/table_filter_state.hpp"

#include <cassert>
#include <chrono>
#include <cstring>
#include <sstream>

namespace duckdb {

using duckdb_parquet::ColumnChunk;
using duckdb_parquet::ConvertedType;
using duckdb_parquet::FieldRepetitionType;
using duckdb_parquet::FileCryptoMetaData;
using duckdb_parquet::FileMetaData;
using ParquetRowGroup = duckdb_parquet::RowGroup;
using duckdb_parquet::SchemaElement;
using duckdb_parquet::Statistics;
using duckdb_parquet::Type;

static unique_ptr<duckdb_apache::thrift::protocol::TProtocol>
CreateThriftFileProtocol(Allocator &allocator, FileHandle &file_handle, bool prefetch_mode) {
	auto transport = std::make_shared<ThriftFileTransport>(allocator, file_handle, prefetch_mode);
	return make_uniq<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(std::move(transport));
}

static shared_ptr<ParquetFileMetadataCache>
LoadMetadata(ClientContext &context, Allocator &allocator, FileHandle &file_handle,
             const shared_ptr<const ParquetEncryptionConfig> &encryption_config,
             const EncryptionUtil &encryption_util) {
	auto current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

	auto file_proto = CreateThriftFileProtocol(allocator, file_handle, false);
	auto &transport = reinterpret_cast<ThriftFileTransport &>(*file_proto->getTransport());
	auto file_size = transport.GetSize();
	if (file_size < 12) {
		throw InvalidInputException("File '%s' too small to be a Parquet file", file_handle.path);
	}

	ResizeableBuffer buf;
	buf.resize(allocator, 8);
	buf.zero();

	transport.SetLocation(file_size - 8);
	transport.read(buf.ptr, 8);

	bool footer_encrypted;
	if (memcmp(buf.ptr + 4, "PAR1", 4) == 0) {
		footer_encrypted = false;
		if (encryption_config) {
			throw InvalidInputException("File '%s' is not encrypted, but 'encryption_config' was set",
			                            file_handle.path);
		}
	} else if (memcmp(buf.ptr + 4, "PARE", 4) == 0) {
		footer_encrypted = true;
		if (!encryption_config) {
			throw InvalidInputException("File '%s' is encrypted, but 'encryption_config' was not set",
			                            file_handle.path);
		}
	} else {
		throw InvalidInputException("No magic bytes found at end of file '%s'", file_handle.path);
	}

	// read four-byte footer length from just before the end magic bytes
	auto footer_len = *reinterpret_cast<uint32_t *>(buf.ptr);
	if (footer_len == 0 || file_size < 12 + footer_len) {
		throw InvalidInputException("Footer length error in file '%s'", file_handle.path);
	}

	auto metadata_pos = file_size - (footer_len + 8);
	transport.SetLocation(metadata_pos);
	transport.Prefetch(metadata_pos, footer_len);

	auto metadata = make_uniq<FileMetaData>();
	if (footer_encrypted) {
		auto crypto_metadata = make_uniq<FileCryptoMetaData>();
		crypto_metadata->read(file_proto.get());
		if (crypto_metadata->encryption_algorithm.__isset.AES_GCM_CTR_V1) {
			throw InvalidInputException("File '%s' is encrypted with AES_GCM_CTR_V1, but only AES_GCM_V1 is supported",
			                            file_handle.path);
		}
		ParquetCrypto::Read(*metadata, *file_proto, encryption_config->GetFooterKey(), encryption_util);
	} else {
		metadata->read(file_proto.get());
	}

	// Try to read the GeoParquet metadata (if present)
	auto geo_metadata = GeoParquetFileMetadata::TryRead(*metadata, context);

	return make_shared_ptr<ParquetFileMetadataCache>(std::move(metadata), current_time, std::move(geo_metadata));
}

LogicalType ParquetReader::DeriveLogicalType(const SchemaElement &s_ele, ParquetColumnSchema &schema) const {
	// inner node
	if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && !s_ele.__isset.type_length) {
		throw IOException("FIXED_LEN_BYTE_ARRAY requires length to be set");
	}
	if (s_ele.__isset.type_length) {
		schema.type_length = NumericCast<uint32_t>(s_ele.type_length);
	}
	schema.parquet_type = s_ele.type;
	if (s_ele.__isset.logicalType) {
		if (s_ele.logicalType.__isset.UNKNOWN) {
			return LogicalType::SQLNULL;
		} else if (s_ele.logicalType.__isset.UUID) {
			if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY) {
				return LogicalType::UUID;
			}
		} else if (s_ele.logicalType.__isset.FLOAT16) {
			if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && s_ele.type_length == 2) {
				schema.type_info = ParquetExtraTypeInfo::FLOAT16;
				return LogicalType::FLOAT;
			}
		} else if (s_ele.logicalType.__isset.TIMESTAMP) {
			if (s_ele.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
				schema.type_info = ParquetExtraTypeInfo::UNIT_MS;
			} else if (s_ele.logicalType.TIMESTAMP.unit.__isset.MICROS) {
				schema.type_info = ParquetExtraTypeInfo::UNIT_MICROS;
			} else if (s_ele.logicalType.TIMESTAMP.unit.__isset.NANOS) {
				schema.type_info = ParquetExtraTypeInfo::UNIT_NS;
			} else {
				throw NotImplementedException("Unimplemented TIMESTAMP encoding - missing UNIT");
			}
			if (s_ele.logicalType.TIMESTAMP.isAdjustedToUTC) {
				return LogicalType::TIMESTAMP_TZ;
			} else if (s_ele.logicalType.TIMESTAMP.unit.__isset.NANOS) {
				return LogicalType::TIMESTAMP_NS;
			}
			return LogicalType::TIMESTAMP;
		} else if (s_ele.logicalType.__isset.TIME) {
			if (s_ele.logicalType.TIME.unit.__isset.MILLIS) {
				schema.type_info = ParquetExtraTypeInfo::UNIT_MS;
			} else if (s_ele.logicalType.TIME.unit.__isset.MICROS) {
				schema.type_info = ParquetExtraTypeInfo::UNIT_MICROS;
			} else if (s_ele.logicalType.TIME.unit.__isset.NANOS) {
				schema.type_info = ParquetExtraTypeInfo::UNIT_NS;
			} else {
				throw NotImplementedException("Unimplemented TIME encoding - missing UNIT");
			}
			if (s_ele.logicalType.TIME.isAdjustedToUTC) {
				return LogicalType::TIME_TZ;
			}
			return LogicalType::TIME;
		}
	}
	if (s_ele.__isset.converted_type) {
		// Legacy NULL type, does no longer exist, but files are still around of course
		if (static_cast<uint8_t>(s_ele.converted_type) == 24) {
			return LogicalTypeId::SQLNULL;
		}
		switch (s_ele.converted_type) {
		case ConvertedType::INT_8:
			if (s_ele.type == Type::INT32) {
				return LogicalType::TINYINT;
			} else {
				throw IOException("INT8 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::INT_16:
			if (s_ele.type == Type::INT32) {
				return LogicalType::SMALLINT;
			} else {
				throw IOException("INT16 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::INT_32:
			if (s_ele.type == Type::INT32) {
				return LogicalType::INTEGER;
			} else {
				throw IOException("INT32 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::INT_64:
			if (s_ele.type == Type::INT64) {
				return LogicalType::BIGINT;
			} else {
				throw IOException("INT64 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_8:
			if (s_ele.type == Type::INT32) {
				return LogicalType::UTINYINT;
			} else {
				throw IOException("UINT8 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_16:
			if (s_ele.type == Type::INT32) {
				return LogicalType::USMALLINT;
			} else {
				throw IOException("UINT16 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_32:
			if (s_ele.type == Type::INT32) {
				return LogicalType::UINTEGER;
			} else {
				throw IOException("UINT32 converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::UINT_64:
			if (s_ele.type == Type::INT64) {
				return LogicalType::UBIGINT;
			} else {
				throw IOException("UINT64 converted type can only be set for value of Type::INT64");
			}
		case ConvertedType::DATE:
			if (s_ele.type == Type::INT32) {
				return LogicalType::DATE;
			} else {
				throw IOException("DATE converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::TIMESTAMP_MICROS:
			schema.type_info = ParquetExtraTypeInfo::UNIT_MICROS;
			if (s_ele.type == Type::INT64) {
				return LogicalType::TIMESTAMP;
			} else {
				throw IOException("TIMESTAMP converted type can only be set for value of Type::INT64");
			}
		case ConvertedType::TIMESTAMP_MILLIS:
			schema.type_info = ParquetExtraTypeInfo::UNIT_MS;
			if (s_ele.type == Type::INT64) {
				return LogicalType::TIMESTAMP;
			} else {
				throw IOException("TIMESTAMP converted type can only be set for value of Type::INT64");
			}
		case ConvertedType::DECIMAL:
			if (!s_ele.__isset.precision || !s_ele.__isset.scale) {
				throw IOException("DECIMAL requires a length and scale specifier!");
			}
			schema.type_scale = NumericCast<uint32_t>(s_ele.scale);
			if (s_ele.precision > DecimalType::MaxWidth()) {
				schema.type_info = ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY;
				return LogicalType::DOUBLE;
			}
			switch (s_ele.type) {
			case Type::BYTE_ARRAY:
			case Type::FIXED_LEN_BYTE_ARRAY:
				schema.type_info = ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY;
				break;
			case Type::INT32:
				schema.type_info = ParquetExtraTypeInfo::DECIMAL_INT32;
				break;
			case Type::INT64:
				schema.type_info = ParquetExtraTypeInfo::DECIMAL_INT64;
				break;
			default:
				throw IOException(
				    "DECIMAL converted type can only be set for value of Type::(FIXED_LEN_)BYTE_ARRAY/INT32/INT64");
			}
			return LogicalType::DECIMAL(s_ele.precision, s_ele.scale);
		case ConvertedType::UTF8:
		case ConvertedType::ENUM:
			switch (s_ele.type) {
			case Type::BYTE_ARRAY:
			case Type::FIXED_LEN_BYTE_ARRAY:
				return LogicalType::VARCHAR;
			default:
				throw IOException("UTF8 converted type can only be set for Type::(FIXED_LEN_)BYTE_ARRAY");
			}
		case ConvertedType::TIME_MILLIS:
			schema.type_info = ParquetExtraTypeInfo::UNIT_MS;
			if (s_ele.type == Type::INT32) {
				return LogicalType::TIME;
			} else {
				throw IOException("TIME_MILLIS converted type can only be set for value of Type::INT32");
			}
		case ConvertedType::TIME_MICROS:
			schema.type_info = ParquetExtraTypeInfo::UNIT_MICROS;
			if (s_ele.type == Type::INT64) {
				return LogicalType::TIME;
			} else {
				throw IOException("TIME_MICROS converted type can only be set for value of Type::INT64");
			}
		case ConvertedType::INTERVAL:
			return LogicalType::INTERVAL;
		case ConvertedType::JSON:
			return LogicalType::JSON();
		case ConvertedType::MAP:
		case ConvertedType::MAP_KEY_VALUE:
		case ConvertedType::LIST:
		case ConvertedType::BSON:
		default:
			throw IOException("Unsupported converted type (%d)", (int32_t)s_ele.converted_type);
		}
	} else {
		// no converted type set
		// use default type for each physical type
		switch (s_ele.type) {
		case Type::BOOLEAN:
			return LogicalType::BOOLEAN;
		case Type::INT32:
			return LogicalType::INTEGER;
		case Type::INT64:
			return LogicalType::BIGINT;
		case Type::INT96: // always a timestamp it would seem
			schema.type_info = ParquetExtraTypeInfo::IMPALA_TIMESTAMP;
			return LogicalType::TIMESTAMP;
		case Type::FLOAT:
			return LogicalType::FLOAT;
		case Type::DOUBLE:
			return LogicalType::DOUBLE;
		case Type::BYTE_ARRAY:
		case Type::FIXED_LEN_BYTE_ARRAY:
			if (parquet_options.binary_as_string) {
				return LogicalType::VARCHAR;
			}
			return LogicalType::BLOB;
		default:
			return LogicalType::INVALID;
		}
	}
}

ParquetColumnSchema ParquetReader::ParseColumnSchema(const SchemaElement &s_ele, idx_t max_define, idx_t max_repeat,
                                                     idx_t schema_index, idx_t column_index,
                                                     ParquetColumnSchemaType type) {
	ParquetColumnSchema schema(max_define, max_repeat, schema_index, column_index, type);
	schema.name = s_ele.name;
	schema.type = DeriveLogicalType(s_ele, schema);
	return schema;
}

unique_ptr<ColumnReader> ParquetReader::CreateReaderRecursive(ClientContext &context,
                                                              const vector<ColumnIndex> &indexes,
                                                              const ParquetColumnSchema &schema) {
	switch (schema.schema_type) {
	case ParquetColumnSchemaType::GEOMETRY:
		return metadata->geo_metadata->CreateColumnReader(*this, schema, context);
	case ParquetColumnSchemaType::FILE_ROW_NUMBER:
		return make_uniq<RowNumberColumnReader>(*this, schema);
	case ParquetColumnSchemaType::COLUMN: {
		if (schema.children.empty()) {
			// leaf reader
			return ColumnReader::CreateReader(*this, schema);
		}
		vector<unique_ptr<ColumnReader>> children;
		children.resize(schema.children.size());
		if (indexes.empty()) {
			for (idx_t child_index = 0; child_index < schema.children.size(); child_index++) {
				children[child_index] = CreateReaderRecursive(context, indexes, schema.children[child_index]);
			}
		} else {
			for (idx_t i = 0; i < indexes.size(); i++) {
				auto child_index = indexes[i].GetPrimaryIndex();
				children[child_index] =
				    CreateReaderRecursive(context, indexes[i].GetChildIndexes(), schema.children[child_index]);
			}
		}
		switch (schema.type.id()) {
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP:
			D_ASSERT(children.size() == 1);
			return make_uniq<ListColumnReader>(*this, schema, std::move(children[0]));
		case LogicalTypeId::STRUCT:
			return make_uniq<StructColumnReader>(*this, schema, std::move(children));
		default:
			throw InternalException("Unsupported schema type for schema with children");
		}
	}
	default:
		throw InternalException("Unsupported ParquetColumnSchemaType");
	}
}

unique_ptr<ColumnReader> ParquetReader::CreateReader(ClientContext &context) {
	auto ret = CreateReaderRecursive(context, reader_data.column_indexes, *root_schema);
	if (ret->Type().id() != LogicalTypeId::STRUCT) {
		throw InternalException("Root element of Parquet file must be a struct");
	}
	// add casts if required
	auto &root_struct_reader = ret->Cast<StructColumnReader>();
	for (auto &entry : reader_data.cast_map) {
		auto column_id = entry.first;
		auto &expected_type = entry.second;
		auto child_reader = std::move(root_struct_reader.child_readers[column_id]);
		auto cast_schema = make_uniq<ParquetColumnSchema>(child_reader->Schema(), expected_type);
		auto cast_reader = make_uniq<CastColumnReader>(std::move(child_reader), std::move(cast_schema));
		root_struct_reader.child_readers[column_id] = std::move(cast_reader);
	}
	return ret;
}

ParquetColumnSchema::ParquetColumnSchema(idx_t max_define, idx_t max_repeat, idx_t schema_index, idx_t column_index,
                                         ParquetColumnSchemaType schema_type)
    : ParquetColumnSchema(string(), LogicalTypeId::INVALID, max_define, max_repeat, schema_index, column_index,
                          schema_type) {
}

ParquetColumnSchema::ParquetColumnSchema(string name_p, LogicalType type_p, idx_t max_define, idx_t max_repeat,
                                         idx_t schema_index, idx_t column_index, ParquetColumnSchemaType schema_type)
    : schema_type(schema_type), name(std::move(name_p)), type(std::move(type_p)), max_define(max_define),
      max_repeat(max_repeat), schema_index(schema_index), column_index(column_index) {
}

ParquetColumnSchema::ParquetColumnSchema(ParquetColumnSchema parent, LogicalType cast_type,
                                         ParquetColumnSchemaType schema_type)
    : schema_type(schema_type), name(parent.name), type(std::move(cast_type)), max_define(parent.max_define),
      max_repeat(parent.max_repeat), schema_index(parent.schema_index), column_index(parent.column_index) {
	children.push_back(std::move(parent));
}

unique_ptr<BaseStatistics> ParquetColumnSchema::Stats(ParquetReader &reader, idx_t row_group_idx_p,
                                                      const vector<ColumnChunk> &columns) const {
	if (schema_type == ParquetColumnSchemaType::CAST) {
		auto stats = children[0].Stats(reader, row_group_idx_p, columns);
		return StatisticsPropagator::TryPropagateCast(*stats, children[0].type, type);
	}
	if (schema_type == ParquetColumnSchemaType::FILE_ROW_NUMBER) {
		auto stats = NumericStats::CreateUnknown(type);
		auto &row_groups = reader.GetFileMetadata()->row_groups;
		D_ASSERT(row_group_idx_p < row_groups.size());
		idx_t row_group_offset_min = 0;
		for (idx_t i = 0; i < row_group_idx_p; i++) {
			row_group_offset_min += row_groups[i].num_rows;
		}

		NumericStats::SetMin(stats, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_offset_min)));
		NumericStats::SetMax(stats, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_offset_min +
		                                                                     row_groups[row_group_idx_p].num_rows)));
		stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		return stats.ToUnique();
	}
	return ParquetStatisticsUtils::TransformColumnStatistics(*this, columns);
}

ParquetColumnSchema ParquetReader::ParseSchemaRecursive(idx_t depth, idx_t max_define, idx_t max_repeat,
                                                        idx_t &next_schema_idx, idx_t &next_file_idx) {

	auto file_meta_data = GetFileMetadata();
	D_ASSERT(file_meta_data);
	D_ASSERT(next_schema_idx < file_meta_data->schema.size());
	auto &s_ele = file_meta_data->schema[next_schema_idx];
	auto this_idx = next_schema_idx;

	auto repetition_type = FieldRepetitionType::REQUIRED;
	if (s_ele.__isset.repetition_type && this_idx > 0) {
		repetition_type = s_ele.repetition_type;
	}
	if (repetition_type != FieldRepetitionType::REQUIRED) {
		max_define++;
	}
	if (repetition_type == FieldRepetitionType::REPEATED) {
		max_repeat++;
	}

	// Check for geoparquet spatial types
	if (depth == 1) {
		// geoparquet types have to be at the root of the schema, and have to be present in the kv metadata
		if (metadata->geo_metadata && metadata->geo_metadata->IsGeometryColumn(s_ele.name)) {
			auto root_schema = ParseColumnSchema(s_ele, max_define, max_repeat, this_idx, next_file_idx++);
			return ParquetColumnSchema(std::move(root_schema), GeoParquetFileMetadata::GeometryType(),
			                           ParquetColumnSchemaType::GEOMETRY);
		}
	}

	if (s_ele.__isset.num_children && s_ele.num_children > 0) { // inner node
		vector<ParquetColumnSchema> child_schemas;

		idx_t c_idx = 0;
		while (c_idx < NumericCast<idx_t>(s_ele.num_children)) {
			next_schema_idx++;

			auto child_schema = ParseSchemaRecursive(depth + 1, max_define, max_repeat, next_schema_idx, next_file_idx);
			child_schemas.push_back(std::move(child_schema));
			c_idx++;
		}
		// rename child type entries if there are case-insensitive duplicates by appending _1, _2 etc.
		// behavior consistent with CSV reader fwiw
		case_insensitive_map_t<idx_t> name_collision_count;
		for (auto &child_schema : child_schemas) {
			auto &col_name = child_schema.name;
			// avoid duplicate header names
			while (name_collision_count.find(col_name) != name_collision_count.end()) {
				name_collision_count[col_name] += 1;
				col_name = col_name + "_" + to_string(name_collision_count[col_name]);
			}
			child_schema.name = col_name;
			name_collision_count[col_name] = 0;
		}

		bool is_repeated = repetition_type == FieldRepetitionType::REPEATED;
		bool is_list = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::LIST;
		bool is_map = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP;
		bool is_map_kv = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP_KEY_VALUE;
		if (!is_map_kv && this_idx > 0) {
			// check if the parent node of this is a map
			auto &p_ele = file_meta_data->schema[this_idx - 1];
			bool parent_is_map = p_ele.__isset.converted_type && p_ele.converted_type == ConvertedType::MAP;
			bool parent_has_children = p_ele.__isset.num_children && p_ele.num_children == 1;
			is_map_kv = parent_is_map && parent_has_children;
		}

		if (is_map_kv) {
			if (child_schemas.size() != 2) {
				throw IOException("MAP_KEY_VALUE requires two children");
			}
			if (!is_repeated) {
				throw IOException("MAP_KEY_VALUE needs to be repeated");
			}
			auto result_type = LogicalType::MAP(child_schemas[0].type, child_schemas[1].type);
			ParquetColumnSchema struct_schema(s_ele.name, ListType::GetChildType(result_type), max_define - 1,
			                                  max_repeat - 1, this_idx, next_file_idx);
			struct_schema.children = std::move(child_schemas);

			ParquetColumnSchema map_schema(s_ele.name, std::move(result_type), max_define, max_repeat, this_idx,
			                               next_file_idx);
			map_schema.children.push_back(std::move(struct_schema));
			return map_schema;
		}
		ParquetColumnSchema result;
		if (child_schemas.size() > 1 || (!is_list && !is_map && !is_repeated)) {
			child_list_t<LogicalType> struct_types;
			for (auto &child_schema : child_schemas) {
				struct_types.emplace_back(make_pair(child_schema.name, child_schema.type));
			}

			auto result_type = LogicalType::STRUCT(std::move(struct_types));
			ParquetColumnSchema struct_schema(s_ele.name, std::move(result_type), max_define, max_repeat, this_idx,
			                                  next_file_idx);
			struct_schema.children = std::move(child_schemas);
			result = std::move(struct_schema);
		} else {
			// if we have a struct with only a single type, pull up
			result = std::move(child_schemas[0]);
			result.name = s_ele.name;
		}
		if (is_repeated) {
			auto list_type = LogicalType::LIST(result.type);
			ParquetColumnSchema list_schema(s_ele.name, std::move(list_type), max_define, max_repeat, this_idx,
			                                next_file_idx);
			list_schema.children.push_back(std::move(result));
			result = std::move(list_schema);
		}
		result.parent_schema_index = this_idx;
		return result;
	} else { // leaf node
		if (!s_ele.__isset.type) {
			throw InvalidInputException(
			    "Node has neither num_children nor type set - this violates the Parquet spec (corrupted file)");
		}
		auto result = ParseColumnSchema(s_ele, max_define, max_repeat, this_idx, next_file_idx++);
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			auto list_type = LogicalType::LIST(result.type);
			ParquetColumnSchema list_schema(s_ele.name, std::move(list_type), max_define, max_repeat, this_idx,
			                                next_file_idx);
			list_schema.children.push_back(std::move(result));
			return list_schema;
		}
		return result;
	}
}

unique_ptr<ParquetColumnSchema> ParquetReader::ParseSchema() {
	auto file_meta_data = GetFileMetadata();
	idx_t next_schema_idx = 0;
	idx_t next_file_idx = 0;

	if (file_meta_data->schema.empty()) {
		throw IOException("Parquet reader: no schema elements found");
	}
	if (file_meta_data->schema[0].num_children == 0) {
		throw IOException("Parquet reader: root schema element has no children");
	}
	auto root = ParseSchemaRecursive(0, 0, 0, next_schema_idx, next_file_idx);
	if (root.type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("Root element of Parquet file must be a struct");
	}
	D_ASSERT(next_schema_idx == file_meta_data->schema.size() - 1);
	D_ASSERT(file_meta_data->row_groups.empty() || next_file_idx == file_meta_data->row_groups[0].columns.size());
	if (parquet_options.file_row_number) {
		for (auto &column : root.children) {
			auto &name = column.name;
			if (StringUtil::CIEquals(name, "file_row_number")) {
				throw BinderException(
				    "Using file_row_number option on file with column named file_row_number is not supported");
			}
		}
		ParquetColumnSchema file_row_number("file_row_number", LogicalType::BIGINT, 0, 0, 0, 0,
		                                    ParquetColumnSchemaType::FILE_ROW_NUMBER);
		root.children.push_back(std::move(file_row_number));
	}
	return make_uniq<ParquetColumnSchema>(root);
}

void ParquetReader::InitializeSchema(ClientContext &context) {
	auto file_meta_data = GetFileMetadata();

	if (file_meta_data->__isset.encryption_algorithm) {
		if (file_meta_data->encryption_algorithm.__isset.AES_GCM_CTR_V1) {
			throw InvalidInputException("File '%s' is encrypted with AES_GCM_CTR_V1, but only AES_GCM_V1 is supported",
			                            file_name);
		}
	}
	// check if we like this schema
	if (file_meta_data->schema.size() < 2) {
		throw InvalidInputException("Failed to read Parquet file '%s': Need at least one non-root column in the file",
		                            file_name);
	}
	root_schema = ParseSchema();
	for (idx_t i = 0; i < root_schema->children.size(); i++) {
		auto &element = root_schema->children[i];
		auto column = MultiFileReaderColumnDefinition(element.name, element.type);
		auto &column_schema = file_meta_data->schema[element.schema_index];

		if (column_schema.__isset.field_id) {
			column.identifier = Value::INTEGER(column_schema.field_id);
		} else if (element.parent_schema_index.IsValid()) {
			auto &parent_column_schema = file_meta_data->schema[element.parent_schema_index.GetIndex()];
			if (parent_column_schema.__isset.field_id) {
				column.identifier = Value::INTEGER(parent_column_schema.field_id);
			}
		}
		columns.emplace_back(std::move(column));
	}
}

ParquetOptions::ParquetOptions(ClientContext &context) {
	Value binary_as_string_val;
	if (context.TryGetCurrentSetting("binary_as_string", binary_as_string_val)) {
		binary_as_string = binary_as_string_val.GetValue<bool>();
	}
}

ParquetColumnDefinition ParquetColumnDefinition::FromSchemaValue(ClientContext &context, const Value &column_value) {
	ParquetColumnDefinition result;
	auto &identifier = StructValue::GetChildren(column_value)[0];
	result.identifier = identifier;

	const auto &column_def = StructValue::GetChildren(column_value)[1];
	D_ASSERT(column_def.type().id() == LogicalTypeId::STRUCT);

	const auto children = StructValue::GetChildren(column_def);
	result.name = StringValue::Get(children[0]);
	result.type = TransformStringToLogicalType(StringValue::Get(children[1]));
	string error_message;
	if (!children[2].TryCastAs(context, result.type, result.default_value, &error_message)) {
		throw BinderException("Unable to cast Parquet schema default_value \"%s\" to %s", children[2].ToString(),
		                      result.type.ToString());
	}

	return result;
}

ParquetReader::ParquetReader(ClientContext &context_p, string file_name_p, ParquetOptions parquet_options_p,
                             shared_ptr<ParquetFileMetadataCache> metadata_p)
    : BaseFileReader(std::move(file_name_p)), fs(FileSystem::GetFileSystem(context_p)),
      allocator(BufferAllocator::Get(context_p)), parquet_options(std::move(parquet_options_p)) {
	file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
	if (!file_handle->CanSeek()) {
		throw NotImplementedException(
		    "Reading parquet files from a FIFO stream is not supported and cannot be efficiently supported since "
		    "metadata is located at the end of the file. Write the stream to disk first and read from there instead.");
	}

	// set pointer to factory method for AES state
	auto &config = DBConfig::GetConfig(context_p);
	if (config.encryption_util && parquet_options.debug_use_openssl) {
		encryption_util = config.encryption_util;
	} else {
		encryption_util = make_shared_ptr<duckdb_mbedtls::MbedTlsWrapper::AESGCMStateMBEDTLSFactory>();
	}

	// If metadata cached is disabled
	// or if this file has cached metadata
	// or if the cached version already expired
	if (!metadata_p) {
		Value metadata_cache = false;
		context_p.TryGetCurrentSetting("parquet_metadata_cache", metadata_cache);
		if (!metadata_cache.GetValue<bool>()) {
			metadata =
			    LoadMetadata(context_p, allocator, *file_handle, parquet_options.encryption_config, *encryption_util);
		} else {
			auto last_modify_time = fs.GetLastModifiedTime(*file_handle);
			metadata = ObjectCache::GetObjectCache(context_p).Get<ParquetFileMetadataCache>(file_name);
			if (!metadata || (last_modify_time + 10 >= metadata->read_time)) {
				metadata = LoadMetadata(context_p, allocator, *file_handle, parquet_options.encryption_config,
				                        *encryption_util);
				ObjectCache::GetObjectCache(context_p).Put(file_name, metadata);
			}
		}
	} else {
		metadata = std::move(metadata_p);
	}
	InitializeSchema(context_p);
}

ParquetUnionData::~ParquetUnionData() {
}

ParquetReader::ParquetReader(ClientContext &context_p, ParquetOptions parquet_options_p,
                             shared_ptr<ParquetFileMetadataCache> metadata_p)
    : BaseFileReader(string()), fs(FileSystem::GetFileSystem(context_p)), allocator(BufferAllocator::Get(context_p)),
      metadata(std::move(metadata_p)), parquet_options(std::move(parquet_options_p)), rows_read(0) {
	InitializeSchema(context_p);
}

ParquetReader::~ParquetReader() {
}

const FileMetaData *ParquetReader::GetFileMetadata() const {
	D_ASSERT(metadata);
	D_ASSERT(metadata->metadata);
	return metadata->metadata.get();
}

unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(const string &name) {
	idx_t file_col_idx;
	for (file_col_idx = 0; file_col_idx < columns.size(); file_col_idx++) {
		if (columns[file_col_idx].name == name) {
			break;
		}
	}
	if (file_col_idx == columns.size()) {
		return nullptr;
	}

	unique_ptr<BaseStatistics> column_stats;
	auto file_meta_data = GetFileMetadata();
	auto &column_schema = root_schema->children[file_col_idx];

	for (idx_t row_group_idx = 0; row_group_idx < file_meta_data->row_groups.size(); row_group_idx++) {
		auto &row_group = file_meta_data->row_groups[row_group_idx];
		auto chunk_stats = column_schema.Stats(*this, row_group_idx, row_group.columns);
		if (!chunk_stats) {
			return nullptr;
		}
		if (!column_stats) {
			column_stats = std::move(chunk_stats);
		} else {
			column_stats->Merge(*chunk_stats);
		}
	}
	return column_stats;
}

unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(ClientContext &context, ParquetOptions parquet_options,
                                                         shared_ptr<ParquetFileMetadataCache> metadata,
                                                         const string &name) {
	ParquetReader reader(context, std::move(parquet_options), std::move(metadata));
	return reader.ReadStatistics(name);
}

uint32_t ParquetReader::Read(duckdb_apache::thrift::TBase &object, TProtocol &iprot) {
	if (parquet_options.encryption_config) {
		return ParquetCrypto::Read(object, iprot, parquet_options.encryption_config->GetFooterKey(), *encryption_util);
	} else {
		return object.read(&iprot);
	}
}

uint32_t ParquetReader::ReadData(duckdb_apache::thrift::protocol::TProtocol &iprot, const data_ptr_t buffer,
                                 const uint32_t buffer_size) {
	if (parquet_options.encryption_config) {
		return ParquetCrypto::ReadData(iprot, buffer, buffer_size, parquet_options.encryption_config->GetFooterKey(),
		                               *encryption_util);
	} else {
		return iprot.getTransport()->read(buffer, buffer_size);
	}
}

const ParquetRowGroup &ParquetReader::GetGroup(ParquetReaderScanState &state) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(state.current_group >= 0 && (idx_t)state.current_group < state.group_idx_list.size());
	D_ASSERT(state.group_idx_list[state.current_group] < file_meta_data->row_groups.size());
	return file_meta_data->row_groups[state.group_idx_list[state.current_group]];
}

uint64_t ParquetReader::GetGroupCompressedSize(ParquetReaderScanState &state) {
	auto &group = GetGroup(state);
	auto total_compressed_size = group.total_compressed_size;

	idx_t calc_compressed_size = 0;

	// If the global total_compressed_size is not set, we can still calculate it
	if (group.total_compressed_size == 0) {
		for (auto &column_chunk : group.columns) {
			calc_compressed_size += column_chunk.meta_data.total_compressed_size;
		}
	}

	if (total_compressed_size != 0 && calc_compressed_size != 0 &&
	    (idx_t)total_compressed_size != calc_compressed_size) {
		throw InvalidInputException("mismatch between calculated compressed size and reported compressed size");
	}

	return total_compressed_size ? total_compressed_size : calc_compressed_size;
}

uint64_t ParquetReader::GetGroupSpan(ParquetReaderScanState &state) {
	auto &group = GetGroup(state);
	idx_t min_offset = NumericLimits<idx_t>::Maximum();
	idx_t max_offset = NumericLimits<idx_t>::Minimum();

	for (auto &column_chunk : group.columns) {

		// Set the min offset
		idx_t current_min_offset = NumericLimits<idx_t>::Maximum();
		if (column_chunk.meta_data.__isset.dictionary_page_offset) {
			current_min_offset = MinValue<idx_t>(current_min_offset, column_chunk.meta_data.dictionary_page_offset);
		}
		if (column_chunk.meta_data.__isset.index_page_offset) {
			current_min_offset = MinValue<idx_t>(current_min_offset, column_chunk.meta_data.index_page_offset);
		}
		current_min_offset = MinValue<idx_t>(current_min_offset, column_chunk.meta_data.data_page_offset);
		min_offset = MinValue<idx_t>(current_min_offset, min_offset);
		max_offset = MaxValue<idx_t>(max_offset, column_chunk.meta_data.total_compressed_size + current_min_offset);
	}

	return max_offset - min_offset;
}

idx_t ParquetReader::GetGroupOffset(ParquetReaderScanState &state) {
	auto &group = GetGroup(state);
	idx_t min_offset = NumericLimits<idx_t>::Maximum();

	for (auto &column_chunk : group.columns) {
		if (column_chunk.meta_data.__isset.dictionary_page_offset) {
			min_offset = MinValue<idx_t>(min_offset, column_chunk.meta_data.dictionary_page_offset);
		}
		if (column_chunk.meta_data.__isset.index_page_offset) {
			min_offset = MinValue<idx_t>(min_offset, column_chunk.meta_data.index_page_offset);
		}
		min_offset = MinValue<idx_t>(min_offset, column_chunk.meta_data.data_page_offset);
	}

	return min_offset;
}

static FilterPropagateResult CheckParquetStringFilter(BaseStatistics &stats, const Statistics &pq_col_stats,
                                                      TableFilter &filter) {
	if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto &min_value = pq_col_stats.min_value;
		auto &max_value = pq_col_stats.max_value;
		return StringStats::CheckZonemap(const_data_ptr_cast(min_value.c_str()), min_value.size(),
		                                 const_data_ptr_cast(max_value.c_str()), max_value.size(),
		                                 constant_filter.comparison_type, StringValue::Get(constant_filter.constant));
	} else {
		return filter.CheckStatistics(stats);
	}
}

void ParquetReader::PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t i) {
	auto &group = GetGroup(state);
	auto col_idx = MultiFileLocalIndex(i);
	auto column_id = reader_data.column_ids[col_idx];
	auto &column_reader = state.root_reader->Cast<StructColumnReader>().GetChildReader(column_id);

	if (reader_data.filters) {
		auto stats = column_reader.Stats(state.group_idx_list[state.current_group], group.columns);
		// filters contain output chunk index, not file col idx!
		auto global_index = reader_data.column_mapping[col_idx];
		auto filter_entry = reader_data.filters->filters.find(global_index);

		if (stats && filter_entry != reader_data.filters->filters.end()) {
			auto &filter = *filter_entry->second;

			FilterPropagateResult prune_result;
			// TODO we might not have stats but STILL a bloom filter so move this up
			// check the bloom filter if present
			bool is_generated_column = column_reader.ColumnIndex() >= group.columns.size();
			bool is_cast = column_reader.Schema().schema_type == ::duckdb::ParquetColumnSchemaType::CAST;
			if (!column_reader.Type().IsNested() && !is_generated_column && !is_cast &&
			    ParquetStatisticsUtils::BloomFilterSupported(column_reader.Type().id()) &&
			    ParquetStatisticsUtils::BloomFilterExcludes(filter,
			                                                group.columns[column_reader.ColumnIndex()].meta_data,
			                                                *state.thrift_file_proto, allocator)) {
				prune_result = FilterPropagateResult::FILTER_ALWAYS_FALSE;
			} else if (column_reader.Type().id() == LogicalTypeId::VARCHAR && !is_generated_column && !is_cast &&
			           group.columns[column_reader.ColumnIndex()].meta_data.statistics.__isset.min_value &&
			           group.columns[column_reader.ColumnIndex()].meta_data.statistics.__isset.max_value) {

				// our StringStats only store the first 8 bytes of strings (even if Parquet has longer string stats)
				// however, when reading remote Parquet files, skipping row groups is really important
				// here, we implement a special case to check the full length for string filters
				if (filter.filter_type == TableFilterType::CONJUNCTION_AND) {
					const auto &and_filter = filter.Cast<ConjunctionAndFilter>();
					auto and_result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
					for (auto &child_filter : and_filter.child_filters) {
						auto child_prune_result = CheckParquetStringFilter(
						    *stats, group.columns[column_reader.ColumnIndex()].meta_data.statistics, *child_filter);
						if (child_prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
							and_result = FilterPropagateResult::FILTER_ALWAYS_FALSE;
							break;
						} else if (child_prune_result != and_result) {
							and_result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
						}
					}
					prune_result = and_result;
				} else {
					prune_result = CheckParquetStringFilter(
					    *stats, group.columns[column_reader.ColumnIndex()].meta_data.statistics, filter);
				}
			} else {
				prune_result = filter.CheckStatistics(*stats);
			}

			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				// this effectively will skip this chunk
				state.group_offset = group.num_rows;
				return;
			}
		}
	}

	state.root_reader->InitializeRead(state.group_idx_list[state.current_group], group.columns,
	                                  *state.thrift_file_proto);
}

idx_t ParquetReader::NumRows() const {
	return GetFileMetadata()->num_rows;
}

idx_t ParquetReader::NumRowGroups() const {
	return GetFileMetadata()->row_groups.size();
}

ParquetScanFilter::ParquetScanFilter(ClientContext &context, idx_t filter_idx, TableFilter &filter)
    : filter_idx(filter_idx), filter(filter) {
	filter_state = TableFilterState::Initialize(filter);
}

ParquetScanFilter::~ParquetScanFilter() {
}

void ParquetReader::InitializeScan(ClientContext &context, ParquetReaderScanState &state,
                                   vector<idx_t> groups_to_read) {
	state.current_group = -1;
	state.finished = false;
	state.group_offset = 0;
	state.group_idx_list = std::move(groups_to_read);
	state.sel.Initialize(STANDARD_VECTOR_SIZE);
	if (!state.file_handle || state.file_handle->path != file_handle->path) {
		auto flags = FileFlags::FILE_FLAGS_READ;

		Value disable_prefetch = false;
		Value prefetch_all_files = false;
		context.TryGetCurrentSetting("disable_parquet_prefetching", disable_prefetch);
		context.TryGetCurrentSetting("prefetch_all_parquet_files", prefetch_all_files);
		bool should_prefetch = !file_handle->OnDiskFile() || prefetch_all_files.GetValue<bool>();
		bool can_prefetch = file_handle->CanSeek() && !disable_prefetch.GetValue<bool>();

		if (should_prefetch && can_prefetch) {
			state.prefetch_mode = true;
			flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
		} else {
			state.prefetch_mode = false;
		}

		state.file_handle = fs.OpenFile(file_handle->path, flags);
	}
	state.thrift_file_proto = CreateThriftFileProtocol(allocator, *state.file_handle, state.prefetch_mode);
	state.root_reader = CreateReader(context);
	state.define_buf.resize(allocator, STANDARD_VECTOR_SIZE);
	state.repeat_buf.resize(allocator, STANDARD_VECTOR_SIZE);
}

void ParquetReader::Scan(ClientContext &context, ParquetReaderScanState &state, DataChunk &result) {
	while (ScanInternal(context, state, result)) {
		if (result.size() > 0) {
			break;
		}
		result.Reset();
	}
}

bool ParquetReader::ScanInternal(ClientContext &context, ParquetReaderScanState &state, DataChunk &result) {
	if (state.finished) {
		return false;
	}

	// see if we have to switch to the next row group in the parquet file
	if (state.current_group < 0 || (int64_t)state.group_offset >= GetGroup(state).num_rows) {
		state.current_group++;
		state.group_offset = 0;

		auto &trans = reinterpret_cast<ThriftFileTransport &>(*state.thrift_file_proto->getTransport());
		trans.ClearPrefetch();
		state.current_group_prefetched = false;

		if ((idx_t)state.current_group == state.group_idx_list.size()) {
			state.finished = true;
			return false;
		}

		uint64_t to_scan_compressed_bytes = 0;
		for (idx_t i = 0; i < reader_data.column_ids.size(); i++) {
			auto col_idx = MultiFileLocalIndex(i);
			PrepareRowGroupBuffer(state, col_idx);

			auto file_col_idx = reader_data.column_ids[col_idx];

			auto &root_reader = state.root_reader->Cast<StructColumnReader>();
			to_scan_compressed_bytes += root_reader.GetChildReader(file_col_idx).TotalCompressedSize();
		}

		auto &group = GetGroup(state);
		if (state.prefetch_mode && state.group_offset != (idx_t)group.num_rows) {
			uint64_t total_row_group_span = GetGroupSpan(state);

			double scan_percentage = (double)(to_scan_compressed_bytes) / static_cast<double>(total_row_group_span);

			if (to_scan_compressed_bytes > total_row_group_span) {
				throw IOException(
				    "The parquet file '%s' seems to have incorrectly set page offsets. This interferes with DuckDB's "
				    "prefetching optimization. DuckDB may still be able to scan this file by manually disabling the "
				    "prefetching mechanism using: 'SET disable_parquet_prefetching=true'.",
				    file_name);
			}

			if (!reader_data.filters &&
			    scan_percentage > ParquetReaderPrefetchConfig::WHOLE_GROUP_PREFETCH_MINIMUM_SCAN) {
				// Prefetch the whole row group
				if (!state.current_group_prefetched) {
					auto total_compressed_size = GetGroupCompressedSize(state);
					if (total_compressed_size > 0) {
						trans.Prefetch(GetGroupOffset(state), total_row_group_span);
					}
					state.current_group_prefetched = true;
				}
			} else {
				// lazy fetching is when all tuples in a column can be skipped. With lazy fetching the buffer is only
				// fetched on the first read to that buffer.
				bool lazy_fetch = reader_data.filters;

				// Prefetch column-wise
				for (idx_t i = 0; i < reader_data.column_ids.size(); i++) {
					auto col_idx = MultiFileLocalIndex(i);
					auto file_col_idx = reader_data.column_ids[col_idx];
					auto &root_reader = state.root_reader->Cast<StructColumnReader>();

					bool has_filter = false;
					if (reader_data.filters) {
						auto global_idx = reader_data.column_mapping[col_idx];
						auto entry = reader_data.filters->filters.find(global_idx.GetIndex());
						has_filter = entry != reader_data.filters->filters.end();
					}
					root_reader.GetChildReader(file_col_idx).RegisterPrefetch(trans, !(lazy_fetch && !has_filter));
				}

				trans.FinalizeRegistration();

				if (!lazy_fetch) {
					trans.PrefetchRegistered();
				}
			}
		}
		return true;
	}

	auto scan_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, GetGroup(state).num_rows - state.group_offset);
	result.SetCardinality(scan_count);

	if (scan_count == 0) {
		state.finished = true;
		return false; // end of last group, we are done
	}

	state.define_buf.zero();
	state.repeat_buf.zero();

	auto define_ptr = (uint8_t *)state.define_buf.ptr;
	auto repeat_ptr = (uint8_t *)state.repeat_buf.ptr;

	auto &root_reader = state.root_reader->Cast<StructColumnReader>();

	if (reader_data.filters) {
		idx_t filter_count = result.size();
		vector<bool> need_to_read(reader_data.column_ids.size(), true);

		state.sel.Initialize(nullptr);
		if (state.scan_filters.empty()) {
			state.adaptive_filter = make_uniq<AdaptiveFilter>(*reader_data.filters);
			for (auto &entry : reader_data.filters->filters) {
				state.scan_filters.emplace_back(context, entry.first, *entry.second);
			}
		}
		D_ASSERT(state.scan_filters.size() == reader_data.filters->filters.size());

		// first load the columns that are used in filters
		auto filter_state = state.adaptive_filter->BeginFilter();
		for (idx_t i = 0; i < state.scan_filters.size(); i++) {
			if (filter_count == 0) {
				// if no rows are left we can stop checking filters
				break;
			}
			auto &scan_filter = state.scan_filters[state.adaptive_filter->permutation[i]];
			auto global_idx = MultiFileGlobalIndex(scan_filter.filter_idx);
			auto filter_entry = reader_data.filter_map[global_idx];
			D_ASSERT(filter_entry.IsSet());
			if (filter_entry.IsConstant()) {
				// this is a constant vector, look for the constant
				auto constant_index = filter_entry.GetConstantIndex();
				auto &constant = reader_data.constant_map[constant_index].value;
				Vector constant_vector(constant);
				ColumnReader::ApplyFilter(constant_vector, scan_filter.filter, *scan_filter.filter_state, scan_count,
				                          state.sel, filter_count);
			} else {
				auto local_idx = filter_entry.GetLocalIndex();
				auto column_id = reader_data.column_ids[local_idx];
				auto result_idx = reader_data.column_mapping[local_idx];

				auto &result_vector = result.data[result_idx];
				auto &child_reader = root_reader.GetChildReader(column_id);
				child_reader.Filter(scan_count, define_ptr, repeat_ptr, result_vector, scan_filter.filter,
				                    *scan_filter.filter_state, state.sel, filter_count, i == 0);
				need_to_read[local_idx] = false;
			}
		}
		state.adaptive_filter->EndFilter(filter_state);

		// we still may have to read some cols
		for (idx_t i = 0; i < reader_data.column_ids.size(); i++) {
			auto col_idx = MultiFileLocalIndex(i);
			if (!need_to_read[col_idx]) {
				continue;
			}
			auto file_col_idx = reader_data.column_ids[col_idx];
			if (filter_count == 0) {
				root_reader.GetChildReader(file_col_idx).Skip(result.size());
				continue;
			}
			auto &result_vector = result.data[reader_data.column_mapping[col_idx].GetIndex()];
			auto &child_reader = root_reader.GetChildReader(file_col_idx);
			child_reader.Select(result.size(), define_ptr, repeat_ptr, result_vector, state.sel, filter_count);
		}
		if (scan_count != filter_count) {
			result.Slice(state.sel, filter_count);
		}
	} else {
		for (idx_t i = 0; i < reader_data.column_ids.size(); i++) {
			auto col_idx = MultiFileLocalIndex(i);
			auto file_col_idx = reader_data.column_ids[col_idx];
			auto global_col_idx = reader_data.column_mapping[col_idx];
			auto &result_vector = result.data[global_col_idx];
			auto &child_reader = root_reader.GetChildReader(file_col_idx);
			auto rows_read = child_reader.Read(scan_count, define_ptr, repeat_ptr, result_vector);
			if (rows_read != scan_count) {
				throw InvalidInputException("Mismatch in parquet read for column %llu, expected %llu rows, got %llu",
				                            file_col_idx, scan_count, rows_read);
			}
		}
	}

	rows_read += scan_count;
	state.group_offset += scan_count;
	return true;
}

} // namespace duckdb
