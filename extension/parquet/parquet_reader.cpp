#include "parquet_reader.hpp"

#include "reader/boolean_column_reader.hpp"
#include "reader/callback_column_reader.hpp"
#include "column_reader.hpp"
#include "duckdb.hpp"
#include "reader/expression_column_reader.hpp"
#include "parquet_geometry.hpp"
#include "reader/list_column_reader.hpp"
#include "parquet_crypto.hpp"
#include "parquet_file_metadata_cache.hpp"
#include "parquet_statistics.hpp"
#include "parquet_timestamp.hpp"
#include "mbedtls_wrapper.hpp"
#include "reader/row_number_column_reader.hpp"
#include "reader/string_column_reader.hpp"
#include "reader/variant_column_reader.hpp"
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
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/logging/log_manager.hpp"

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
CreateThriftFileProtocol(QueryContext context, CachingFileHandle &file_handle, bool prefetch_mode) {
	auto transport = duckdb_base_std::make_shared<ThriftFileTransport>(file_handle, prefetch_mode);
	return make_uniq<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(std::move(transport));
}

static bool ShouldAndCanPrefetch(ClientContext &context, CachingFileHandle &file_handle) {
	Value disable_prefetch = false;
	Value prefetch_all_files = false;
	context.TryGetCurrentSetting("disable_parquet_prefetching", disable_prefetch);
	context.TryGetCurrentSetting("prefetch_all_parquet_files", prefetch_all_files);
	bool should_prefetch = !file_handle.OnDiskFile() || prefetch_all_files.GetValue<bool>();
	bool can_prefetch = file_handle.CanSeek() && !disable_prefetch.GetValue<bool>();
	return should_prefetch && can_prefetch;
}

static void ParseParquetFooter(data_ptr_t buffer, const string &file_path, idx_t file_size,
                               const shared_ptr<const ParquetEncryptionConfig> &encryption_config, uint32_t &footer_len,
                               bool &footer_encrypted) {
	if (memcmp(buffer + 4, "PAR1", 4) == 0) {
		footer_encrypted = false;
		if (encryption_config) {
			throw InvalidInputException("File '%s' is not encrypted, but 'encryption_config' was set", file_path);
		}
	} else if (memcmp(buffer + 4, "PARE", 4) == 0) {
		footer_encrypted = true;
		if (!encryption_config) {
			throw InvalidInputException("File '%s' is encrypted, but 'encryption_config' was not set", file_path);
		}
	} else {
		throw InvalidInputException("No magic bytes found at end of file '%s'", file_path);
	}

	// read four-byte footer length from just before the end magic bytes
	footer_len = Load<uint32_t>(buffer);
	if (footer_len == 0 || file_size < 12 + footer_len) {
		throw InvalidInputException("Footer length error in file '%s'", file_path);
	}
}

static shared_ptr<ParquetFileMetadataCache>
LoadMetadata(ClientContext &context, Allocator &allocator, CachingFileHandle &file_handle,
             const shared_ptr<const ParquetEncryptionConfig> &encryption_config, const EncryptionUtil &encryption_util,
             optional_idx footer_size) {
	auto file_proto = CreateThriftFileProtocol(context, file_handle, false);
	auto &transport = reinterpret_cast<ThriftFileTransport &>(*file_proto->getTransport());
	auto file_size = transport.GetSize();
	if (file_size < 12) {
		throw InvalidInputException("File '%s' too small to be a Parquet file", file_handle.GetPath());
	}

	bool footer_encrypted;
	uint32_t footer_len;
	// footer size is not provided - read it from the back
	if (!footer_size.IsValid()) {
		// We have to do two reads here:
		// 1. The 8 bytes from the back to check if it's a Parquet file and the footer size
		// 2. The footer (after getting the size)
		// For local reads this doesn't matter much, but for remote reads this means two round trips,
		// which is especially bad for small Parquet files where the read cost is mostly round trips.
		// So, we prefetch more, to hopefully save a round trip.
		static constexpr idx_t ESTIMATED_FOOTER_RATIO = 1000; // Estimate 1/1000th of the file to be footer
		static constexpr idx_t MIN_PREFETCH_SIZE = 16384;     // Prefetch at least this many bytes
		static constexpr idx_t MAX_PREFETCH_SIZE = 262144;    // Prefetch at most this many bytes
		idx_t prefetch_size = 8;
		if (ShouldAndCanPrefetch(context, file_handle)) {
			prefetch_size = ClampValue(file_size / ESTIMATED_FOOTER_RATIO, MIN_PREFETCH_SIZE, MAX_PREFETCH_SIZE);
			prefetch_size = MinValue(NextPowerOfTwo(prefetch_size), file_size);
		}

		ResizeableBuffer buf;
		buf.resize(allocator, 8);
		buf.zero();

		transport.Prefetch(file_size - prefetch_size, prefetch_size);
		transport.SetLocation(file_size - 8);
		transport.read(buf.ptr, 8);

		ParseParquetFooter(buf.ptr, file_handle.GetPath(), file_size, encryption_config, footer_len, footer_encrypted);

		auto metadata_pos = file_size - (footer_len + 8);
		transport.SetLocation(metadata_pos);
		if (footer_len > prefetch_size - 8) {
			transport.Prefetch(metadata_pos, footer_len);
		}
	} else {
		footer_len = UnsafeNumericCast<uint32_t>(footer_size.GetIndex());
		if (footer_len == 0 || file_size < 12 + footer_len) {
			throw InvalidInputException("Invalid footer length provided for file '%s'", file_handle.GetPath());
		}

		idx_t total_footer_len = footer_len + 8;
		auto metadata_pos = file_size - total_footer_len;
		transport.SetLocation(metadata_pos);
		transport.Prefetch(metadata_pos, total_footer_len);

		auto read_head = transport.GetReadHead(metadata_pos);
		auto data_ptr = read_head->buffer_ptr;

		uint32_t read_footer_len;
		ParseParquetFooter(data_ptr + footer_len, file_handle.GetPath(), file_size, encryption_config, read_footer_len,
		                   footer_encrypted);
		if (read_footer_len != footer_len) {
			throw InvalidInputException("Parquet footer length stored in file is not equal to footer length provided");
		}
	}

	auto metadata = make_uniq<FileMetaData>();
	if (footer_encrypted) {
		auto crypto_metadata = make_uniq<FileCryptoMetaData>();
		crypto_metadata->read(file_proto.get());
		if (crypto_metadata->encryption_algorithm.__isset.AES_GCM_CTR_V1) {
			throw InvalidInputException("File '%s' is encrypted with AES_GCM_CTR_V1, but only AES_GCM_V1 is supported",
			                            file_handle.GetPath());
		}
		ParquetCrypto::Read(*metadata, *file_proto, encryption_config->GetFooterKey(), encryption_util);
	} else {
		metadata->read(file_proto.get());
	}

	// Try to read the GeoParquet metadata (if present)
	auto geo_metadata = GeoParquetFileMetadata::TryRead(*metadata, context);
	return make_shared_ptr<ParquetFileMetadataCache>(std::move(metadata), file_handle, std::move(geo_metadata),
	                                                 footer_len);
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
	case ParquetColumnSchemaType::FILE_ROW_NUMBER:
		return make_uniq<RowNumberColumnReader>(*this, schema);
	case ParquetColumnSchemaType::GEOMETRY: {
		return GeometryColumnReader::Create(*this, schema, context);
	}
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
	case ParquetColumnSchemaType::VARIANT: {
		if (schema.children.size() < 2) {
			throw InternalException("VARIANT schema type used for a non-variant type column");
		}
		vector<unique_ptr<ColumnReader>> children;
		children.resize(schema.children.size());
		for (idx_t child_index = 0; child_index < schema.children.size(); child_index++) {
			children[child_index] = CreateReaderRecursive(context, indexes, schema.children[child_index]);
		}
		return make_uniq<VariantColumnReader>(context, *this, schema, std::move(children));
	}
	default:
		throw InternalException("Unsupported ParquetColumnSchemaType");
	}
}

unique_ptr<ColumnReader> ParquetReader::CreateReader(ClientContext &context) {
	auto ret = CreateReaderRecursive(context, column_indexes, *root_schema);
	if (ret->Type().id() != LogicalTypeId::STRUCT) {
		throw InternalException("Root element of Parquet file must be a struct");
	}
	// add expressions if required
	auto &root_struct_reader = ret->Cast<StructColumnReader>();
	for (auto &entry : expression_map) {
		auto column_id = entry.first;
		auto &expression = entry.second;
		auto child_reader = std::move(root_struct_reader.child_readers[column_id]);
		auto expr_schema = make_uniq<ParquetColumnSchema>(child_reader->Schema(), expression->return_type,
		                                                  ParquetColumnSchemaType::EXPRESSION);
		auto expr_reader = make_uniq<ExpressionColumnReader>(context, std::move(child_reader), expression->Copy(),
		                                                     std::move(expr_schema));
		root_struct_reader.child_readers[column_id] = std::move(expr_reader);
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

ParquetColumnSchema::ParquetColumnSchema(ParquetColumnSchema child, LogicalType result_type,
                                         ParquetColumnSchemaType schema_type)
    : schema_type(schema_type), name(child.name), type(std::move(result_type)), max_define(child.max_define),
      max_repeat(child.max_repeat), schema_index(child.schema_index), column_index(child.column_index) {
	children.push_back(std::move(child));
}

unique_ptr<BaseStatistics> ParquetColumnSchema::Stats(const FileMetaData &file_meta_data,
                                                      const ParquetOptions &parquet_options, idx_t row_group_idx_p,
                                                      const vector<ColumnChunk> &columns) const {
	if (schema_type == ParquetColumnSchemaType::EXPRESSION) {
		return nullptr;
	}
	if (schema_type == ParquetColumnSchemaType::FILE_ROW_NUMBER) {
		auto stats = NumericStats::CreateUnknown(type);
		auto &row_groups = file_meta_data.row_groups;
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
	return ParquetStatisticsUtils::TransformColumnStatistics(*this, columns, parquet_options.can_have_nan);
}

static bool IsGeometryType(const SchemaElement &s_ele, const ParquetFileMetadataCache &metadata, idx_t depth) {
	const auto is_blob = s_ele.__isset.type && s_ele.type == Type::BYTE_ARRAY;
	if (!is_blob) {
		return false;
	}

	// TODO: Handle CRS in the future
	const auto is_native_geom = s_ele.__isset.logicalType && s_ele.logicalType.__isset.GEOMETRY;
	const auto is_native_geog = s_ele.__isset.logicalType && s_ele.logicalType.__isset.GEOGRAPHY;
	if (is_native_geom || is_native_geog) {
		return true;
	}

	// geoparquet types have to be at the root of the schema, and have to be present in the kv metadata.
	const auto is_at_root = depth == 1;
	const auto is_in_gpq_metadata = metadata.geo_metadata && metadata.geo_metadata->IsGeometryColumn(s_ele.name);
	const auto is_leaf = s_ele.num_children == 0;
	const auto is_geoparquet_geom = is_at_root && is_in_gpq_metadata && is_leaf;

	if (is_geoparquet_geom) {
		return true;
	}

	return false;
}

ParquetColumnSchema ParquetReader::ParseSchemaRecursive(idx_t depth, idx_t max_define, idx_t max_repeat,
                                                        idx_t &next_schema_idx, idx_t &next_file_idx,
                                                        ClientContext &context) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(file_meta_data);
	if (next_schema_idx >= file_meta_data->schema.size()) {
		throw InvalidInputException("Malformed Parquet schema in file \"%s\": invalid schema index %d", file.path,
		                            next_schema_idx);
	}
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

	// Check for geometry type
	if (IsGeometryType(s_ele, *metadata, depth)) {
		// Geometries in both GeoParquet and native parquet are stored as a WKB-encoded BLOB.
		// Because we don't just want to validate that the WKB encoding is correct, but also transform it into
		// little-endian if necessary, we cant just make use of the StringColumnReader without heavily modifying it.
		// Therefore, we create a dedicated GEOMETRY parquet column schema type, which wraps the underlying BLOB column.
		// This schema type gets instantiated as a ExpressionColumnReader on top of the standard Blob/String reader,
		// which performs the WKB validation/transformation using the `ST_GeomFromWKB` function of DuckDB.
		// This enables us to also support other geometry encodings (such as GeoArrow geometries) easier in the future.

		// Inner BLOB schema
		ParquetColumnSchema blob_schema(max_define, max_repeat, this_idx, next_file_idx++,
		                                ParquetColumnSchemaType::COLUMN);
		blob_schema.name = s_ele.name;
		blob_schema.type = LogicalType::BLOB;

		// Wrap in geometry schema
		ParquetColumnSchema geom_schema(std::move(blob_schema), LogicalType::GEOMETRY(),
		                                ParquetColumnSchemaType::GEOMETRY);
		return geom_schema;
	}

	if (s_ele.__isset.num_children && s_ele.num_children > 0) { // inner node
		vector<ParquetColumnSchema> child_schemas;

		idx_t c_idx = 0;
		while (c_idx < NumericCast<idx_t>(s_ele.num_children)) {
			next_schema_idx++;

			auto child_schema =
			    ParseSchemaRecursive(depth + 1, max_define, max_repeat, next_schema_idx, next_file_idx, context);
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
		const bool is_list = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::LIST;
		const bool is_map = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP;
		bool is_map_kv = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP_KEY_VALUE;
		bool is_variant = s_ele.__isset.logicalType && s_ele.logicalType.__isset.VARIANT == true;

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

			LogicalType result_type;
			if (is_variant) {
				result_type = LogicalType::VARIANT();
			} else {
				result_type = LogicalType::STRUCT(std::move(struct_types));
			}
			ParquetColumnSchema struct_schema(s_ele.name, std::move(result_type), max_define, max_repeat, this_idx,
			                                  next_file_idx);
			struct_schema.children = std::move(child_schemas);
			if (is_variant) {
				struct_schema.schema_type = ParquetColumnSchemaType::VARIANT;
			}
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
			    "Node '%s' has neither num_children nor type set - this violates the Parquet spec (corrupted file)",
			    s_ele.name.c_str());
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

static ParquetColumnSchema FileRowNumberSchema() {
	return ParquetColumnSchema("file_row_number", LogicalType::BIGINT, 0, 0, 0, 0,
	                           ParquetColumnSchemaType::FILE_ROW_NUMBER);
}

unique_ptr<ParquetColumnSchema> ParquetReader::ParseSchema(ClientContext &context) {
	auto file_meta_data = GetFileMetadata();
	idx_t next_schema_idx = 0;
	idx_t next_file_idx = 0;

	if (file_meta_data->schema.empty()) {
		throw IOException("Failed to read Parquet file \"%s\": no schema elements found", file.path);
	}
	if (file_meta_data->schema[0].num_children == 0) {
		throw IOException("Failed to read Parquet file \"%s\": root schema element has no children", file.path);
	}
	auto root = ParseSchemaRecursive(0, 0, 0, next_schema_idx, next_file_idx, context);
	if (root.type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("Failed to read Parquet file \"%s\": Root element of Parquet file must be a struct",
		                            file.path);
	}
	D_ASSERT(next_schema_idx == file_meta_data->schema.size() - 1);
	if (!file_meta_data->row_groups.empty() && next_file_idx != file_meta_data->row_groups[0].columns.size()) {
		throw InvalidInputException("Failed to read Parquet file \"%s\": row group does not have enough columns",
		                            file.path);
	}
	if (parquet_options.file_row_number) {
		for (auto &column : root.children) {
			auto &name = column.name;
			if (StringUtil::CIEquals(name, "file_row_number")) {
				throw BinderException("Failed to read Parquet file \"%s\": Using file_row_number option on file with "
				                      "column named file_row_number is not supported",
				                      file.path);
			}
		}
		root.children.push_back(FileRowNumberSchema());
	}
	return make_uniq<ParquetColumnSchema>(root);
}

MultiFileColumnDefinition ParquetReader::ParseColumnDefinition(const FileMetaData &file_meta_data,
                                                               ParquetColumnSchema &element) {
	MultiFileColumnDefinition result(element.name, element.type);
	if (element.schema_type == ParquetColumnSchemaType::FILE_ROW_NUMBER) {
		result.identifier = Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID);
		return result;
	}
	auto &column_schema = file_meta_data.schema[element.schema_index];

	if (column_schema.__isset.field_id) {
		result.identifier = Value::INTEGER(column_schema.field_id);
	} else if (element.parent_schema_index.IsValid()) {
		auto &parent_column_schema = file_meta_data.schema[element.parent_schema_index.GetIndex()];
		if (parent_column_schema.__isset.field_id) {
			result.identifier = Value::INTEGER(parent_column_schema.field_id);
		}
	}
	for (auto &child : element.children) {
		result.children.push_back(ParseColumnDefinition(file_meta_data, child));
	}
	return result;
}

void ParquetReader::InitializeSchema(ClientContext &context) {
	auto file_meta_data = GetFileMetadata();

	if (file_meta_data->__isset.encryption_algorithm) {
		if (file_meta_data->encryption_algorithm.__isset.AES_GCM_CTR_V1) {
			throw InvalidInputException("File '%s' is encrypted with AES_GCM_CTR_V1, but only AES_GCM_V1 is supported",
			                            GetFileName());
		}
	}
	// check if we like this schema
	if (file_meta_data->schema.size() < 2) {
		throw InvalidInputException("Failed to read Parquet file '%s': Need at least one non-root column in the file",
		                            GetFileName());
	}
	root_schema = ParseSchema(context);
	for (idx_t i = 0; i < root_schema->children.size(); i++) {
		auto &element = root_schema->children[i];
		columns.push_back(ParseColumnDefinition(*file_meta_data, element));
	}
}

void ParquetReader::AddVirtualColumn(column_t virtual_column_id) {
	if (virtual_column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		root_schema->children.push_back(FileRowNumberSchema());
	} else {
		throw InternalException("Unsupported virtual column id %d for parquet reader", virtual_column_id);
	}
}

ParquetOptions::ParquetOptions(ClientContext &context) {
	Value lookup_value;
	if (context.TryGetCurrentSetting("binary_as_string", lookup_value)) {
		binary_as_string = lookup_value.GetValue<bool>();
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

ParquetReader::ParquetReader(ClientContext &context_p, OpenFileInfo file_p, ParquetOptions parquet_options_p,
                             shared_ptr<ParquetFileMetadataCache> metadata_p)
    : BaseFileReader(std::move(file_p)), fs(CachingFileSystem::Get(context_p)),
      allocator(BufferAllocator::Get(context_p)), parquet_options(std::move(parquet_options_p)) {
	file_handle = fs.OpenFile(context_p, file, FileFlags::FILE_FLAGS_READ);
	if (!file_handle->CanSeek()) {
		throw NotImplementedException(
		    "Reading parquet files from a FIFO stream is not supported and cannot be efficiently supported since "
		    "metadata is located at the end of the file. Write the stream to disk first and read from there instead.");
	}

	// read the extended file open info (if any)
	optional_idx footer_size;
	if (file.extended_info) {
		auto &open_options = file.extended_info->options;
		auto encryption_entry = file.extended_info->options.find("encryption_key");
		if (encryption_entry != open_options.end()) {
			parquet_options.encryption_config =
			    make_shared_ptr<ParquetEncryptionConfig>(StringValue::Get(encryption_entry->second));
		}
		auto footer_entry = file.extended_info->options.find("footer_size");
		if (footer_entry != open_options.end()) {
			footer_size = UBigIntValue::Get(footer_entry->second);
		}
	}
	// set pointer to factory method for AES state
	auto &config = DBConfig::GetConfig(context_p);
	if (config.encryption_util && parquet_options.debug_use_openssl) {
		encryption_util = config.encryption_util;
	} else {
		encryption_util = make_shared_ptr<duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLSFactory>();
	}
	// If metadata cached is disabled
	// or if this file has cached metadata
	// or if the cached version already expired
	if (!metadata_p) {
		if (!MetadataCacheEnabled(context_p)) {
			metadata = LoadMetadata(context_p, allocator, *file_handle, parquet_options.encryption_config,
			                        *encryption_util, footer_size);
		} else {
			metadata = ObjectCache::GetObjectCache(context_p).Get<ParquetFileMetadataCache>(file.path);
			if (!metadata || !metadata->IsValid(*file_handle)) {
				metadata = LoadMetadata(context_p, allocator, *file_handle, parquet_options.encryption_config,
				                        *encryption_util, footer_size);
				ObjectCache::GetObjectCache(context_p).Put(file.path, metadata);
			}
		}
	} else {
		metadata = std::move(metadata_p);
	}
	InitializeSchema(context_p);
}

bool ParquetReader::MetadataCacheEnabled(ClientContext &context) {
	Value metadata_cache = false;
	context.TryGetCurrentSetting("parquet_metadata_cache", metadata_cache);
	return metadata_cache.GetValue<bool>();
}

shared_ptr<ParquetFileMetadataCache> ParquetReader::GetMetadataCacheEntry(ClientContext &context,
                                                                          const OpenFileInfo &file) {
	return ObjectCache::GetObjectCache(context).Get<ParquetFileMetadataCache>(file.path);
}

ParquetUnionData::~ParquetUnionData() {
}

unique_ptr<BaseStatistics> ParquetUnionData::GetStatistics(ClientContext &context, const string &name) {
	if (reader) {
		return reader->Cast<ParquetReader>().GetStatistics(context, name);
	}
	return ParquetReader::ReadStatistics(*this, name);
}

ParquetReader::ParquetReader(ClientContext &context_p, ParquetOptions parquet_options_p,
                             shared_ptr<ParquetFileMetadataCache> metadata_p)
    : BaseFileReader(string()), fs(CachingFileSystem::Get(context_p)), allocator(BufferAllocator::Get(context_p)),
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

static unique_ptr<BaseStatistics> ReadStatisticsInternal(const FileMetaData &file_meta_data,
                                                         const ParquetColumnSchema &root_schema,
                                                         const ParquetOptions &parquet_options,
                                                         const idx_t &file_col_idx) {
	unique_ptr<BaseStatistics> column_stats;
	auto &column_schema = root_schema.children[file_col_idx];

	for (idx_t row_group_idx = 0; row_group_idx < file_meta_data.row_groups.size(); row_group_idx++) {
		auto &row_group = file_meta_data.row_groups[row_group_idx];
		auto chunk_stats = column_schema.Stats(file_meta_data, parquet_options, row_group_idx, row_group.columns);
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

	return ReadStatisticsInternal(*GetFileMetadata(), *root_schema, parquet_options, file_col_idx);
}

unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(ClientContext &context, ParquetOptions parquet_options,
                                                         shared_ptr<ParquetFileMetadataCache> metadata,
                                                         const string &name) {
	ParquetReader reader(context, std::move(parquet_options), std::move(metadata));
	return reader.ReadStatistics(name);
}

unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(const ParquetUnionData &union_data, const string &name) {
	const auto &col_names = union_data.names;

	idx_t file_col_idx;
	for (file_col_idx = 0; file_col_idx < col_names.size(); file_col_idx++) {
		if (col_names[file_col_idx] == name) {
			break;
		}
	}
	if (file_col_idx == col_names.size()) {
		return nullptr;
	}

	return ReadStatisticsInternal(*union_data.metadata->metadata, *union_data.root_schema, union_data.options,
	                              file_col_idx);
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

static idx_t GetRowGroupOffset(ParquetReader &reader, idx_t group_idx) {
	idx_t row_group_offset = 0;
	auto &row_groups = reader.GetFileMetadata()->row_groups;
	for (idx_t i = 0; i < group_idx; i++) {
		row_group_offset += row_groups[i].num_rows;
	}
	return row_group_offset;
}

const ParquetRowGroup &ParquetReader::GetGroup(ParquetReaderScanState &state) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(state.current_group >= 0 && (idx_t)state.current_group < state.group_idx_list.size());
	D_ASSERT(state.group_idx_list[state.current_group] < file_meta_data->row_groups.size());
	return file_meta_data->row_groups[state.group_idx_list[state.current_group]];
}

uint64_t ParquetReader::GetGroupCompressedSize(ParquetReaderScanState &state) {
	const auto &group = GetGroup(state);
	int64_t total_compressed_size = group.__isset.total_compressed_size ? group.total_compressed_size : 0;

	idx_t calc_compressed_size = 0;

	// If the global total_compressed_size is not set, we can still calculate it
	if (group.total_compressed_size == 0) {
		for (auto &column_chunk : group.columns) {
			calc_compressed_size += column_chunk.meta_data.total_compressed_size;
		}
	}

	if (total_compressed_size != 0 && calc_compressed_size != 0 &&
	    (idx_t)total_compressed_size != calc_compressed_size) {
		throw InvalidInputException(
		    "Failed to read file \"%s\": mismatch between calculated compressed size and reported compressed size",
		    GetFileName());
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
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = filter.Cast<ConjunctionAndFilter>();
		auto and_result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
		for (auto &child_filter : conjunction_filter.child_filters) {
			auto child_prune_result = CheckParquetStringFilter(stats, pq_col_stats, *child_filter);
			if (child_prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				return FilterPropagateResult::FILTER_ALWAYS_FALSE;
			}
			if (child_prune_result != and_result) {
				and_result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
		}
		return and_result;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto &min_value = pq_col_stats.min_value;
		auto &max_value = pq_col_stats.max_value;
		return StringStats::CheckZonemap(const_data_ptr_cast(min_value.c_str()), min_value.size(),
		                                 const_data_ptr_cast(max_value.c_str()), max_value.size(),
		                                 constant_filter.comparison_type, StringValue::Get(constant_filter.constant));
	}
	default:
		return filter.CheckStatistics(stats);
	}
}

static FilterPropagateResult CheckParquetFloatFilter(ColumnReader &reader, const Statistics &pq_col_stats,
                                                     TableFilter &filter) {
	// floating point values can have values in the [min, max] domain AND nan values
	// check both stats against the filter
	auto &type = reader.Type();
	auto nan_stats = NumericStats::CreateUnknown(type);
	auto nan_value = Value("nan").DefaultCastAs(type);
	NumericStats::SetMin(nan_stats, nan_value);
	NumericStats::SetMax(nan_stats, nan_value);
	auto nan_prune = filter.CheckStatistics(nan_stats);

	auto min_max_stats = ParquetStatisticsUtils::CreateNumericStats(reader.Type(), reader.Schema(), pq_col_stats);
	auto prune = filter.CheckStatistics(*min_max_stats);

	// if EITHER of them cannot be pruned - we cannot prune
	if (prune == FilterPropagateResult::NO_PRUNING_POSSIBLE ||
	    nan_prune == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	// if both are the same we can return that value
	if (prune == nan_prune) {
		return prune;
	}
	// if they are different we need to return that we cannot prune
	// e.g. prune = always false, nan_prune = always true -> we don't know
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

void ParquetReader::PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t i) {
	auto &group = GetGroup(state);
	auto col_idx = MultiFileLocalIndex(i);
	auto column_id = column_ids[col_idx];
	auto &column_reader = state.root_reader->Cast<StructColumnReader>().GetChildReader(column_id);

	if (filters) {
		auto stats = column_reader.Stats(state.group_idx_list[state.current_group], group.columns);
		// filters contain output chunk index, not file col idx!
		auto filter_entry = filters->filters.find(col_idx);
		if (stats && filter_entry != filters->filters.end()) {
			auto &filter = *filter_entry->second;

			FilterPropagateResult prune_result;
			bool is_generated_column = column_reader.ColumnIndex() >= group.columns.size();
			bool is_column = column_reader.Schema().schema_type == ParquetColumnSchemaType::COLUMN;
			bool is_expression = column_reader.Schema().schema_type == ParquetColumnSchemaType::EXPRESSION;
			bool has_min_max = false;
			if (!is_generated_column) {
				has_min_max = group.columns[column_reader.ColumnIndex()].meta_data.statistics.__isset.min_value &&
				              group.columns[column_reader.ColumnIndex()].meta_data.statistics.__isset.max_value;
			}
			if (is_expression) {
				// no pruning possible for expressions
				prune_result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
			} else if (!is_generated_column && has_min_max && column_reader.Type().id() == LogicalTypeId::VARCHAR) {
				// our StringStats only store the first 8 bytes of strings (even if Parquet has longer string stats)
				// however, when reading remote Parquet files, skipping row groups is really important
				// here, we implement a special case to check the full length for string filters
				prune_result = CheckParquetStringFilter(
				    *stats, group.columns[column_reader.ColumnIndex()].meta_data.statistics, filter);
			} else if (!is_generated_column && has_min_max &&
			           (column_reader.Type().id() == LogicalTypeId::FLOAT ||
			            column_reader.Type().id() == LogicalTypeId::DOUBLE) &&
			           parquet_options.can_have_nan) {
				// floating point columns can have NaN values in addition to the min/max bounds defined in the file
				// in order to do optimal pruning - we prune based on the [min, max] of the file followed by pruning
				// based on nan
				prune_result = CheckParquetFloatFilter(
				    column_reader, group.columns[column_reader.ColumnIndex()].meta_data.statistics, filter);
			} else {
				prune_result = filter.CheckStatistics(*stats);
			}
			// check the bloom filter if present
			if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE && !column_reader.Type().IsNested() &&
			    is_column && ParquetStatisticsUtils::BloomFilterSupported(column_reader.Type().id()) &&
			    ParquetStatisticsUtils::BloomFilterExcludes(filter,
			                                                group.columns[column_reader.ColumnIndex()].meta_data,
			                                                *state.thrift_file_proto, allocator)) {
				prune_result = FilterPropagateResult::FILTER_ALWAYS_FALSE;
			}

			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				// this effectively will skip this chunk
				state.offset_in_group = group.num_rows;
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
	filter_state = TableFilterState::Initialize(context, filter);
}

ParquetScanFilter::~ParquetScanFilter() {
}

void ParquetReader::InitializeScan(ClientContext &context, ParquetReaderScanState &state,
                                   vector<idx_t> groups_to_read) {
	state.current_group = -1;
	state.finished = false;
	state.offset_in_group = 0;
	state.group_idx_list = std::move(groups_to_read);
	state.sel.Initialize(STANDARD_VECTOR_SIZE);
	if (!state.file_handle || state.file_handle->GetPath() != file_handle->GetPath()) {
		auto flags = FileFlags::FILE_FLAGS_READ;
		if (ShouldAndCanPrefetch(context, *file_handle)) {
			state.prefetch_mode = true;
			if (file_handle->IsRemoteFile()) {
				flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
			}
		} else {
			state.prefetch_mode = false;
		}

		state.file_handle = fs.OpenFile(context, file, flags);
	}
	state.adaptive_filter.reset();
	state.scan_filters.clear();
	if (filters) {
		state.adaptive_filter = make_uniq<AdaptiveFilter>(*filters);
		for (auto &entry : filters->filters) {
			state.scan_filters.emplace_back(context, entry.first, *entry.second);
		}
	}

	state.thrift_file_proto = CreateThriftFileProtocol(context, *state.file_handle, state.prefetch_mode);
	state.root_reader = CreateReader(context);
	state.define_buf.resize(allocator, STANDARD_VECTOR_SIZE);
	state.repeat_buf.resize(allocator, STANDARD_VECTOR_SIZE);
}

void ParquetReader::GetPartitionStats(vector<PartitionStatistics> &result) {
	GetPartitionStats(*GetFileMetadata(), result);
}

void ParquetReader::GetPartitionStats(const duckdb_parquet::FileMetaData &metadata,
                                      vector<PartitionStatistics> &result) {
	idx_t offset = 0;
	for (auto &row_group : metadata.row_groups) {
		PartitionStatistics partition_stats;
		partition_stats.row_start = offset;
		partition_stats.count = row_group.num_rows;
		partition_stats.count_type = CountType::COUNT_EXACT;
		offset += row_group.num_rows;
		result.push_back(partition_stats);
	}
}

AsyncResult ParquetReader::Scan(ClientContext &context, ParquetReaderScanState &state, DataChunk &result) {
	result.Reset();
	if (state.finished) {
		return SourceResultType::FINISHED;
	}

	// see if we have to switch to the next row group in the parquet file
	if (state.current_group < 0 || (int64_t)state.offset_in_group >= GetGroup(state).num_rows) {
		state.current_group++;
		state.offset_in_group = 0;

		auto &trans = reinterpret_cast<ThriftFileTransport &>(*state.thrift_file_proto->getTransport());
		trans.ClearPrefetch();
		state.current_group_prefetched = false;

		if ((idx_t)state.current_group == state.group_idx_list.size()) {
			state.finished = true;
			return SourceResultType::FINISHED;
		}

		// TODO: only need this if we have a deletion vector?
		state.group_offset = GetRowGroupOffset(state.root_reader->Reader(), state.group_idx_list[state.current_group]);

		uint64_t to_scan_compressed_bytes = 0;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = MultiFileLocalIndex(i);
			PrepareRowGroupBuffer(state, col_idx);

			auto file_col_idx = column_ids[col_idx];

			auto &root_reader = state.root_reader->Cast<StructColumnReader>();
			to_scan_compressed_bytes += root_reader.GetChildReader(file_col_idx).TotalCompressedSize();
		}

		auto &group = GetGroup(state);
		if (state.op) {
			DUCKDB_LOG(context, PhysicalOperatorLogType, *state.op, "ParquetReader",
			           state.offset_in_group == (idx_t)group.num_rows ? "SkipRowGroup" : "ReadRowGroup",
			           {{"file", file.path}, {"row_group_id", to_string(state.group_idx_list[state.current_group])}});
		}

		if (state.prefetch_mode && state.offset_in_group != (idx_t)group.num_rows) {
			uint64_t total_row_group_span = GetGroupSpan(state);

			double scan_percentage = (double)(to_scan_compressed_bytes) / static_cast<double>(total_row_group_span);

			if (to_scan_compressed_bytes > total_row_group_span) {
				throw IOException(
				    "The parquet file '%s' seems to have incorrectly set page offsets. This interferes with DuckDB's "
				    "prefetching optimization. DuckDB may still be able to scan this file by manually disabling the "
				    "prefetching mechanism using: 'SET disable_parquet_prefetching=true'.",
				    GetFileName());
			}

			if (!filters && scan_percentage > ParquetReaderPrefetchConfig::WHOLE_GROUP_PREFETCH_MINIMUM_SCAN) {
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
				bool lazy_fetch = filters != nullptr;

				// Prefetch column-wise
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto col_idx = MultiFileLocalIndex(i);
					auto file_col_idx = column_ids[col_idx];
					auto &root_reader = state.root_reader->Cast<StructColumnReader>();

					bool has_filter = false;
					if (filters) {
						auto entry = filters->filters.find(col_idx);
						has_filter = entry != filters->filters.end();
					}
					root_reader.GetChildReader(file_col_idx).RegisterPrefetch(trans, !(lazy_fetch && !has_filter));
				}

				trans.FinalizeRegistration();

				if (!lazy_fetch) {
					trans.PrefetchRegistered();
				}
			}
		}
		result.Reset();
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	auto scan_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, GetGroup(state).num_rows - state.offset_in_group);
	result.SetCardinality(scan_count);

	if (scan_count == 0) {
		state.finished = true;
		// end of last group, we are done
		return SourceResultType::FINISHED;
	}

	auto &deletion_filter = state.root_reader->Reader().deletion_filter;

	state.define_buf.zero();
	state.repeat_buf.zero();

	auto define_ptr = (uint8_t *)state.define_buf.ptr;
	auto repeat_ptr = (uint8_t *)state.repeat_buf.ptr;

	auto &root_reader = state.root_reader->Cast<StructColumnReader>();

	if (filters || deletion_filter) {
		idx_t filter_count = result.size();
		D_ASSERT(filter_count == scan_count);
		vector<bool> need_to_read(column_ids.size(), true);

		state.sel.Initialize(nullptr);
		D_ASSERT(!filters || state.scan_filters.size() == filters->filters.size());

		bool is_first_filter = true;
		if (deletion_filter) {
			auto row_start = UnsafeNumericCast<row_t>(state.offset_in_group + state.group_offset);
			filter_count = deletion_filter->Filter(row_start, scan_count, state.sel);
			//! FIXME: does this need to be set?
			//! As part of 'DirectFilter' we also initialize reads of the child readers
			is_first_filter = false;
		}

		if (filters) {
			// first load the columns that are used in filters
			auto filter_state = state.adaptive_filter->BeginFilter();
			for (idx_t i = 0; i < state.scan_filters.size(); i++) {
				if (filter_count == 0) {
					// if no rows are left we can stop checking filters
					break;
				}
				auto &scan_filter = state.scan_filters[state.adaptive_filter->permutation[i]];
				auto local_idx = MultiFileLocalIndex(scan_filter.filter_idx);
				auto column_id = column_ids[local_idx];

				auto &result_vector = result.data[local_idx.GetIndex()];
				auto &child_reader = root_reader.GetChildReader(column_id);
				child_reader.Filter(scan_count, define_ptr, repeat_ptr, result_vector, scan_filter.filter,
				                    *scan_filter.filter_state, state.sel, filter_count, is_first_filter);
				need_to_read[local_idx.GetIndex()] = false;
				is_first_filter = false;
			}
			state.adaptive_filter->EndFilter(filter_state);
		}

		// we still may have to read some cols
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = MultiFileLocalIndex(i);
			if (!need_to_read[col_idx]) {
				continue;
			}
			auto file_col_idx = column_ids[col_idx];
			if (filter_count == 0) {
				root_reader.GetChildReader(file_col_idx).Skip(result.size());
				continue;
			}
			auto &result_vector = result.data[i];
			auto &child_reader = root_reader.GetChildReader(file_col_idx);
			child_reader.Select(result.size(), define_ptr, repeat_ptr, result_vector, state.sel, filter_count);
		}
		if (scan_count != filter_count) {
			result.Slice(state.sel, filter_count);
		}
	} else {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = MultiFileLocalIndex(i);
			auto file_col_idx = column_ids[col_idx];
			auto &result_vector = result.data[i];
			auto &child_reader = root_reader.GetChildReader(file_col_idx);
			auto rows_read = child_reader.Read(scan_count, define_ptr, repeat_ptr, result_vector);
			if (rows_read != scan_count) {
				throw InvalidInputException("Mismatch in parquet read for column %llu, expected %llu rows, got %llu",
				                            file_col_idx, scan_count, rows_read);
			}
		}
	}

	rows_read += scan_count;
	state.offset_in_group += scan_count;
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
