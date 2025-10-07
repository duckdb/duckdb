#include "column_writer.hpp"

#include "duckdb.hpp"
#include "geo_parquet.hpp"
#include "parquet_rle_bp_decoder.hpp"
#include "parquet_bss_encoder.hpp"
#include "parquet_statistics.hpp"
#include "parquet_writer.hpp"
#include "writer/array_column_writer.hpp"
#include "writer/boolean_column_writer.hpp"
#include "writer/decimal_column_writer.hpp"
#include "writer/enum_column_writer.hpp"
#include "writer/list_column_writer.hpp"
#include "writer/primitive_column_writer.hpp"
#include "writer/struct_column_writer.hpp"
#include "writer/variant_column_writer.hpp"
#include "writer/templated_column_writer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "brotli/encode.h"
#include "lz4.hpp"
#include "miniz_wrapper.hpp"
#include "snappy.h"
#include "zstd.h"

#include <cmath>

namespace duckdb {

using namespace duckdb_parquet; // NOLINT
using namespace duckdb_miniz;   // NOLINT

using duckdb_parquet::CompressionCodec;
using duckdb_parquet::ConvertedType;
using duckdb_parquet::Encoding;
using duckdb_parquet::FieldRepetitionType;
using duckdb_parquet::FileMetaData;
using duckdb_parquet::PageHeader;
using duckdb_parquet::PageType;
using ParquetRowGroup = duckdb_parquet::RowGroup;
using duckdb_parquet::Type;

constexpr uint16_t ColumnWriter::PARQUET_DEFINE_VALID;

//===--------------------------------------------------------------------===//
// ColumnWriterStatistics
//===--------------------------------------------------------------------===//
ColumnWriterStatistics::~ColumnWriterStatistics() {
}

bool ColumnWriterStatistics::HasStats() {
	return false;
}

string ColumnWriterStatistics::GetMin() {
	return string();
}

string ColumnWriterStatistics::GetMax() {
	return string();
}

string ColumnWriterStatistics::GetMinValue() {
	return string();
}

string ColumnWriterStatistics::GetMaxValue() {
	return string();
}

bool ColumnWriterStatistics::CanHaveNaN() {
	return false;
}

bool ColumnWriterStatistics::HasNaN() {
	return false;
}

bool ColumnWriterStatistics::MinIsExact() {
	return true;
}

bool ColumnWriterStatistics::MaxIsExact() {
	return true;
}

bool ColumnWriterStatistics::HasGeoStats() {
	return false;
}

optional_ptr<GeometryStats> ColumnWriterStatistics::GetGeoStats() {
	return nullptr;
}

void ColumnWriterStatistics::WriteGeoStats(duckdb_parquet::GeospatialStatistics &stats) {
	D_ASSERT(false); // this should never be called
}

//===--------------------------------------------------------------------===//
// ColumnWriter
//===--------------------------------------------------------------------===//
ColumnWriter::ColumnWriter(ParquetWriter &writer, ParquetColumnSchema &&column_schema_p, vector<string> schema_path_p)
    : writer(writer), column_schema(std::move(column_schema_p)), schema_path(std::move(schema_path_p)) {
	can_have_nulls = column_schema.repetition_type == duckdb_parquet::FieldRepetitionType::OPTIONAL;
}
ColumnWriter::~ColumnWriter() {
}

ColumnWriterState::~ColumnWriterState() {
}

void ColumnWriter::CompressPage(MemoryStream &temp_writer, size_t &compressed_size, data_ptr_t &compressed_data,
                                AllocatedData &compressed_buf) {
	switch (writer.GetCodec()) {
	case CompressionCodec::UNCOMPRESSED:
		compressed_size = temp_writer.GetPosition();
		compressed_data = temp_writer.GetData();
		break;

	case CompressionCodec::SNAPPY: {
		compressed_size = duckdb_snappy::MaxCompressedLength(temp_writer.GetPosition());
		compressed_buf = BufferAllocator::Get(writer.GetContext()).Allocate(compressed_size);
		duckdb_snappy::RawCompress(const_char_ptr_cast(temp_writer.GetData()), temp_writer.GetPosition(),
		                           char_ptr_cast(compressed_buf.get()), &compressed_size);
		compressed_data = compressed_buf.get();
		D_ASSERT(compressed_size <= duckdb_snappy::MaxCompressedLength(temp_writer.GetPosition()));
		break;
	}
	case CompressionCodec::LZ4_RAW: {
		compressed_size = duckdb_lz4::LZ4_compressBound(UnsafeNumericCast<int32_t>(temp_writer.GetPosition()));
		compressed_buf = BufferAllocator::Get(writer.GetContext()).Allocate(compressed_size);
		compressed_size = duckdb_lz4::LZ4_compress_default(
		    const_char_ptr_cast(temp_writer.GetData()), char_ptr_cast(compressed_buf.get()),
		    UnsafeNumericCast<int32_t>(temp_writer.GetPosition()), UnsafeNumericCast<int32_t>(compressed_size));
		compressed_data = compressed_buf.get();
		break;
	}
	case CompressionCodec::GZIP: {
		MiniZStream s;
		compressed_size = s.MaxCompressedLength(temp_writer.GetPosition());
		compressed_buf = BufferAllocator::Get(writer.GetContext()).Allocate(compressed_size);
		s.Compress(const_char_ptr_cast(temp_writer.GetData()), temp_writer.GetPosition(),
		           char_ptr_cast(compressed_buf.get()), &compressed_size);
		compressed_data = compressed_buf.get();
		break;
	}
	case CompressionCodec::ZSTD: {
		compressed_size = duckdb_zstd::ZSTD_compressBound(temp_writer.GetPosition());
		compressed_buf = BufferAllocator::Get(writer.GetContext()).Allocate(compressed_size);
		compressed_size = duckdb_zstd::ZSTD_compress((void *)compressed_buf.get(), compressed_size,
		                                             (const void *)temp_writer.GetData(), temp_writer.GetPosition(),
		                                             UnsafeNumericCast<int32_t>(writer.CompressionLevel()));
		compressed_data = compressed_buf.get();
		break;
	}
	case CompressionCodec::BROTLI: {
		compressed_size = duckdb_brotli::BrotliEncoderMaxCompressedSize(temp_writer.GetPosition());
		compressed_buf = BufferAllocator::Get(writer.GetContext()).Allocate(compressed_size);
		duckdb_brotli::BrotliEncoderCompress(BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE,
		                                     temp_writer.GetPosition(), temp_writer.GetData(), &compressed_size,
		                                     compressed_buf.get());
		compressed_data = compressed_buf.get();
		break;
	}
	default:
		throw InternalException("Unsupported codec for Parquet Writer");
	}

	if (compressed_size > idx_t(NumericLimits<int32_t>::Maximum())) {
		throw InternalException("Parquet writer: %d compressed page size out of range for type integer",
		                        temp_writer.GetPosition());
	}
}

void ColumnWriter::HandleRepeatLevels(ColumnWriterState &state, ColumnWriterState *parent, idx_t count) const {
	if (!parent) {
		// no repeat levels without a parent node
		return;
	}
	if (state.repetition_levels.size() >= parent->repetition_levels.size()) {
		return;
	}
	state.repetition_levels.insert(state.repetition_levels.end(),
	                               parent->repetition_levels.begin() + state.repetition_levels.size(),
	                               parent->repetition_levels.end());
}

void ColumnWriter::HandleDefineLevels(ColumnWriterState &state, ColumnWriterState *parent, const ValidityMask &validity,
                                      const idx_t count, const uint16_t define_value, const uint16_t null_value) const {
	if (parent) {
		// parent node: inherit definition level from the parent
		idx_t vector_index = 0;
		while (state.definition_levels.size() < parent->definition_levels.size()) {
			idx_t current_index = state.definition_levels.size();
			if (parent->definition_levels[current_index] != PARQUET_DEFINE_VALID) {
				//! Inherit nulls from parent
				state.definition_levels.push_back(parent->definition_levels[current_index]);
				state.parent_null_count++;
			} else if (validity.RowIsValid(vector_index)) {
				//! Produce a non-null define
				state.definition_levels.push_back(define_value);
			} else {
				//! Produce a null define
				if (!can_have_nulls) {
					throw IOException("Parquet writer: map key column is not allowed to contain NULL values");
				}
				state.null_count++;
				state.definition_levels.push_back(null_value);
			}
			D_ASSERT(parent->is_empty.empty() || current_index < parent->is_empty.size());
			if (parent->is_empty.empty() || !parent->is_empty[current_index]) {
				vector_index++;
			}
		}
		return;
	}

	// no parent: set definition levels only from this validity mask
	if (validity.AllValid()) {
		state.definition_levels.insert(state.definition_levels.end(), count, define_value);
	} else {
		for (idx_t i = 0; i < count; i++) {
			const auto is_null = !validity.RowIsValid(i);
			state.definition_levels.emplace_back(is_null ? null_value : define_value);
			state.null_count += is_null;
		}
	}
	if (!can_have_nulls && state.null_count != 0) {
		throw IOException("Parquet writer: map key column is not allowed to contain NULL values");
	}
}

//===--------------------------------------------------------------------===//
// Create Column Writer
//===--------------------------------------------------------------------===//

unique_ptr<ColumnWriter> ColumnWriter::CreateWriterRecursive(ClientContext &context, ParquetWriter &writer,
                                                             vector<string> path_in_schema, const LogicalType &type,
                                                             const string &name,
                                                             optional_ptr<const ChildFieldIDs> field_ids,
                                                             optional_ptr<const ShreddingType> shredding_types,
                                                             idx_t max_repeat, idx_t max_define, bool can_have_nulls) {
	path_in_schema.push_back(name);

	if (!can_have_nulls) {
		max_define--;
	}
	auto null_type = can_have_nulls ? FieldRepetitionType::OPTIONAL : FieldRepetitionType::REQUIRED;

	optional_ptr<const FieldID> field_id;
	optional_ptr<const ChildFieldIDs> child_field_ids;
	optional_ptr<const ShreddingType> shredding_type;
	if (field_ids) {
		auto field_id_it = field_ids->ids->find(name);
		if (field_id_it != field_ids->ids->end()) {
			field_id = &field_id_it->second;
			child_field_ids = &field_id->child_field_ids;
		}
	}
	if (shredding_types) {
		shredding_type = shredding_types->GetChild(name);
	}

	if (type.id() == LogicalTypeId::VARIANT) {
		const bool is_shredded = shredding_type != nullptr;

		//! Build the child types for the Parquet VARIANT
		child_list_t<LogicalType> child_types;
		child_types.emplace_back("metadata", LogicalType::BLOB);
		child_types.emplace_back("value", LogicalType::BLOB);
		if (is_shredded) {
			auto &typed_value_type = shredding_type->type;
			if (typed_value_type.id() != LogicalTypeId::ANY) {
				child_types.emplace_back("typed_value",
				                         VariantColumnWriter::TransformTypedValueRecursive(typed_value_type));
			}
		}

		//! Construct the column schema
		auto variant_column = ParquetColumnSchema::FromLogicalType(name, type, max_define, max_repeat, 0, null_type);
		vector<unique_ptr<ColumnWriter>> child_writers;
		child_writers.reserve(child_types.size());

		//! Then construct the child writers for the Parquet VARIANT
		for (auto &entry : child_types) {
			auto &child_name = entry.first;
			auto &child_type = entry.second;
			bool is_optional;
			if (child_name == "metadata") {
				is_optional = false;
			} else if (child_name == "value") {
				if (is_shredded) {
					//! When shredding the variant, the 'value' becomes optional
					is_optional = true;
				} else {
					is_optional = false;
				}
			} else {
				D_ASSERT(child_name == "typed_value");
				is_optional = true;
			}

			child_writers.push_back(CreateWriterRecursive(context, writer, path_in_schema, child_type, child_name,
			                                              child_field_ids, shredding_type, max_repeat, max_define + 1,
			                                              is_optional));
		}
		return make_uniq<VariantColumnWriter>(writer, std::move(variant_column), path_in_schema,
		                                      std::move(child_writers));
	}

	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION) {
		auto struct_column = ParquetColumnSchema::FromLogicalType(name, type, max_define, max_repeat, 0, null_type);
		if (field_id && field_id->set) {
			struct_column.field_id = field_id->field_id;
		}

		// construct the child schemas recursively
		auto &child_types = StructType::GetChildTypes(type);
		vector<unique_ptr<ColumnWriter>> child_writers;
		child_writers.reserve(child_types.size());
		for (auto &entry : child_types) {
			auto &child_type = entry.second;
			auto &child_name = entry.first;
			child_writers.push_back(CreateWriterRecursive(context, writer, path_in_schema, child_type, child_name,
			                                              child_field_ids, shredding_type, max_repeat, max_define + 1,
			                                              true));
		}
		return make_uniq<StructColumnWriter>(writer, std::move(struct_column), std::move(path_in_schema),
		                                     std::move(child_writers));
	}

	if (type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::ARRAY) {
		auto is_list = type.id() == LogicalTypeId::LIST;
		auto &child_type = is_list ? ListType::GetChildType(type) : ArrayType::GetChildType(type);

		path_in_schema.push_back("list");
		auto child_writer =
		    CreateWriterRecursive(context, writer, path_in_schema, child_type, "element", child_field_ids,
		                          shredding_type, max_repeat + 1, max_define + 2, true);

		auto list_column = ParquetColumnSchema::FromLogicalType(name, type, max_define, max_repeat, 0, null_type);
		if (is_list) {
			return make_uniq<ListColumnWriter>(writer, std::move(list_column), std::move(path_in_schema),
			                                   std::move(child_writer));
		} else {
			return make_uniq<ArrayColumnWriter>(writer, std::move(list_column), std::move(path_in_schema),
			                                    std::move(child_writer));
		}
	}

	if (type.id() == LogicalTypeId::MAP) {
		path_in_schema.push_back("key_value");

		// construct the child types recursively
		child_list_t<LogicalType> key_value;
		key_value.reserve(2);
		key_value.emplace_back("key", MapType::KeyType(type));
		key_value.emplace_back("value", MapType::ValueType(type));
		auto key_value_type = LogicalType::STRUCT(key_value);

		auto map_column = ParquetColumnSchema::FromLogicalType(name, type, max_define, max_repeat, 0, null_type);
		vector<unique_ptr<ColumnWriter>> child_writers;
		child_writers.reserve(2);
		for (idx_t i = 0; i < 2; i++) {
			// key needs to be marked as REQUIRED
			bool is_key = i == 0;
			auto &child_name = key_value[i].first;
			auto &child_type = key_value[i].second;
			auto child_writer =
			    CreateWriterRecursive(context, writer, path_in_schema, child_type, child_name, child_field_ids,
			                          shredding_type, max_repeat + 1, max_define + 2, !is_key);

			child_writers.push_back(std::move(child_writer));
		}

		auto key_value_schema = ParquetColumnSchema::FromLogicalType("key_value", key_value_type, max_define + 1,
		                                                             max_repeat + 1, 0, FieldRepetitionType::REPEATED);
		auto struct_writer = make_uniq<StructColumnWriter>(writer, std::move(key_value_schema), path_in_schema,
		                                                   std::move(child_writers));
		return make_uniq<ListColumnWriter>(writer, std::move(map_column), path_in_schema, std::move(struct_writer));
	}

	auto schema = ParquetColumnSchema::FromLogicalType(name, type, max_define, max_repeat, 0, null_type);
	if (field_id && field_id->set) {
		schema.field_id = field_id->field_id;
	}

	if (type.id() == LogicalTypeId::BLOB && type.GetAlias() == "WKB_BLOB") {
		return make_uniq<StandardColumnWriter<string_t, string_t, ParquetGeometryOperator>>(writer, std::move(schema),
		                                                                                    std::move(path_in_schema));
	}

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_uniq<BooleanColumnWriter>(writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::TINYINT:
		return make_uniq<StandardColumnWriter<int8_t, int32_t>>(writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::SMALLINT:
		return make_uniq<StandardColumnWriter<int16_t, int32_t>>(writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return make_uniq<StandardColumnWriter<int32_t, int32_t>>(writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
		return make_uniq<StandardColumnWriter<int64_t, int64_t>>(writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::TIME_TZ:
		return make_uniq<StandardColumnWriter<dtime_tz_t, int64_t, ParquetTimeTZOperator>>(writer, std::move(schema),
		                                                                                   std::move(path_in_schema));
	case LogicalTypeId::HUGEINT:
		return make_uniq<StandardColumnWriter<hugeint_t, double, ParquetHugeintOperator>>(writer, std::move(schema),
		                                                                                  std::move(path_in_schema));
	case LogicalTypeId::UHUGEINT:
		return make_uniq<StandardColumnWriter<uhugeint_t, double, ParquetUhugeintOperator>>(writer, std::move(schema),
		                                                                                    std::move(path_in_schema));
	case LogicalTypeId::TIMESTAMP_NS:
		return make_uniq<StandardColumnWriter<int64_t, int64_t, ParquetTimestampNSOperator>>(writer, std::move(schema),
		                                                                                     std::move(path_in_schema));
	case LogicalTypeId::TIMESTAMP_SEC:
		return make_uniq<StandardColumnWriter<int64_t, int64_t, ParquetTimestampSOperator>>(writer, std::move(schema),
		                                                                                    std::move(path_in_schema));
	case LogicalTypeId::UTINYINT:
		return make_uniq<StandardColumnWriter<uint8_t, int32_t>>(writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::USMALLINT:
		return make_uniq<StandardColumnWriter<uint16_t, int32_t>>(writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::UINTEGER:
		return make_uniq<StandardColumnWriter<uint32_t, uint32_t>>(writer, std::move(schema),
		                                                           std::move(path_in_schema));
	case LogicalTypeId::UBIGINT:
		return make_uniq<StandardColumnWriter<uint64_t, uint64_t>>(writer, std::move(schema),
		                                                           std::move(path_in_schema));
	case LogicalTypeId::FLOAT:
		return make_uniq<StandardColumnWriter<float_na_equal, float, FloatingPointOperator>>(writer, std::move(schema),
		                                                                                     std::move(path_in_schema));
	case LogicalTypeId::DOUBLE:
		return make_uniq<StandardColumnWriter<double_na_equal, double, FloatingPointOperator>>(
		    writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return make_uniq<StandardColumnWriter<int16_t, int32_t>>(writer, std::move(schema),
			                                                         std::move(path_in_schema));
		case PhysicalType::INT32:
			return make_uniq<StandardColumnWriter<int32_t, int32_t>>(writer, std::move(schema),
			                                                         std::move(path_in_schema));
		case PhysicalType::INT64:
			return make_uniq<StandardColumnWriter<int64_t, int64_t>>(writer, std::move(schema),
			                                                         std::move(path_in_schema));
		default:
			return make_uniq<FixedDecimalColumnWriter>(writer, std::move(schema), std::move(path_in_schema));
		}
	case LogicalTypeId::BLOB:
		return make_uniq<StandardColumnWriter<string_t, string_t, ParquetBlobOperator>>(writer, std::move(schema),
		                                                                                std::move(path_in_schema));
	case LogicalTypeId::VARCHAR:
		return make_uniq<StandardColumnWriter<string_t, string_t, ParquetStringOperator>>(writer, std::move(schema),
		                                                                                  std::move(path_in_schema));
	case LogicalTypeId::UUID:
		return make_uniq<StandardColumnWriter<hugeint_t, ParquetUUIDTargetType, ParquetUUIDOperator>>(
		    writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::INTERVAL:
		return make_uniq<StandardColumnWriter<interval_t, ParquetIntervalTargetType, ParquetIntervalOperator>>(
		    writer, std::move(schema), std::move(path_in_schema));
	case LogicalTypeId::ENUM:
		return make_uniq<EnumColumnWriter>(writer, std::move(schema), std::move(path_in_schema));
	default:
		throw InternalException("Unsupported type \"%s\" in Parquet writer", type.ToString());
	}
}

template <>
struct NumericLimits<float_na_equal> {
	static constexpr float Minimum() {
		return std::numeric_limits<float>::lowest();
	};
	static constexpr float Maximum() {
		return std::numeric_limits<float>::max();
	};
	static constexpr bool IsSigned() {
		return std::is_signed<float>::value;
	}
	static constexpr bool IsIntegral() {
		return std::is_integral<float>::value;
	}
};

template <>
struct NumericLimits<double_na_equal> {
	static constexpr double Minimum() {
		return std::numeric_limits<double>::lowest();
	};
	static constexpr double Maximum() {
		return std::numeric_limits<double>::max();
	};
	static constexpr bool IsSigned() {
		return std::is_signed<double>::value;
	}
	static constexpr bool IsIntegral() {
		return std::is_integral<double>::value;
	}
};

template <>
hash_t Hash(ParquetIntervalTargetType val) {
	return Hash(const_char_ptr_cast(val.bytes), ParquetIntervalTargetType::PARQUET_INTERVAL_SIZE);
}

template <>
hash_t Hash(ParquetUUIDTargetType val) {
	return Hash(const_char_ptr_cast(val.bytes), ParquetUUIDTargetType::PARQUET_UUID_SIZE);
}

template <>
hash_t Hash(float_na_equal val) {
	if (std::isnan(val.val)) {
		return Hash<float>(std::numeric_limits<float>::quiet_NaN());
	}
	return Hash<float>(val.val);
}

template <>
hash_t Hash(double_na_equal val) {
	if (std::isnan(val.val)) {
		return Hash<double>(std::numeric_limits<double>::quiet_NaN());
	}
	return Hash<double>(val.val);
}

} // namespace duckdb
