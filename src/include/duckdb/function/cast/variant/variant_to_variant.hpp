#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

static bool VariantIsTrivialPrimitive(VariantLogicalType type) {
	switch (type) {
	case VariantLogicalType::INT8:
	case VariantLogicalType::INT16:
	case VariantLogicalType::INT32:
	case VariantLogicalType::INT64:
	case VariantLogicalType::INT128:
	case VariantLogicalType::UINT8:
	case VariantLogicalType::UINT16:
	case VariantLogicalType::UINT32:
	case VariantLogicalType::UINT64:
	case VariantLogicalType::UINT128:
	case VariantLogicalType::FLOAT:
	case VariantLogicalType::DOUBLE:
	case VariantLogicalType::UUID:
	case VariantLogicalType::DATE:
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIME_NANOS:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
	case VariantLogicalType::TIME_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
	case VariantLogicalType::INTERVAL:
		return true;
	default:
		return false;
	}
}

static uint32_t VariantTrivialPrimitiveSize(VariantLogicalType type) {
	switch (type) {
	case VariantLogicalType::INT8:
		return sizeof(int8_t);
	case VariantLogicalType::INT16:
		return sizeof(int16_t);
	case VariantLogicalType::INT32:
		return sizeof(int32_t);
	case VariantLogicalType::INT64:
		return sizeof(int64_t);
	case VariantLogicalType::INT128:
		return sizeof(hugeint_t);
	case VariantLogicalType::UINT8:
		return sizeof(uint8_t);
	case VariantLogicalType::UINT16:
		return sizeof(uint16_t);
	case VariantLogicalType::UINT32:
		return sizeof(uint32_t);
	case VariantLogicalType::UINT64:
		return sizeof(uint64_t);
	case VariantLogicalType::UINT128:
		return sizeof(uhugeint_t);
	case VariantLogicalType::FLOAT:
		return sizeof(float);
	case VariantLogicalType::DOUBLE:
		return sizeof(double);
	case VariantLogicalType::UUID:
		return sizeof(hugeint_t);
	case VariantLogicalType::DATE:
		return sizeof(int32_t);
	case VariantLogicalType::TIME_MICROS:
		return sizeof(dtime_t);
	case VariantLogicalType::TIME_NANOS:
		return sizeof(dtime_ns_t);
	case VariantLogicalType::TIMESTAMP_SEC:
		return sizeof(timestamp_sec_t);
	case VariantLogicalType::TIMESTAMP_MILIS:
		return sizeof(timestamp_ms_t);
	case VariantLogicalType::TIMESTAMP_MICROS:
		return sizeof(timestamp_t);
	case VariantLogicalType::TIMESTAMP_NANOS:
		return sizeof(timestamp_ns_t);
	case VariantLogicalType::TIME_MICROS_TZ:
		return sizeof(dtime_tz_t);
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return sizeof(timestamp_tz_t);
	case VariantLogicalType::INTERVAL:
		return sizeof(interval_t);
	default:
		throw InternalException("VariantLogicalType '%s' is not a trivial primitive", EnumUtil::ToString(type));
	}
}

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertVariantToVariant(ToVariantSourceData &source_data, ToVariantGlobalResultData &result_data, idx_t count,
                             optional_ptr<const SelectionVector> selvec,
                             optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {

	auto keys_offset_data = OffsetData::GetKeys(result_data.offsets);
	auto children_offset_data = OffsetData::GetChildren(result_data.offsets);
	auto values_offset_data = OffsetData::GetValues(result_data.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result_data.offsets);

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source_data.vec, source_data.source_size, source_format);
	UnifiedVariantVectorData source(source_format);

	auto &result = result_data.variant;
	for (idx_t source_index = 0; source_index < count; source_index++) {
		auto result_index = selvec ? selvec->get_index(source_index) : source_index;

		auto &keys_list_entry = result.keys_data[result_index];
		auto &children_list_entry = result.children_data[result_index];
		auto blob_data = data_ptr_cast(result.blob_data[result_index].GetDataWriteable());

		auto &keys_offset = keys_offset_data[result_index];
		auto &children_offset = children_offset_data[result_index];
		auto &blob_offset = blob_offset_data[result_index];

		uint32_t keys_count = 0;
		uint32_t blob_size = 0;
		if (!source.RowIsValid(source_index)) {
			if (!IGNORE_NULLS) {
				HandleVariantNull<WRITE_DATA>(result_data, result_index, values_offset_data, blob_offset,
				                              values_index_selvec, source_index, is_root);
			}
			continue;
		}
		if (WRITE_DATA && values_index_selvec) {
			auto &values_offset = values_offset_data[result_index];
			//! Write the values_index for the parent of this column
			result.values_index_data[values_index_selvec->get_index(source_index)] = values_offset;
		}

		//! FIXME: we might want to add some checks to make sure the NumericLimits<uint32_t>::Maximum isn't exceeded,
		//! but that's hard to test

		//! First write all children
		//! NOTE: this has to happen first because we use 'values_offset', which is increased when we write the values
		auto source_children_list_entry = source.GetChildrenListEntry(source_index);
		for (idx_t source_children_index = 0; source_children_index < source_children_list_entry.length;
		     source_children_index++) {
			//! values_index
			if (WRITE_DATA) {
				auto &values_offset = values_offset_data[result_index];
				auto source_value_index = source.GetValuesIndex(source_index, source_children_index);
				result.values_index_data[children_list_entry.offset + children_offset + source_children_index] =
				    values_offset + source_value_index;
			}

			//! keys_index
			if (source.KeysIndexIsValid(source_index, source_children_index)) {
				if (WRITE_DATA) {
					//! Look up the existing key from 'source'
					auto source_key_index = source.GetKeysIndex(source_index, source_children_index);
					auto &source_key_value = source.GetKey(source_index, source_key_index);

					//! Now write this key to the dictionary of the result
					auto dict_index = result_data.GetOrCreateIndex(source_key_value);
					result.keys_index_data[children_list_entry.offset + children_offset + source_children_index] =
					    NumericCast<uint32_t>(keys_offset + keys_count);
					result_data.keys_selvec.set_index(keys_list_entry.offset + keys_offset + keys_count, dict_index);
				}
				keys_count++;
			} else {
				if (WRITE_DATA) {
					result.keys_index_validity.SetInvalid(children_list_entry.offset + children_offset +
					                                      source_children_index);
				}
			}
		}

		auto source_blob_data = const_data_ptr_cast(source.GetData(source_index).GetData());

		//! Then write all values
		auto source_values_list_entry = source.GetValuesListEntry(source_index);
		for (uint32_t source_value_index = 0; source_value_index < source_values_list_entry.length;
		     source_value_index++) {
			auto source_type_id = source.GetTypeId(source_index, source_value_index);
			auto source_byte_offset = source.GetByteOffset(source_index, source_value_index);

			//! NOTE: we have to deserialize these in both passes
			//! because to figure out the size of the 'data' that is added by the VARIANT, we have to traverse the
			//! VARIANT solely because the 'child_index' stored in the OBJECT/ARRAY data could require more bits
			WriteVariantMetadata<WRITE_DATA>(result_data, result_index, values_offset_data, blob_offset + blob_size,
			                                 nullptr, 0, source_type_id);

			if (source_type_id == VariantLogicalType::ARRAY || source_type_id == VariantLogicalType::OBJECT) {
				auto source_nested_data = VariantUtils::DecodeNestedData(source, source_index, source_value_index);
				if (WRITE_DATA) {
					VarintEncode(source_nested_data.child_count, blob_data + blob_offset + blob_size);
				}
				blob_size += GetVarintSize(source_nested_data.child_count);
				if (source_nested_data.child_count) {
					auto new_child_index = source_nested_data.children_idx + children_offset;
					if (WRITE_DATA) {
						VarintEncode(new_child_index, blob_data + blob_offset + blob_size);
					}
					blob_size += GetVarintSize(new_child_index);
				}
			} else if (source_type_id == VariantLogicalType::VARIANT_NULL ||
			           source_type_id == VariantLogicalType::BOOL_FALSE ||
			           source_type_id == VariantLogicalType::BOOL_TRUE) {
				// no-op
			} else if (source_type_id == VariantLogicalType::DECIMAL) {
				auto decimal_blob_data = source_blob_data + source_byte_offset;
				auto width = static_cast<uint8_t>(VarintDecode<uint32_t>(decimal_blob_data));
				auto width_varint_size = GetVarintSize(width);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, decimal_blob_data - width_varint_size,
					       width_varint_size);
				}
				blob_size += width_varint_size;
				auto scale = static_cast<uint8_t>(VarintDecode<uint32_t>(decimal_blob_data));
				auto scale_varint_size = GetVarintSize(scale);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, decimal_blob_data - scale_varint_size,
					       scale_varint_size);
				}
				blob_size += scale_varint_size;

				if (width > DecimalWidth<int64_t>::max) {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(hugeint_t));
					}
					blob_size += sizeof(hugeint_t);
				} else if (width > DecimalWidth<int32_t>::max) {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(int64_t));
					}
					blob_size += sizeof(int64_t);
				} else if (width > DecimalWidth<int16_t>::max) {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(int32_t));
					}
					blob_size += sizeof(int32_t);
				} else {
					if (WRITE_DATA) {
						memcpy(blob_data + blob_offset + blob_size, decimal_blob_data, sizeof(int16_t));
					}
					blob_size += sizeof(int16_t);
				}
			} else if (source_type_id == VariantLogicalType::BITSTRING ||
			           source_type_id == VariantLogicalType::BIGNUM || source_type_id == VariantLogicalType::VARCHAR ||
			           source_type_id == VariantLogicalType::BLOB || source_type_id == VariantLogicalType::GEOMETRY) {
				auto str_blob_data = source_blob_data + source_byte_offset;
				auto str_length = VarintDecode<uint32_t>(str_blob_data);
				auto str_length_varint_size = GetVarintSize(str_length);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, str_blob_data - str_length_varint_size,
					       str_length_varint_size);
				}
				blob_size += str_length_varint_size;
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, str_blob_data, str_length);
				}
				blob_size += str_length;
			} else if (VariantIsTrivialPrimitive(source_type_id)) {
				auto size = VariantTrivialPrimitiveSize(source_type_id);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset, size);
				}
				blob_size += size;
			} else {
				throw InternalException("Unrecognized VariantLogicalType: %s", EnumUtil::ToString(source_type_id));
			}
		}

		keys_offset += keys_count;
		children_offset += source_children_list_entry.length;
		blob_offset += blob_size;
	}
	return true;
}

} // namespace variant
} // namespace duckdb
