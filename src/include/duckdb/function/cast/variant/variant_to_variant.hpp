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
bool ConvertVariantToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                             idx_t source_size, optional_ptr<const SelectionVector> selvec,
                             optional_ptr<const SelectionVector> source_sel, SelectionVector &keys_selvec,
                             OrderedOwningStringMap<uint32_t> &dictionary,
                             optional_ptr<const SelectionVector> value_ids_selvec, const bool is_root) {

	auto keys_offset_data = OffsetData::GetKeys(offsets);
	auto children_offset_data = OffsetData::GetChildren(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto blob_offset_data = OffsetData::GetBlob(offsets);

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(source, source_size, source_format);

	//! source keys
	auto &source_keys = UnifiedVariantVector::GetKeys(source_format);
	auto source_keys_data = source_keys.GetData<list_entry_t>(source_keys);

	//! source keys entry
	auto &source_keys_entry = UnifiedVariantVector::GetKeysEntry(source_format);
	auto source_keys_entry_data = source_keys_entry.GetData<string_t>(source_keys_entry);

	//! source children
	auto &source_children = UnifiedVariantVector::GetChildren(source_format);
	auto source_children_data = source_children.GetData<list_entry_t>(source_children);

	//! source values
	auto &source_values = UnifiedVariantVector::GetValues(source_format);
	auto source_values_data = source_values.GetData<list_entry_t>(source_values);

	//! source data
	auto &source_data = UnifiedVariantVector::GetData(source_format);
	auto source_data_data = source_data.GetData<string_t>(source_data);

	//! source byte_offset
	auto &source_byte_offset = UnifiedVariantVector::GetValuesByteOffset(source_format);
	auto source_byte_offset_data = source_byte_offset.GetData<uint32_t>(source_byte_offset);

	//! source type_id
	auto &source_type_id = UnifiedVariantVector::GetValuesTypeId(source_format);
	auto source_type_id_data = source_type_id.GetData<uint8_t>(source_type_id);

	//! source key_id
	auto &source_key_id = UnifiedVariantVector::GetChildrenKeyId(source_format);
	auto source_key_id_data = source_key_id.GetData<uint32_t>(source_key_id);

	//! source value_id
	auto &source_value_id = UnifiedVariantVector::GetChildrenValueId(source_format);
	auto source_value_id_data = source_value_id.GetData<uint32_t>(source_value_id);

	auto &source_validity = source_format.unified.validity;

	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.unified.sel->get_index(i);
		auto result_index = selvec ? selvec->get_index(i) : i;

		auto &keys_list_entry = result.keys_data[result_index];
		auto &children_list_entry = result.children_data[result_index];
		auto blob_data = data_ptr_cast(result.blob_data[result_index].GetDataWriteable());

		auto &keys_offset = keys_offset_data[result_index];
		auto &children_offset = children_offset_data[result_index];
		auto &values_offset = values_offset_data[result_index];
		auto &blob_offset = blob_offset_data[result_index];

		uint32_t keys_count = 0;
		uint32_t blob_size = 0;
		if (!source_validity.RowIsValid(index)) {
			if (!IGNORE_NULLS) {
				HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec,
				                              i, is_root);
			}
			continue;
		}
		auto source_keys_list_entry = source_keys_data[index];
		auto source_children_list_entry = source_children_data[index];
		auto source_values_list_entry = source_values_data[index];
		auto source_blob_data = const_data_ptr_cast(source_data_data[index].GetData());

		D_ASSERT(source_values_list_entry.length);
		if (WRITE_DATA && value_ids_selvec) {
			//! Write the value_id for the parent of this column
			result.value_id_data[value_ids_selvec->get_index(i)] = values_offset;
		}

		//! FIXME: we might want to add some checks to make sure the NumericLimits<uint32_t>::Maximum isn't exceeded,
		//! but that's hard to test

		//! First write all children
		//! NOTE: this has to happen first because we use 'values_offset', which is increased when we write the values
		for (idx_t j = 0; j < source_children_list_entry.length; j++) {

			//! value_id
			if (WRITE_DATA) {
				auto source_value_id_index = source_value_id.sel->get_index(j + source_children_list_entry.offset);
				auto source_value_id_value = source_value_id_data[source_value_id_index];
				result.value_id_data[children_list_entry.offset + children_offset + j] =
				    values_offset + source_value_id_value;
			}

			//! key_id
			auto source_key_id_index = source_key_id.sel->get_index(j + source_children_list_entry.offset);
			if (source_key_id.validity.RowIsValid(source_key_id_index)) {
				if (WRITE_DATA) {
					auto source_key_id_value = source_key_id_data[source_key_id_index];

					//! Look up the existing key from 'source'
					auto source_key_entry_index =
					    source_keys_entry.sel->get_index(source_keys_list_entry.offset + source_key_id_value);
					auto &source_key_value = source_keys_entry_data[source_key_entry_index];

					//! Now write this key to the dictionary of the result
					auto dictionary_size = dictionary.size();
					auto dict_index =
					    dictionary.emplace(std::make_pair(source_key_value, dictionary_size)).first->second;
					result.key_id_data[children_list_entry.offset + children_offset + j] =
					    NumericCast<uint32_t>(keys_offset + keys_count);
					keys_selvec.set_index(keys_list_entry.offset + keys_offset + keys_count, dict_index);
				}
				keys_count++;
			} else {
				if (WRITE_DATA) {
					result.key_id_validity.SetInvalid(children_list_entry.offset + children_offset + j);
				}
			}
		}

		//! Then write all values
		for (idx_t j = 0; j < source_values_list_entry.length; j++) {
			auto source_type_id_index = source_type_id.sel->get_index(j + source_values_list_entry.offset);
			auto source_type_id_value = static_cast<VariantLogicalType>(source_type_id_data[source_type_id_index]);

			auto source_byte_offset_index = source_byte_offset.sel->get_index(j + source_values_list_entry.offset);
			auto source_byte_offset_value = source_byte_offset_data[source_byte_offset_index];

			//! NOTE: we have to deserialize these in both passes
			//! because to figure out the size of the 'data' that is added by the VARIANT, we have to traverse the
			//! VARIANT solely because the 'child_index' stored in the OBJECT/ARRAY data could require more bits
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset + blob_size, nullptr,
			                                 0, source_type_id_value);

			if (source_type_id_value == VariantLogicalType::ARRAY ||
			    source_type_id_value == VariantLogicalType::OBJECT) {
				auto container_blob_data = source_blob_data + source_byte_offset_value;
				auto length = VarintDecode<uint32_t>(container_blob_data);
				if (WRITE_DATA) {
					VarintEncode(length, blob_data + blob_offset + blob_size);
				}
				blob_size += GetVarintSize(length);
				if (length) {
					auto child_index = VarintDecode<uint32_t>(container_blob_data);
					auto new_child_index = child_index + children_offset;
					if (WRITE_DATA) {
						VarintEncode(new_child_index, blob_data + blob_offset + blob_size);
					}
					blob_size += GetVarintSize(new_child_index);
				}
			} else if (source_type_id_value == VariantLogicalType::VARIANT_NULL ||
			           source_type_id_value == VariantLogicalType::BOOL_FALSE ||
			           source_type_id_value == VariantLogicalType::BOOL_TRUE) {
				// no-op
			} else if (source_type_id_value == VariantLogicalType::DECIMAL) {
				auto decimal_blob_data = source_blob_data + source_byte_offset_value;
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
			} else if (source_type_id_value == VariantLogicalType::BITSTRING ||
			           source_type_id_value == VariantLogicalType::BIGNUM ||
			           source_type_id_value == VariantLogicalType::VARCHAR ||
			           source_type_id_value == VariantLogicalType::BLOB) {
				auto str_blob_data = source_blob_data + source_byte_offset_value;
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
			} else if (VariantIsTrivialPrimitive(source_type_id_value)) {
				auto size = VariantTrivialPrimitiveSize(source_type_id_value);
				if (WRITE_DATA) {
					memcpy(blob_data + blob_offset + blob_size, source_blob_data + source_byte_offset_value, size);
				}
				blob_size += size;
			} else {
				throw InternalException("Unrecognized VariantLogicalType: %s",
				                        EnumUtil::ToString(source_type_id_value));
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
