#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/serializer/varint.hpp"

namespace duckdb {

PhysicalType VariantDecimalData::GetPhysicalType() const {
	if (width > DecimalWidth<int64_t>::max) {
		return PhysicalType::INT128;
	} else if (width > DecimalWidth<int32_t>::max) {
		return PhysicalType::INT64;
	} else if (width > DecimalWidth<int16_t>::max) {
		return PhysicalType::INT32;
	} else {
		return PhysicalType::INT16;
	}
}

bool VariantUtils::IsNestedType(const UnifiedVariantVectorData &variant, idx_t row, uint32_t value_index) {
	auto type_id = variant.GetTypeId(row, value_index);
	return type_id == VariantLogicalType::ARRAY || type_id == VariantLogicalType::OBJECT;
}

VariantDecimalData VariantUtils::DecodeDecimalData(const UnifiedVariantVectorData &variant, idx_t row,
                                                   uint32_t value_index) {
	D_ASSERT(variant.GetTypeId(row, value_index) == VariantLogicalType::DECIMAL);
	auto byte_offset = variant.GetByteOffset(row, value_index);
	auto data = const_data_ptr_cast(variant.GetData(row).GetData());
	auto ptr = data + byte_offset;

	auto width = VarintDecode<uint32_t>(ptr);
	auto scale = VarintDecode<uint32_t>(ptr);
	auto value_ptr = ptr;
	return VariantDecimalData(width, scale, value_ptr);
}

string_t VariantUtils::DecodeStringData(const UnifiedVariantVectorData &variant, idx_t row, uint32_t value_index) {
	auto byte_offset = variant.GetByteOffset(row, value_index);
	auto data = const_data_ptr_cast(variant.GetData(row).GetData());
	auto ptr = data + byte_offset;

	auto length = VarintDecode<uint32_t>(ptr);
	return string_t(reinterpret_cast<const char *>(ptr), length);
}

VariantNestedData VariantUtils::DecodeNestedData(const UnifiedVariantVectorData &variant, idx_t row,
                                                 uint32_t value_index) {
	D_ASSERT(IsNestedType(variant, row, value_index));
	auto byte_offset = variant.GetByteOffset(row, value_index);
	auto data = const_data_ptr_cast(variant.GetData(row).GetData());
	auto ptr = data + byte_offset;

	VariantNestedData result;
	result.is_null = false;
	result.child_count = VarintDecode<uint32_t>(ptr);
	if (result.child_count) {
		result.children_idx = VarintDecode<uint32_t>(ptr);
	} else {
		result.children_idx = 0;
	}
	return result;
}

vector<string> VariantUtils::GetObjectKeys(const UnifiedVariantVectorData &variant, idx_t row,
                                           const VariantNestedData &nested_data) {
	vector<string> object_keys;
	for (idx_t child_idx = 0; child_idx < nested_data.child_count; child_idx++) {
		auto child_key_id = variant.GetKeysIndex(row, nested_data.children_idx + child_idx);
		object_keys.push_back(variant.GetKey(row, child_key_id).GetString());
	}
	return object_keys;
}

void VariantUtils::FindChildValues(const UnifiedVariantVectorData &variant, const VariantPathComponent &component,
                                   optional_ptr<const SelectionVector> sel, SelectionVector &res,
                                   ValidityMask &res_validity, VariantNestedData *nested_data, idx_t count) {

	for (idx_t i = 0; i < count; i++) {
		auto row_index = sel ? sel->get_index(i) : i;

		auto &nested_data_entry = nested_data[i];
		if (nested_data_entry.is_null) {
			continue;
		}
		if (component.lookup_mode == VariantChildLookupMode::BY_INDEX) {
			auto child_idx = component.index;
			if (child_idx >= nested_data_entry.child_count) {
				//! The list is too small to contain this index
				res_validity.SetInvalid(i);
				continue;
			}
			auto value_id = variant.GetValuesIndex(row_index, nested_data_entry.children_idx + child_idx);
			res[i] = static_cast<uint8_t>(value_id);
			continue;
		}
		bool found_child = false;
		for (idx_t child_idx = 0; child_idx < nested_data_entry.child_count; child_idx++) {
			auto value_id = variant.GetValuesIndex(row_index, nested_data_entry.children_idx + child_idx);
			auto key_id = variant.GetKeysIndex(row_index, nested_data_entry.children_idx + child_idx);

			auto &child_key = variant.GetKey(row_index, key_id);
			if (child_key == component.key) {
				//! Found the key we're looking for
				res[i] = value_id;
				found_child = true;
				break;
			}
		}
		if (!found_child) {
			res_validity.SetInvalid(i);
		}
	}
}

vector<uint32_t> VariantUtils::ValueIsNull(const UnifiedVariantVectorData &variant, const SelectionVector &sel,
                                           idx_t count, optional_idx row) {
	vector<uint32_t> res;
	res.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		if (!variant.RowIsValid(row_index)) {
			res.push_back(static_cast<uint32_t>(i));
			continue;
		}

		//! Get the index into 'values'
		auto type_id = variant.GetTypeId(row_index, sel[i]);
		if (type_id == VariantLogicalType::VARIANT_NULL) {
			res.push_back(static_cast<uint32_t>(i));
		}
	}
	return res;
}

VariantNestedDataCollectionResult
VariantUtils::CollectNestedData(const UnifiedVariantVectorData &variant, VariantLogicalType expected_type,
                                const SelectionVector &sel, idx_t count, optional_idx row, idx_t offset,
                                VariantNestedData *child_data, ValidityMask &validity) {
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		//! NOTE: the validity is assumed to be from a FlatVector
		if (!variant.RowIsValid(row_index) || !validity.RowIsValid(offset + i)) {
			child_data[i].is_null = true;
			continue;
		}
		auto type_id = variant.GetTypeId(row_index, sel[i]);
		if (type_id == VariantLogicalType::VARIANT_NULL) {
			child_data[i].is_null = true;
			continue;
		}

		if (type_id != expected_type) {
			return VariantNestedDataCollectionResult(type_id);
		}
		child_data[i] = DecodeNestedData(variant, row_index, sel[i]);
	}
	return VariantNestedDataCollectionResult();
}

Value VariantUtils::ConvertVariantToValue(const UnifiedVariantVectorData &variant, idx_t row, idx_t values_idx) {
	if (!variant.RowIsValid(row)) {
		return Value(LogicalTypeId::SQLNULL);
	}

	//! The 'values' data of the value we're currently converting
	auto type_id = variant.GetTypeId(row, values_idx);
	auto byte_offset = variant.GetByteOffset(row, values_idx);

	//! The blob data of the Variant, accessed by byte offset retrieved above ^
	auto blob_data = const_data_ptr_cast(variant.GetData(row).GetData());

	auto ptr = const_data_ptr_cast(blob_data + byte_offset);
	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
		return Value(LogicalType::SQLNULL);
	case VariantLogicalType::BOOL_TRUE:
		return Value::BOOLEAN(true);
	case VariantLogicalType::BOOL_FALSE:
		return Value::BOOLEAN(false);
	case VariantLogicalType::INT8:
		return Value::TINYINT(Load<int8_t>(ptr));
	case VariantLogicalType::INT16:
		return Value::SMALLINT(Load<int16_t>(ptr));
	case VariantLogicalType::INT32:
		return Value::INTEGER(Load<int32_t>(ptr));
	case VariantLogicalType::INT64:
		return Value::BIGINT(Load<int64_t>(ptr));
	case VariantLogicalType::INT128:
		return Value::HUGEINT(Load<hugeint_t>(ptr));
	case VariantLogicalType::UINT8:
		return Value::UTINYINT(Load<uint8_t>(ptr));
	case VariantLogicalType::UINT16:
		return Value::USMALLINT(Load<uint16_t>(ptr));
	case VariantLogicalType::UINT32:
		return Value::UINTEGER(Load<uint32_t>(ptr));
	case VariantLogicalType::UINT64:
		return Value::UBIGINT(Load<uint64_t>(ptr));
	case VariantLogicalType::UINT128:
		return Value::UHUGEINT(Load<uhugeint_t>(ptr));
	case VariantLogicalType::UUID:
		return Value::UUID(Load<hugeint_t>(ptr));
	case VariantLogicalType::INTERVAL:
		return Value::INTERVAL(Load<interval_t>(ptr));
	case VariantLogicalType::FLOAT:
		return Value::FLOAT(Load<float>(ptr));
	case VariantLogicalType::DOUBLE:
		return Value::DOUBLE(Load<double>(ptr));
	case VariantLogicalType::DATE:
		return Value::DATE(date_t(Load<int32_t>(ptr)));
	case VariantLogicalType::BLOB: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		return Value::BLOB(const_data_ptr_cast(string_data), string_length);
	}
	case VariantLogicalType::VARCHAR: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		return Value(string_t(string_data, string_length));
	}
	case VariantLogicalType::DECIMAL: {
		auto width = NumericCast<uint8_t>(VarintDecode<idx_t>(ptr));
		auto scale = NumericCast<uint8_t>(VarintDecode<idx_t>(ptr));

		if (width > DecimalWidth<int64_t>::max) {
			return Value::DECIMAL(Load<hugeint_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int32_t>::max) {
			return Value::DECIMAL(Load<int64_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int16_t>::max) {
			return Value::DECIMAL(Load<int32_t>(ptr), width, scale);
		} else {
			return Value::DECIMAL(Load<int16_t>(ptr), width, scale);
		}
	}
	case VariantLogicalType::TIME_MICROS:
		return Value::TIME(Load<dtime_t>(ptr));
	case VariantLogicalType::TIME_MICROS_TZ:
		return Value::TIMETZ(Load<dtime_tz_t>(ptr));
	case VariantLogicalType::TIMESTAMP_MICROS:
		return Value::TIMESTAMP(Load<timestamp_t>(ptr));
	case VariantLogicalType::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(Load<timestamp_sec_t>(ptr));
	case VariantLogicalType::TIMESTAMP_NANOS:
		return Value::TIMESTAMPNS(Load<timestamp_ns_t>(ptr));
	case VariantLogicalType::TIMESTAMP_MILIS:
		return Value::TIMESTAMPMS(Load<timestamp_ms_t>(ptr));
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return Value::TIMESTAMPTZ(Load<timestamp_tz_t>(ptr));
	case VariantLogicalType::ARRAY: {
		auto count = VarintDecode<uint32_t>(ptr);
		vector<Value> array_items;
		if (count) {
			auto child_index_start = VarintDecode<uint32_t>(ptr);
			for (idx_t i = 0; i < count; i++) {
				auto child_index = variant.GetValuesIndex(row, child_index_start + i);
				array_items.emplace_back(ConvertVariantToValue(variant, row, child_index));
			}
		}
		return Value::LIST(LogicalType::VARIANT(), std::move(array_items));
	}
	case VariantLogicalType::OBJECT: {
		auto count = VarintDecode<uint32_t>(ptr);
		child_list_t<Value> object_children;
		if (count) {
			auto child_index_start = VarintDecode<uint32_t>(ptr);
			for (idx_t i = 0; i < count; i++) {
				auto child_value_idx = variant.GetValuesIndex(row, child_index_start + i);
				auto val = ConvertVariantToValue(variant, row, child_value_idx);

				auto child_key_id = variant.GetKeysIndex(row, child_index_start + i);
				auto &key = variant.GetKey(row, child_key_id);

				object_children.emplace_back(key.GetString(), std::move(val));
			}
		}
		return Value::STRUCT(std::move(object_children));
	}
	case VariantLogicalType::BITSTRING: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		return Value::BIT(ptr, string_length);
	}
	case VariantLogicalType::BIGNUM: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		return Value::BIGNUM(ptr, string_length);
	}
	default:
		throw InternalException("VariantLogicalType(%d) not handled", static_cast<uint8_t>(type_id));
	}
}

bool VariantUtils::Verify(Vector &variant, const SelectionVector &sel_p, idx_t count) {
	RecursiveUnifiedVectorFormat format;
	Vector::RecursiveToUnifiedFormat(variant, count, format);

	//! keys
	auto &keys = UnifiedVariantVector::GetKeys(format);
	auto keys_data = keys.GetData<list_entry_t>(keys);

	//! keys_entry
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(format);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);
	D_ASSERT(keys_entry.validity.AllValid());

	//! children
	auto &children = UnifiedVariantVector::GetChildren(format);
	auto children_data = children.GetData<list_entry_t>(children);

	//! children.key_id
	auto &key_id = UnifiedVariantVector::GetChildrenKeysIndex(format);
	auto key_id_data = key_id.GetData<uint32_t>(key_id);

	//! children.value_id
	auto &value_id = UnifiedVariantVector::GetChildrenValuesIndex(format);
	auto value_id_data = value_id.GetData<uint32_t>(value_id);

	//! values
	auto &values = UnifiedVariantVector::GetValues(format);
	auto values_data = values.GetData<list_entry_t>(values);

	//! values.type_id
	auto &type_id = UnifiedVariantVector::GetValuesTypeId(format);
	auto type_id_data = type_id.GetData<uint8_t>(type_id);

	//! values.byte_offset
	auto &byte_offset = UnifiedVariantVector::GetValuesByteOffset(format);
	auto byte_offset_data = byte_offset.GetData<uint32_t>(byte_offset);

	//! data
	auto &data = UnifiedVariantVector::GetData(format);
	auto data_data = data.GetData<string_t>(data);

	for (idx_t i = 0; i < count; i++) {
		auto mapped_row = sel_p.get_index(i);
		auto index = format.unified.sel->get_index(mapped_row);

		if (!format.unified.validity.RowIsValid(index)) {
			continue;
		}

		auto keys_index = keys.sel->get_index(mapped_row);
		auto children_index = children.sel->get_index(mapped_row);
		auto values_index = values.sel->get_index(mapped_row);
		auto data_index = data.sel->get_index(mapped_row);

		D_ASSERT(keys.validity.RowIsValid(keys_index));
		D_ASSERT(children.validity.RowIsValid(children_index));
		D_ASSERT(values.validity.RowIsValid(values_index));
		D_ASSERT(data.validity.RowIsValid(data_index));

		auto keys_list_entry = keys_data[keys_index];
		auto children_list_entry = children_data[children_index];
		auto values_list_entry = values_data[values_index];
		auto &blob = data_data[data_index];

		//! verify keys
		for (idx_t j = 0; j < keys_list_entry.length; j++) {
			auto keys_entry_index = keys_entry.sel->get_index(j + keys_list_entry.offset);
			D_ASSERT(keys_entry.validity.RowIsValid(keys_entry_index));
			keys_entry_data[keys_entry_index].Verify();
		}
		//! verify children
		for (idx_t j = 0; j < children_list_entry.length; j++) {
			auto key_id_index = key_id.sel->get_index(j + children_list_entry.offset);
			if (key_id.validity.RowIsValid(key_id_index)) {
				auto children_key_id = key_id_data[key_id_index];
				D_ASSERT(children_key_id < keys_list_entry.length);
				(void)children_key_id;
			}

			auto value_id_index = value_id.sel->get_index(j + children_list_entry.offset);
			D_ASSERT(value_id.validity.RowIsValid(value_id_index));
			auto children_value_id = value_id_data[value_id_index];
			D_ASSERT(children_value_id < values_list_entry.length);
			(void)children_value_id;
		}

		//! verify values
		for (idx_t j = 0; j < values_list_entry.length; j++) {
			auto type_id_index = type_id.sel->get_index(j + values_list_entry.offset);
			D_ASSERT(type_id.validity.RowIsValid(type_id_index));
			auto value_type_id = type_id_data[type_id_index];
			D_ASSERT(value_type_id < static_cast<uint8_t>(VariantLogicalType::ENUM_SIZE));

			auto byte_offset_index = byte_offset.sel->get_index(j + values_list_entry.offset);
			D_ASSERT(byte_offset.validity.RowIsValid(byte_offset_index));
			auto value_byte_offset = byte_offset_data[byte_offset_index];
			D_ASSERT(value_byte_offset <= blob.GetSize());

			if (j == 0) {
				//! If the root value is NULL, the row itself should be NULL, not use VARIANT_NULL for the value
				D_ASSERT(value_type_id != static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL));
			}

			auto blob_data = const_data_ptr_cast(blob.GetData()) + value_byte_offset;
			switch (static_cast<VariantLogicalType>(value_type_id)) {
			case VariantLogicalType::OBJECT:
			case VariantLogicalType::ARRAY: {
				auto length = VarintDecode<uint32_t>(blob_data);
				if (!length) {
					break;
				}
				auto children_start_index = VarintDecode<uint32_t>(blob_data);
				D_ASSERT(children_start_index + length <= children_list_entry.length);

				//! Verify the validity of array/object key_ids
				for (idx_t child_idx = 0; child_idx < length; child_idx++) {
					auto child_key_id_index =
					    key_id.sel->get_index(children_list_entry.offset + children_start_index + child_idx);
					if (value_type_id == static_cast<uint8_t>(VariantLogicalType::OBJECT)) {
						D_ASSERT(key_id.validity.RowIsValid(child_key_id_index));
					} else {
						D_ASSERT(!key_id.validity.RowIsValid(child_key_id_index));
					}
					(void)child_key_id_index;
				}
				break;
			}
			default:
				break;
			}
		}
	}

	return true;
}

} // namespace duckdb
