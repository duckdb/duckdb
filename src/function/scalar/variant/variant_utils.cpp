#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/serializer/varint.hpp"

namespace duckdb {

void VariantUtils::FinalizeVariantKeys(Vector &variant, OrderedOwningStringMap<uint32_t> &dictionary,
                                       SelectionVector &sel, idx_t sel_size) {
	auto &keys = VariantVector::GetKeys(variant);
	auto &keys_entry = ListVector::GetEntry(keys);
	auto keys_entry_data = FlatVector::GetData<string_t>(keys_entry);

	bool already_sorted = true;

	vector<idx_t> unsorted_to_sorted(dictionary.size());
	auto it = dictionary.begin();
	for (idx_t i = 0; i < dictionary.size(); i++) {
		auto unsorted_idx = it->second;
		if (unsorted_idx != i) {
			already_sorted = false;
		}
		unsorted_to_sorted[unsorted_idx] = i;
		D_ASSERT(i < ListVector::GetListSize(keys));
		keys_entry_data[i] = it->first;
		keys_entry_data[i].SetSizeAndFinalize(static_cast<uint32_t>(keys_entry_data[i].GetSize()));
		it++;
	}

	if (!already_sorted) {
		//! Adjust the selection vector to point to the right dictionary index
		for (idx_t i = 0; i < sel_size; i++) {
			auto old_dictionary_index = sel.get_index(i);
			auto new_dictionary_index = unsorted_to_sorted[old_dictionary_index];
			sel.set_index(i, new_dictionary_index);
		}
	}
}

bool VariantUtils::FindChildValues(RecursiveUnifiedVectorFormat &source, const VariantPathComponent &component,
                                   optional_idx row, SelectionVector &res, VariantNestedData *nested_data,
                                   idx_t count) {
	//! children
	auto &children = UnifiedVariantVector::GetChildren(source);
	auto children_data = children.GetData<list_entry_t>(children);

	//! value_ids
	auto &value_ids = UnifiedVariantVector::GetChildrenValueId(source);
	auto value_ids_data = value_ids.GetData<uint32_t>(value_ids);

	//! key_ids
	auto &key_ids = UnifiedVariantVector::GetChildrenKeyId(source);
	auto key_ids_data = key_ids.GetData<uint32_t>(key_ids);

	//! keys
	auto &keys = UnifiedVariantVector::GetKeys(source);
	auto keys_data = keys.GetData<list_entry_t>(keys);

	//! entry of the keys list
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);

	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;
		auto &children_list_entry = children_data[children.sel->get_index(row_index)];

		auto &nested_data_entry = nested_data[i];
		if (nested_data_entry.is_null) {
			continue;
		}
		if (component.lookup_mode == VariantChildLookupMode::BY_INDEX) {
			auto child_idx = component.index;
			if (child_idx == 0) {
				return false;
			}
			child_idx--;
			if (child_idx >= nested_data_entry.child_count) {
				//! The list is too small to contain this index
				return false;
			}
			auto children_index = children_list_entry.offset + nested_data_entry.children_idx + child_idx;
			auto value_id = value_ids_data[value_ids.sel->get_index(children_index)];
			res[i] = value_id;
			continue;
		}
		auto &keys_list_entry = keys_data[keys.sel->get_index(row_index)];
		bool found_child = false;
		for (idx_t child_idx = 0; child_idx < nested_data_entry.child_count; child_idx++) {
			auto children_index = children_list_entry.offset + nested_data_entry.children_idx + child_idx;
			auto value_id = value_ids_data[value_ids.sel->get_index(children_index)];

			auto key_id = key_ids_data[key_ids.sel->get_index(children_index)];
			auto key_index = keys_entry.sel->get_index(keys_list_entry.offset + key_id);
			auto &child_key = keys_entry_data[key_index];
			if (child_key == component.key) {
				//! Found the key we're looking for
				res[i] = value_id;
				found_child = true;
				break;
			}
		}
		if (!found_child) {
			return false;
		}
	}
	return true;
}

vector<uint8_t> VariantUtils::ValueIsNull(RecursiveUnifiedVectorFormat &variant, const SelectionVector &sel,
                                          idx_t count, optional_idx row) {
	auto &values_format = UnifiedVariantVector::GetValues(variant);
	auto values_data = values_format.GetData<list_entry_t>(values_format);

	auto &type_id_format = UnifiedVariantVector::GetValuesTypeId(variant);
	auto type_id_data = type_id_format.GetData<uint8_t>(type_id_format);

	vector<uint8_t> res;
	res.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		auto index = variant.unified.sel->get_index(row_index);
		if (!variant.unified.validity.RowIsValid(index)) {
			res.push_back(true);
			continue;
		}

		//! values
		auto values_index = values_format.sel->get_index(index);
		D_ASSERT(values_format.validity.RowIsValid(values_index));
		auto values_list_entry = values_data[values_index];

		//! Get the index into 'values'
		uint32_t value_index = sel[i];
		auto type_id = static_cast<VariantLogicalType>(
		    type_id_data[type_id_format.sel->get_index(values_list_entry.offset + value_index)]);

		if (type_id == VariantLogicalType::VARIANT_NULL) {
			res.push_back(true);
		} else {
			res.push_back(false);
		}
	}
	return res;
}

bool VariantUtils::CollectNestedData(RecursiveUnifiedVectorFormat &variant, VariantLogicalType expected_type,
                                     const SelectionVector &sel, idx_t count, optional_idx row,
                                     VariantNestedData *child_data, ValidityMask &validity, string &error) {
	auto &values_format = UnifiedVariantVector::GetValues(variant);
	auto values_data = values_format.GetData<list_entry_t>(values_format);

	auto &type_id_format = UnifiedVariantVector::GetValuesTypeId(variant);
	auto type_id_data = type_id_format.GetData<uint8_t>(type_id_format);

	auto &byte_offset_format = UnifiedVariantVector::GetValuesByteOffset(variant);
	auto byte_offset_data = byte_offset_format.GetData<uint32_t>(byte_offset_format);

	auto &value_format = UnifiedVariantVector::GetData(variant);
	auto value_data = value_format.GetData<string_t>(value_format);

	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		auto index = variant.unified.sel->get_index(row_index);
		//! NOTE: the validity is assumed to be from a FlatVector
		if (!variant.unified.validity.RowIsValid(index) || !validity.RowIsValid(i)) {
			child_data[i].is_null = true;
			continue;
		}
		child_data[i].is_null = false;

		//! values
		auto values_index = values_format.sel->get_index(row_index);
		D_ASSERT(values_format.validity.RowIsValid(values_index));
		auto values_list_entry = values_data[values_index];

		//! Get the index into 'values'
		uint32_t value_index = sel[i];

		//! type_id + byte_offset
		auto type_id = static_cast<VariantLogicalType>(
		    type_id_data[type_id_format.sel->get_index(values_list_entry.offset + value_index)]);
		auto byte_offset = byte_offset_data[byte_offset_format.sel->get_index(values_list_entry.offset + value_index)];

		if (type_id == VariantLogicalType::VARIANT_NULL) {
			child_data[i].is_null = true;
			continue;
		}

		if (type_id != expected_type) {
			error = StringUtil::Format("'%s' was expected, found '%s', can't convert VARIANT",
			                           EnumUtil::ToString(expected_type), EnumUtil::ToString(type_id));
			return false;
		}

		auto blob_index = value_format.sel->get_index(row_index);
		auto blob_data = const_data_ptr_cast(value_data[blob_index].GetData());

		auto ptr = blob_data + byte_offset;
		child_data[i].child_count = VarintDecode<uint32_t>(ptr);
		if (child_data[i].child_count) {
			child_data[i].children_idx = VarintDecode<uint32_t>(ptr);
		} else {
			child_data[i].children_idx = 0;
		}
	}
	return true;
}

Value VariantUtils::ConvertVariantToValue(RecursiveUnifiedVectorFormat &source, idx_t row, idx_t values_idx) {
	auto index = source.unified.sel->get_index(row);
	if (!source.unified.validity.RowIsValid(index)) {
		return Value(LogicalTypeId::SQLNULL);
	}

	//! values
	auto &values = UnifiedVariantVector::GetValues(source);
	auto values_data = values.GetData<list_entry_t>(values);

	//! type_ids
	auto &type_ids = UnifiedVariantVector::GetValuesTypeId(source);
	auto type_ids_data = type_ids.GetData<uint8_t>(type_ids);

	//! byte_offsets
	auto &byte_offsets = UnifiedVariantVector::GetValuesByteOffset(source);
	auto byte_offsets_data = byte_offsets.GetData<uint32_t>(byte_offsets);

	//! children
	auto &children = UnifiedVariantVector::GetChildren(source);
	auto children_data = children.GetData<list_entry_t>(children);

	//! value_ids
	auto &value_ids = UnifiedVariantVector::GetChildrenValueId(source);
	auto value_ids_data = value_ids.GetData<uint32_t>(value_ids);

	//! key_ids
	auto &key_ids = UnifiedVariantVector::GetChildrenKeyId(source);
	auto key_ids_data = key_ids.GetData<uint32_t>(key_ids);

	//! keys
	auto &keys = UnifiedVariantVector::GetKeys(source);
	auto keys_data = keys.GetData<list_entry_t>(keys);
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);

	//! list entries
	auto keys_list_entry = keys_data[keys.sel->get_index(row)];
	auto children_list_entry = children_data[children.sel->get_index(row)];
	auto values_list_entry = values_data[values.sel->get_index(row)];

	//! The 'values' data of the value we're currently converting
	values_idx += values_list_entry.offset;
	auto type_id = static_cast<VariantLogicalType>(type_ids_data[type_ids.sel->get_index(values_idx)]);
	auto byte_offset = byte_offsets_data[byte_offsets.sel->get_index(values_idx)];

	//! The blob data of the Variant, accessed by byte offset retrieved above ^
	auto &value = UnifiedVariantVector::GetData(source);
	auto value_data = value.GetData<string_t>(value);
	auto &blob = value_data[value.sel->get_index(row)];
	auto blob_data = const_data_ptr_cast(blob.GetData());

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
				auto index = value_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_index = value_ids_data[index];
				array_items.emplace_back(ConvertVariantToValue(source, row, child_index));
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
				auto children_index = value_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_value_idx = value_ids_data[children_index];
				auto val = ConvertVariantToValue(source, row, child_value_idx);

				auto key_ids_index = key_ids.sel->get_index(children_list_entry.offset + child_index_start + i);
				auto child_key_id = key_ids_data[key_ids_index];
				auto &key = keys_entry_data[keys_entry.sel->get_index(keys_list_entry.offset + child_key_id)];

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

	return nullptr;
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
	auto &key_id = UnifiedVariantVector::GetChildrenKeyId(format);
	auto key_id_data = key_id.GetData<uint32_t>(key_id);

	//! children.value_id
	auto &value_id = UnifiedVariantVector::GetChildrenValueId(format);
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
			auto keys_index = keys_entry.sel->get_index(j + keys_list_entry.offset);
			D_ASSERT(keys_entry.validity.RowIsValid(keys_index));
			keys_entry_data[keys_index].Verify();
		}
		//! verify children
		for (idx_t j = 0; j < children_list_entry.length; j++) {
			auto key_id_index = key_id.sel->get_index(j + children_list_entry.offset);
			if (key_id.validity.RowIsValid(key_id_index)) {
				auto children_key_id = key_id_data[key_id_index];
				D_ASSERT(children_key_id < keys_list_entry.length);
			}

			auto value_id_index = value_id.sel->get_index(j + children_list_entry.offset);
			D_ASSERT(value_id.validity.RowIsValid(value_id_index));
			auto children_value_id = value_id_data[value_id_index];
			D_ASSERT(children_value_id < values_list_entry.length);
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
