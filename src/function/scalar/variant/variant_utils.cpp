#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/types/variant_visitor.hpp"

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

//! FIXME: this shouldn't return a "result", it should populate a validity mask instead.
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

namespace {

struct ValueConverter {
	using result_type = Value;

	static Value VisitNull() {
		return Value(LogicalType::SQLNULL);
	}

	static Value VisitBoolean(bool val) {
		return Value::BOOLEAN(val);
	}

	template <typename T>
	static Value VisitInteger(T val) {
		throw InternalException("ValueConverter::VisitInteger not implemented!");
	}

	static Value VisitTime(dtime_t val) {
		return Value::TIME(val);
	}

	static Value VisitTimeNanos(dtime_ns_t val) {
		return Value::TIME_NS(val);
	}

	static Value VisitTimeTZ(dtime_tz_t val) {
		return Value::TIMETZ(val);
	}

	static Value VisitTimestampSec(timestamp_sec_t val) {
		return Value::TIMESTAMPSEC(val);
	}

	static Value VisitTimestampMs(timestamp_ms_t val) {
		return Value::TIMESTAMPMS(val);
	}

	static Value VisitTimestamp(timestamp_t val) {
		return Value::TIMESTAMP(val);
	}

	static Value VisitTimestampNanos(timestamp_ns_t val) {
		return Value::TIMESTAMPNS(val);
	}

	static Value VisitTimestampTZ(timestamp_tz_t val) {
		return Value::TIMESTAMPTZ(val);
	}

	static Value VisitFloat(float val) {
		return Value::FLOAT(val);
	}
	static Value VisitDouble(double val) {
		return Value::DOUBLE(val);
	}
	static Value VisitUUID(hugeint_t val) {
		return Value::UUID(val);
	}
	static Value VisitDate(date_t val) {
		return Value::DATE(val);
	}
	static Value VisitInterval(interval_t val) {
		return Value::INTERVAL(val);
	}

	static Value VisitString(const string_t &str) {
		return Value(str);
	}
	static Value VisitBlob(const string_t &str) {
		return Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	static Value VisitBignum(const string_t &str) {
		return Value::BIGNUM(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	static Value VisitGeometry(const string_t &str) {
		return Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	static Value VisitBitstring(const string_t &str) {
		return Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize());
	}

	template <typename T>
	static Value VisitDecimal(T val, uint32_t width, uint32_t scale) {
		if (std::is_same<T, int16_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int32_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int64_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, hugeint_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else {
			throw InternalException("Unhandled decimal type");
		}
	}

	static Value VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data) {
		auto array_items = VariantVisitor<ValueConverter>::VisitArrayItems(variant, row, nested_data);
		return Value::LIST(LogicalType::VARIANT(), std::move(array_items));
	}

	static Value VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data) {
		auto object_children = VariantVisitor<ValueConverter>::VisitObjectItems(variant, row, nested_data);
		return Value::STRUCT(std::move(object_children));
	}

	static Value VisitDefault(VariantLogicalType type_id, const_data_ptr_t) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

template <>
Value ValueConverter::VisitInteger<int8_t>(int8_t val) {
	return Value::TINYINT(val);
}

template <>
Value ValueConverter::VisitInteger<int16_t>(int16_t val) {
	return Value::SMALLINT(val);
}

template <>
Value ValueConverter::VisitInteger<int32_t>(int32_t val) {
	return Value::INTEGER(val);
}

template <>
Value ValueConverter::VisitInteger<int64_t>(int64_t val) {
	return Value::BIGINT(val);
}

template <>
Value ValueConverter::VisitInteger<hugeint_t>(hugeint_t val) {
	return Value::HUGEINT(val);
}

template <>
Value ValueConverter::VisitInteger<uint8_t>(uint8_t val) {
	return Value::UTINYINT(val);
}

template <>
Value ValueConverter::VisitInteger<uint16_t>(uint16_t val) {
	return Value::USMALLINT(val);
}

template <>
Value ValueConverter::VisitInteger<uint32_t>(uint32_t val) {
	return Value::UINTEGER(val);
}

template <>
Value ValueConverter::VisitInteger<uint64_t>(uint64_t val) {
	return Value::UBIGINT(val);
}

template <>
Value ValueConverter::VisitInteger<uhugeint_t>(uhugeint_t val) {
	return Value::UHUGEINT(val);
}

} // namespace

Value VariantUtils::ConvertVariantToValue(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_idx) {
	return VariantVisitor<ValueConverter>::Visit(variant, row, values_idx);
}

void VariantUtils::FinalizeVariantKeys(Vector &variant, OrderedOwningStringMap<uint32_t> &dictionary,
                                       SelectionVector &sel, idx_t sel_size) {
	auto &keys = VariantVector::GetKeys(variant);
	auto &keys_entry = ListVector::GetEntry(keys);
	auto keys_entry_data = FlatVector::GetData<string_t>(keys_entry);

	bool already_sorted = true;

	vector<uint32_t> unsorted_to_sorted(dictionary.size());
	auto it = dictionary.begin();
	for (uint32_t sorted_idx = 0; sorted_idx < dictionary.size(); sorted_idx++) {
		auto unsorted_idx = it->second;
		if (unsorted_idx != sorted_idx) {
			already_sorted = false;
		}
		unsorted_to_sorted[unsorted_idx] = sorted_idx;
		D_ASSERT(sorted_idx < ListVector::GetListSize(keys));
		keys_entry_data[sorted_idx] = it->first;
		auto size = static_cast<uint32_t>(keys_entry_data[sorted_idx].GetSize());
		keys_entry_data[sorted_idx].SetSizeAndFinalize(size, size);
		it++;
	}

	if (!already_sorted) {
		//! Adjust the selection vector to point to the right dictionary index
		for (idx_t i = 0; i < sel_size; i++) {
			auto &entry = sel[i];
			auto sorted_idx = unsorted_to_sorted[entry];
			entry = sorted_idx;
		}
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
