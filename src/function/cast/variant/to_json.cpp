#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//! ------------ Variant -> JSON ------------

yyjson_mut_val *VariantCasts::ConvertVariantToJSON(yyjson_mut_doc *doc, const RecursiveUnifiedVectorFormat &source,
                                                   idx_t row, uint32_t values_idx) {
	auto index = source.unified.sel->get_index(row);
	if (!source.unified.validity.RowIsValid(index)) {
		return yyjson_mut_null(doc);
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

	//! values_index
	auto &values_index = UnifiedVariantVector::GetChildrenValuesIndex(source);
	auto values_index_data = values_index.GetData<uint32_t>(values_index);

	//! keys_index
	auto &keys_index = UnifiedVariantVector::GetChildrenKeysIndex(source);
	auto keys_index_data = keys_index.GetData<uint32_t>(keys_index);

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
		return yyjson_mut_null(doc);
	case VariantLogicalType::BOOL_TRUE:
		return yyjson_mut_true(doc);
	case VariantLogicalType::BOOL_FALSE:
		return yyjson_mut_false(doc);
	case VariantLogicalType::INT8: {
		auto val = Load<int8_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT16: {
		auto val = Load<int16_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT32: {
		auto val = Load<int32_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT64: {
		auto val = Load<int64_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::INT128: {
		auto val = Load<hugeint_t>(ptr);
		auto val_str = val.ToString();
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::UINT8: {
		auto val = Load<uint8_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::UINT16: {
		auto val = Load<uint16_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::UINT32: {
		auto val = Load<uint32_t>(ptr);
		return yyjson_mut_sint(doc, val);
	}
	case VariantLogicalType::UINT64: {
		auto val = Load<uint64_t>(ptr);
		return yyjson_mut_uint(doc, val);
	}
	case VariantLogicalType::UINT128: {
		auto val = Load<uhugeint_t>(ptr);
		auto val_str = val.ToString();
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::UUID: {
		auto val = Value::UUID(Load<hugeint_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::INTERVAL: {
		auto val = Value::INTERVAL(Load<interval_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::FLOAT: {
		auto val = Load<float>(ptr);
		return yyjson_mut_real(doc, val);
	}
	case VariantLogicalType::DOUBLE: {
		auto val = Load<double>(ptr);
		return yyjson_mut_real(doc, val);
	}
	case VariantLogicalType::DATE: {
		auto val = Load<int32_t>(ptr);
		auto val_str = Date::ToString(date_t(val));
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::BLOB: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		auto val_str = Value::BLOB(const_data_ptr_cast(string_data), string_length).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::VARCHAR: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		return yyjson_mut_strncpy(doc, string_data, static_cast<size_t>(string_length));
	}
	case VariantLogicalType::DECIMAL: {
		auto width = NumericCast<uint8_t>(VarintDecode<idx_t>(ptr));
		auto scale = NumericCast<uint8_t>(VarintDecode<idx_t>(ptr));

		string val_str;
		if (width > DecimalWidth<int64_t>::max) {
			val_str = Decimal::ToString(Load<hugeint_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int32_t>::max) {
			val_str = Decimal::ToString(Load<int64_t>(ptr), width, scale);
		} else if (width > DecimalWidth<int16_t>::max) {
			val_str = Decimal::ToString(Load<int32_t>(ptr), width, scale);
		} else {
			val_str = Decimal::ToString(Load<int16_t>(ptr), width, scale);
		}
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIME_MICROS: {
		auto val = Load<dtime_t>(ptr);
		auto val_str = Time::ToString(val);
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIME_NANOS: {
		auto val = Value::TIME_NS(Load<dtime_ns_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIME_MICROS_TZ: {
		auto val = Value::TIMETZ(Load<dtime_tz_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_MICROS: {
		auto val = Load<timestamp_t>(ptr);
		auto val_str = Timestamp::ToString(val);
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_SEC: {
		auto val = Value::TIMESTAMPSEC(Load<timestamp_sec_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_NANOS: {
		auto val = Value::TIMESTAMPNS(Load<timestamp_ns_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_MILIS: {
		auto val = Value::TIMESTAMPMS(Load<timestamp_ms_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::TIMESTAMP_MICROS_TZ: {
		auto val = Value::TIMESTAMPTZ(Load<timestamp_tz_t>(ptr));
		auto val_str = val.ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::ARRAY: {
		auto count = VarintDecode<uint32_t>(ptr);
		auto arr = yyjson_mut_arr(doc);
		if (!count) {
			return arr;
		}
		auto child_index_start = VarintDecode<uint32_t>(ptr);
		for (idx_t i = 0; i < count; i++) {
			auto index = values_index.sel->get_index(children_list_entry.offset + child_index_start + i);
			auto child_index = values_index_data[index];
#ifdef DEBUG
			auto key_id_index = keys_index.sel->get_index(children_list_entry.offset + child_index_start + i);
			D_ASSERT(!keys_index.validity.RowIsValid(key_id_index));
#endif
			auto val = ConvertVariantToJSON(doc, source, row, child_index);
			if (!val) {
				return nullptr;
			}
			yyjson_mut_arr_add_val(arr, val);
		}
		return arr;
	}
	case VariantLogicalType::OBJECT: {
		auto count = VarintDecode<uint32_t>(ptr);
		auto obj = yyjson_mut_obj(doc);
		if (!count) {
			return obj;
		}
		auto child_index_start = VarintDecode<uint32_t>(ptr);

		for (idx_t i = 0; i < count; i++) {
			auto children_index = values_index.sel->get_index(children_list_entry.offset + child_index_start + i);
			auto child_value_idx = values_index_data[children_index];
			auto val = ConvertVariantToJSON(doc, source, row, child_value_idx);
			if (!val) {
				return nullptr;
			}
			auto keys_index_index = keys_index.sel->get_index(children_list_entry.offset + child_index_start + i);
			D_ASSERT(keys_index.validity.RowIsValid(keys_index_index));
			auto child_key_id = keys_index_data[keys_index_index];
			auto &key = keys_entry_data[keys_entry.sel->get_index(keys_list_entry.offset + child_key_id)];
			yyjson_mut_obj_put(obj, yyjson_mut_strncpy(doc, key.GetData(), key.GetSize()), val);
		}
		return obj;
	}
	case VariantLogicalType::BITSTRING: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		auto val_str = Value::BIT(const_data_ptr_cast(string_data), string_length).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}
	case VariantLogicalType::BIGNUM: {
		auto string_length = VarintDecode<uint32_t>(ptr);
		auto string_data = reinterpret_cast<const char *>(ptr);
		auto val_str = Value::BIGNUM(const_data_ptr_cast(string_data), string_length).ToString();
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}
	default:
		throw InternalException("VariantLogicalType(%d) not handled", static_cast<uint8_t>(type_id));
	}

	return nullptr;
}

} // namespace duckdb
