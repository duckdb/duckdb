#include "reader/variant/variant_value.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

void VariantValue::AddChild(const string &key, VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::OBJECT);
	object_children.emplace(key, std::move(val));
}

void VariantValue::AddItem(VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	array_items.push_back(std::move(val));
}

namespace {

struct VariantConversionOffsets {
	vector<uint32_t> keys;
	vector<uint32_t> children;
	vector<uint32_t> values;
	vector<uint32_t> data;
};

struct StringDictionary {
public:
	uint32_t AddString(Vector &list, string_t str) {
		auto it = dictionary.find(str);

		if (it != dictionary.end()) {
			return it->second;
		}
		auto dict_count = NumericCast<uint32_t>(dictionary.size());
		if (dict_count >= dictionary_capacity) {
			auto new_capacity = NextPowerOfTwo(dictionary_capacity + 1);
			ListVector::Reserve(list, new_capacity);
			dictionary_capacity = new_capacity;
		}
		auto &list_entry = ListVector::GetEntry(list);
		auto vec_data = FlatVector::GetData<string_t>(list_entry);
		vec_data[dict_count] = StringVector::AddStringOrBlob(list_entry, str);
		dictionary.emplace(vec_data[dict_count], dict_count);
		return dict_count;
	}

	idx_t Size() const {
		return dictionary.size();
	}

	uint32_t Find(string_t str) {
		auto it = dictionary.find(str);
		//! The dictionary was populated in the first pass, looked up in the second
		D_ASSERT(it != dictionary.end());
		return it->second;
	}

public:
	//! Ensure uniqueness of the dictionary entries
	string_map_t<uint32_t> dictionary;
	idx_t dictionary_capacity = STANDARD_VECTOR_SIZE;
};

struct VariantVectorData {
public:
	explicit VariantVectorData(Vector &variant)
	    : key_id_validity(FlatVector::Validity(VariantVector::GetChildrenKeyId(variant))),
	      keys(VariantVector::GetKeys(variant)) {
		blob_data = FlatVector::GetData<string_t>(VariantVector::GetData(variant));
		type_ids_data = FlatVector::GetData<uint8_t>(VariantVector::GetValuesTypeId(variant));
		byte_offset_data = FlatVector::GetData<uint32_t>(VariantVector::GetValuesByteOffset(variant));
		key_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenKeyId(variant));
		value_id_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenValueId(variant));
		values_data = FlatVector::GetData<list_entry_t>(VariantVector::GetValues(variant));
		children_data = FlatVector::GetData<list_entry_t>(VariantVector::GetChildren(variant));
		keys_data = FlatVector::GetData<list_entry_t>(keys);
	}

public:
	//! value
	string_t *blob_data;

	//! values
	uint8_t *type_ids_data;
	uint32_t *byte_offset_data;

	//! children
	uint32_t *key_id_data;
	uint32_t *value_id_data;
	ValidityMask &key_id_validity;

	//! values | children | keys
	list_entry_t *values_data;
	list_entry_t *children_data;
	list_entry_t *keys_data;

	Vector &keys;
};

} // namespace

static void AnalyzeValue(const VariantValue &value, idx_t row, VariantConversionOffsets &offsets) {
	offsets.values[row]++;
	switch (value.value_type) {
	case VariantValueType::OBJECT: {
		//! Write the count of the children
		auto &children = value.object_children;
		offsets.data[row] += GetVarintSize(children.size());
		if (!children.empty()) {
			//! Write the children offset
			offsets.data[row] += GetVarintSize(offsets.children[row]);
			offsets.children[row] += children.size();
			offsets.keys[row] += children.size();
			for (auto &child : children) {
				auto &child_value = child.second;
				AnalyzeValue(child_value, row, offsets);
			}
		}
		break;
	}
	case VariantValueType::ARRAY: {
		//! Write the count of the children
		auto &children = value.array_items;
		offsets.data[row] += GetVarintSize(children.size());
		if (!children.empty()) {
			//! Write the children offset
			offsets.data[row] += GetVarintSize(offsets.children[row]);
			offsets.children[row] += children.size();
			for (auto &child : children) {
				AnalyzeValue(child, row, offsets);
			}
		}
		break;
	}
	case VariantValueType::PRIMITIVE: {
		auto &primitive = value.primitive_value;
		auto type_id = primitive.type().id();
		switch (type_id) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::SQLNULL: {
			break;
		}
		case LogicalTypeId::TINYINT: {
			offsets.data[row] += sizeof(int8_t);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			offsets.data[row] += sizeof(int16_t);
			break;
		}
		case LogicalTypeId::INTEGER: {
			offsets.data[row] += sizeof(int32_t);
			break;
		}
		case LogicalTypeId::BIGINT: {
			offsets.data[row] += sizeof(int64_t);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			offsets.data[row] += sizeof(double);
			break;
		}
		case LogicalTypeId::FLOAT: {
			offsets.data[row] += sizeof(float);
			break;
		}
		case LogicalTypeId::DATE: {
			offsets.data[row] += sizeof(date_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			offsets.data[row] += sizeof(timestamp_tz_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			offsets.data[row] += sizeof(timestamp_t);
			break;
		}
		case LogicalTypeId::TIME: {
			offsets.data[row] += sizeof(dtime_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			offsets.data[row] += sizeof(timestamp_ns_t);
			break;
		}
		case LogicalTypeId::UUID: {
			offsets.data[row] += sizeof(hugeint_t);
			break;
		}
		case LogicalTypeId::DECIMAL: {
			auto &type = primitive.type();
			uint8_t width;
			uint8_t scale;
			type.GetDecimalProperties(width, scale);

			auto physical_type = type.InternalType();
			offsets.data[row] += GetVarintSize(width);
			offsets.data[row] += GetVarintSize(scale);
			switch (physical_type) {
			case PhysicalType::INT32: {
				offsets.data[row] += sizeof(int32_t);
				break;
			}
			case PhysicalType::INT64: {
				offsets.data[row] += sizeof(int64_t);
				break;
			}
			case PhysicalType::INT128: {
				offsets.data[row] += sizeof(hugeint_t);
				break;
			}
			default:
				throw InternalException("Unexpected physical type for Decimal value: %s",
				                        EnumUtil::ToString(physical_type));
			}
			break;
		}
		case LogicalTypeId::BLOB:
		case LogicalTypeId::VARCHAR: {
			auto string_data = primitive.GetValueUnsafe<string_t>();
			offsets.data[row] += GetVarintSize(string_data.GetSize());
			offsets.data[row] += string_data.GetSize();
			break;
		}
		default:
			throw InternalException("Encountered unrecognized LogicalType in VariantValue::AnalyzeValue: %s",
			                        primitive.type().ToString());
		}
		break;
	}
	default:
		throw InternalException("VariantValueType not handled");
	}
}

static void ConvertValue(const VariantValue &value, VariantVectorData &result, idx_t row,
                         VariantConversionOffsets &offsets, SelectionVector &keys_selvec,
                         StringDictionary &dictionary) {
	auto blob_data = data_ptr_cast(result.blob_data[row].GetDataWriteable());
	auto keys_offset = result.keys_data[row].offset;
	auto children_offset = result.children_data[row].offset;
	auto values_offset = result.values_data[row].offset;

	switch (value.value_type) {
	case VariantValueType::OBJECT: {
		//! Write the count of the children
		auto &children = value.object_children;

		//! values
		result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::OBJECT);
		result.byte_offset_data[values_offset + offsets.values[row]] = offsets.data[row];
		offsets.values[row]++;

		//! data
		VarintEncode<uint32_t>(children.size(), blob_data + offsets.data[row]);
		offsets.data[row] += GetVarintSize(children.size());

		if (!children.empty()) {
			//! Write the children offset
			VarintEncode<uint32_t>(offsets.children[row], blob_data + offsets.data[row]);
			offsets.data[row] += GetVarintSize(offsets.children[row]);

			auto start_of_children = offsets.children[row];
			offsets.children[row] += children.size();

			auto it = children.begin();
			for (idx_t i = 0; i < children.size(); i++) {
				//! children
				result.key_id_data[children_offset + start_of_children + i] = offsets.keys[row];
				result.value_id_data[children_offset + start_of_children + i] = offsets.values[row];

				auto &child = *it;
				//! keys
				auto &child_key = child.first;
				auto dictionary_index =
				    dictionary.AddString(result.keys, string_t(child_key.c_str(), child_key.size()));
				keys_selvec.set_index(keys_offset + offsets.keys[row], dictionary_index);
				offsets.keys[row]++;

				auto &child_value = child.second;
				ConvertValue(child_value, result, row, offsets, keys_selvec, dictionary);
				it++;
			}
		}
		break;
	}
	case VariantValueType::ARRAY: {
		//! Write the count of the children
		auto &children = value.array_items;

		//! values
		result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::ARRAY);
		result.byte_offset_data[values_offset + offsets.values[row]] = offsets.data[row];
		offsets.values[row]++;

		//! data
		VarintEncode<uint32_t>(children.size(), blob_data + offsets.data[row]);
		offsets.data[row] += GetVarintSize(children.size());

		if (!children.empty()) {
			//! Write the children offset
			VarintEncode<uint32_t>(offsets.children[row], blob_data + offsets.data[row]);
			offsets.data[row] += GetVarintSize(offsets.children[row]);

			auto start_of_children = offsets.children[row];
			offsets.children[row] += children.size();

			for (idx_t i = 0; i < children.size(); i++) {
				//! children
				result.key_id_data[children_offset + start_of_children + i] = offsets.keys[row];
				result.value_id_data[children_offset + start_of_children + i] = offsets.values[row];

				auto &child_value = children[i];
				ConvertValue(child_value, result, row, offsets, keys_selvec, dictionary);
			}
		}
		break;
	}
	case VariantValueType::PRIMITIVE: {
		auto &primitive = value.primitive_value;
		auto type_id = primitive.type().id();
		result.byte_offset_data[values_offset + offsets.values[row]] = offsets.data[row];
		switch (type_id) {
		case LogicalTypeId::BOOLEAN: {
			if (primitive.GetValue<bool>()) {
				result.type_ids_data[values_offset + offsets.values[row]] =
				    static_cast<uint8_t>(VariantLogicalType::BOOL_TRUE);
			} else {
				result.type_ids_data[values_offset + offsets.values[row]] =
				    static_cast<uint8_t>(VariantLogicalType::BOOL_FALSE);
			}
			break;
		}
		case LogicalTypeId::SQLNULL: {
			result.type_ids_data[values_offset + offsets.values[row]] =
			    static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL);
			break;
		}
		case LogicalTypeId::TINYINT: {
			result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::INT8);
			Store(primitive.GetValueUnsafe<int8_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(int8_t);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::INT16);
			Store(primitive.GetValueUnsafe<int16_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(int16_t);
			break;
		}
		case LogicalTypeId::INTEGER: {
			result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::INT32);
			Store(primitive.GetValueUnsafe<int32_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(int32_t);
			break;
		}
		case LogicalTypeId::BIGINT: {
			result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::INT64);
			Store(primitive.GetValueUnsafe<int64_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(int64_t);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			result.type_ids_data[values_offset + offsets.values[row]] =
			    static_cast<uint8_t>(VariantLogicalType::DOUBLE);
			Store(primitive.GetValueUnsafe<double>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(double);
			break;
		}
		case LogicalTypeId::FLOAT: {
			result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::FLOAT);
			Store(primitive.GetValueUnsafe<float>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(float);
			break;
		}
		case LogicalTypeId::DATE: {
			result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::DATE);
			Store(primitive.GetValueUnsafe<date_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(date_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			result.type_ids_data[values_offset + offsets.values[row]] =
			    static_cast<uint8_t>(VariantLogicalType::TIMESTAMP_MICROS_TZ);
			Store(primitive.GetValueUnsafe<timestamp_tz_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(timestamp_tz_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			result.type_ids_data[values_offset + offsets.values[row]] =
			    static_cast<uint8_t>(VariantLogicalType::TIMESTAMP_MICROS);
			Store(primitive.GetValueUnsafe<timestamp_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(timestamp_t);
			break;
		}
		case LogicalTypeId::TIME: {
			result.type_ids_data[values_offset + offsets.values[row]] =
			    static_cast<uint8_t>(VariantLogicalType::TIME_MICROS);
			Store(primitive.GetValueUnsafe<dtime_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(dtime_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			result.type_ids_data[values_offset + offsets.values[row]] =
			    static_cast<uint8_t>(VariantLogicalType::TIMESTAMP_NANOS);
			Store(primitive.GetValueUnsafe<timestamp_ns_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(timestamp_ns_t);
			break;
		}
		case LogicalTypeId::UUID: {
			result.type_ids_data[values_offset + offsets.values[row]] = static_cast<uint8_t>(VariantLogicalType::UUID);
			Store(primitive.GetValueUnsafe<hugeint_t>(), blob_data + offsets.data[row]);
			offsets.data[row] += sizeof(hugeint_t);
			break;
		}
		case LogicalTypeId::DECIMAL: {
			result.type_ids_data[values_offset + offsets.values[row]] =
			    static_cast<uint8_t>(VariantLogicalType::DECIMAL);
			auto &type = primitive.type();
			uint8_t width;
			uint8_t scale;
			type.GetDecimalProperties(width, scale);

			auto physical_type = type.InternalType();
			VarintEncode<uint32_t>(width, blob_data + offsets.data[row]);
			offsets.data[row] += GetVarintSize(width);
			VarintEncode<uint32_t>(scale, blob_data + offsets.data[row]);
			offsets.data[row] += GetVarintSize(scale);
			switch (physical_type) {
			case PhysicalType::INT32: {
				Store(primitive.GetValueUnsafe<int32_t>(), blob_data + offsets.data[row]);
				offsets.data[row] += sizeof(int32_t);
				break;
			}
			case PhysicalType::INT64: {
				Store(primitive.GetValueUnsafe<int64_t>(), blob_data + offsets.data[row]);
				offsets.data[row] += sizeof(int64_t);
				break;
			}
			case PhysicalType::INT128: {
				Store(primitive.GetValueUnsafe<hugeint_t>(), blob_data + offsets.data[row]);
				offsets.data[row] += sizeof(hugeint_t);
				break;
			}
			default:
				throw InternalException("Unexpected physical type for Decimal value: %s",
				                        EnumUtil::ToString(physical_type));
			}
			break;
		}
		case LogicalTypeId::BLOB:
		case LogicalTypeId::VARCHAR: {
			if (type_id == LogicalTypeId::BLOB) {
				result.type_ids_data[values_offset + offsets.values[row]] =
				    static_cast<uint8_t>(VariantLogicalType::BLOB);
			} else {
				result.type_ids_data[values_offset + offsets.values[row]] =
				    static_cast<uint8_t>(VariantLogicalType::VARCHAR);
			}
			auto string_data = primitive.GetValueUnsafe<string_t>();
			auto string_size = string_data.GetSize();
			VarintEncode<uint32_t>(string_size, blob_data + offsets.data[row]);
			offsets.data[row] += GetVarintSize(string_size);
			memcpy(blob_data + offsets.data[row], string_data.GetData(), string_size);
			offsets.data[row] += string_size;
			break;
		}
		default:
			throw InternalException("Encountered unrecognized LogicalType in VariantValue::AnalyzeValue: %s",
			                        primitive.type().ToString());
		}
		offsets.values[row]++;
		break;
	}
	default:
		throw InternalException("VariantValueType not handled");
	}
}

//! Copied and modified from 'to_variant.cpp'
static void InitializeVariants(VariantConversionOffsets &offsets, Vector &result, SelectionVector &keys_selvec,
                               idx_t &selvec_size) {
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = ListVector::GetData(keys);

	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	idx_t children_offset = 0;
	idx_t values_offset = 0;
	idx_t keys_offset = 0;

	auto keys_sizes = offsets.keys;
	auto children_sizes = offsets.children;
	auto values_sizes = offsets.values;
	auto blob_sizes = offsets.data;

	auto count = values_sizes.size();
	for (idx_t i = 0; i < count; i++) {
		auto &keys_entry = keys_data[i];
		auto &children_entry = children_data[i];
		auto &values_entry = values_data[i];

		//! keys
		keys_entry.length = offsets.keys[i];
		keys_entry.offset = keys_offset;
		keys_offset += keys_entry.length;

		//! children
		children_entry.length = children_sizes[i];
		children_entry.offset = children_offset;
		children_offset += children_entry.length;

		//! values
		values_entry.length = values_sizes[i];
		values_entry.offset = values_offset;
		values_offset += values_entry.length;

		//! value
		blob_data[i] = StringVector::EmptyString(blob, blob_sizes[i]);
	}

	//! Reserve for the children of the lists
	ListVector::Reserve(keys, keys_offset);
	ListVector::Reserve(children, children_offset);
	ListVector::Reserve(values, values_offset);

	//! Set list sizes
	ListVector::SetListSize(keys, keys_offset);
	ListVector::SetListSize(children, children_offset);
	ListVector::SetListSize(values, values_offset);

	keys_selvec.Initialize(keys_offset);
	//! NOTE: we initialize these to 0 because some rows will not set them, if the row is NULL
	//! To simplify the implementation we just allocate 'dictionary.Size()' keys for each row
	for (idx_t i = 0; i < keys_offset; i++) {
		keys_selvec.set_index(i, 0);
	}
	selvec_size = keys_offset;
}

void VariantValue::ToVARIANT(vector<VariantValue> &input, Vector &result) {
	auto count = input.size();

	//! Keep track of all the offsets for each row.
	VariantConversionOffsets offsets;
	offsets.keys.resize(count);
	offsets.children.resize(count);
	offsets.values.resize(count);
	offsets.data.resize(count);

	for (idx_t i = 0; i < count; i++) {
		auto &value = input[i];
		AnalyzeValue(value, i, offsets);
	}

	SelectionVector keys_selvec;
	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size);
	StringDictionary dictionary;

	//! Reset the offsets
	memset(offsets.keys.data(), 0, sizeof(uint32_t) * count);
	memset(offsets.children.data(), 0, sizeof(uint32_t) * count);
	memset(offsets.values.data(), 0, sizeof(uint32_t) * count);
	memset(offsets.data.data(), 0, sizeof(uint32_t) * count);
	VariantVectorData variant_data(result);
	for (idx_t i = 0; i < count; i++) {
		auto &value = input[i];
		ConvertValue(value, variant_data, i, offsets, keys_selvec, dictionary);
	}

	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);
	VariantUtils::SortVariantKeys(keys_entry, dictionary.Size(), keys_selvec, keys_selvec_size);

	keys_entry.Slice(keys_selvec, keys_selvec_size);
	keys_entry.Flatten(keys_selvec_size);
}

yyjson_mut_val *VariantValue::ToJSON(ClientContext &context, yyjson_mut_doc *doc) const {
	switch (value_type) {
	case VariantValueType::PRIMITIVE: {
		if (primitive_value.IsNull()) {
			return yyjson_mut_null(doc);
		}
		switch (primitive_value.type().id()) {
		case LogicalTypeId::BOOLEAN: {
			if (primitive_value.GetValue<bool>()) {
				return yyjson_mut_true(doc);
			} else {
				return yyjson_mut_false(doc);
			}
		}
		case LogicalTypeId::TINYINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int8_t>());
		case LogicalTypeId::SMALLINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int16_t>());
		case LogicalTypeId::INTEGER:
			return yyjson_mut_int(doc, primitive_value.GetValue<int32_t>());
		case LogicalTypeId::BIGINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int64_t>());
		case LogicalTypeId::FLOAT:
			return yyjson_mut_real(doc, primitive_value.GetValue<float>());
		case LogicalTypeId::DOUBLE:
			return yyjson_mut_real(doc, primitive_value.GetValue<double>());
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::VARCHAR: {
			auto value_str = primitive_value.ToString();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP: {
			auto value_str = primitive_value.ToString();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		default:
			throw InternalException("Unexpected primitive type: %s", primitive_value.type().ToString());
		}
	}
	case VariantValueType::OBJECT: {
		auto obj = yyjson_mut_obj(doc);
		for (const auto &it : object_children) {
			auto &key = it.first;
			auto value = it.second.ToJSON(context, doc);
			yyjson_mut_obj_add_val(doc, obj, key.c_str(), value);
		}
		return obj;
	}
	case VariantValueType::ARRAY: {
		auto arr = yyjson_mut_arr(doc);
		for (auto &item : array_items) {
			auto value = item.ToJSON(context, doc);
			yyjson_mut_arr_add_val(arr, value);
		}
		return arr;
	}
	default:
		throw InternalException("Can't serialize this VariantValue type to JSON");
	}
}

} // namespace duckdb
