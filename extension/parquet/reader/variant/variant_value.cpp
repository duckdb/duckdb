#include "reader/variant/variant_value.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/enum_util.hpp"
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
#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {

void VariantValue::AddChild(const string &key, VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::OBJECT);
	object_children.emplace(key, std::move(val));
}

void VariantValue::AddItem(VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	array_items.push_back(std::move(val));
}

static void InitializeOffsets(DataChunk &offsets, idx_t count) {
	auto keys = variant::OffsetData::GetKeys(offsets);
	auto children = variant::OffsetData::GetChildren(offsets);
	auto values = variant::OffsetData::GetValues(offsets);
	auto blob = variant::OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		keys[i] = 0;
		children[i] = 0;
		values[i] = 0;
		blob[i] = 0;
	}
}

static void AnalyzeValue(const VariantValue &value, idx_t row, DataChunk &offsets) {
	auto &keys_offset = variant::OffsetData::GetKeys(offsets)[row];
	auto &children_offset = variant::OffsetData::GetChildren(offsets)[row];
	auto &values_offset = variant::OffsetData::GetValues(offsets)[row];
	auto &data_offset = variant::OffsetData::GetBlob(offsets)[row];

	values_offset++;
	switch (value.value_type) {
	case VariantValueType::OBJECT: {
		//! Write the count of the children
		auto &children = value.object_children;
		data_offset += GetVarintSize(children.size());
		if (!children.empty()) {
			//! Write the children offset
			data_offset += GetVarintSize(children_offset);
			children_offset += children.size();
			keys_offset += children.size();
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
		data_offset += GetVarintSize(children.size());
		if (!children.empty()) {
			//! Write the children offset
			data_offset += GetVarintSize(children_offset);
			children_offset += children.size();
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
			data_offset += sizeof(int8_t);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			data_offset += sizeof(int16_t);
			break;
		}
		case LogicalTypeId::INTEGER: {
			data_offset += sizeof(int32_t);
			break;
		}
		case LogicalTypeId::BIGINT: {
			data_offset += sizeof(int64_t);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			data_offset += sizeof(double);
			break;
		}
		case LogicalTypeId::FLOAT: {
			data_offset += sizeof(float);
			break;
		}
		case LogicalTypeId::DATE: {
			data_offset += sizeof(date_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			data_offset += sizeof(timestamp_tz_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			data_offset += sizeof(timestamp_t);
			break;
		}
		case LogicalTypeId::TIME: {
			data_offset += sizeof(dtime_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			data_offset += sizeof(timestamp_ns_t);
			break;
		}
		case LogicalTypeId::UUID: {
			data_offset += sizeof(hugeint_t);
			break;
		}
		case LogicalTypeId::DECIMAL: {
			auto &type = primitive.type();
			uint8_t width;
			uint8_t scale;
			type.GetDecimalProperties(width, scale);

			auto physical_type = type.InternalType();
			data_offset += GetVarintSize(width);
			data_offset += GetVarintSize(scale);
			switch (physical_type) {
			case PhysicalType::INT32: {
				data_offset += sizeof(int32_t);
				break;
			}
			case PhysicalType::INT64: {
				data_offset += sizeof(int64_t);
				break;
			}
			case PhysicalType::INT128: {
				data_offset += sizeof(hugeint_t);
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
			data_offset += GetVarintSize(string_data.GetSize());
			data_offset += string_data.GetSize();
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

uint32_t GetOrCreateIndex(OrderedOwningStringMap<uint32_t> &dictionary, const string_t &key) {
	auto unsorted_idx = dictionary.size();
	//! This will later be remapped to the sorted idx (see FinalizeVariantKeys in 'to_variant.cpp')
	return dictionary.emplace(std::make_pair(key, unsorted_idx)).first->second;
}

static void ConvertValue(const VariantValue &value, VariantVectorData &result, idx_t row, DataChunk &offsets,
                         SelectionVector &keys_selvec, OrderedOwningStringMap<uint32_t> &dictionary) {
	auto blob_data = data_ptr_cast(result.blob_data[row].GetDataWriteable());
	auto keys_list_offset = result.keys_data[row].offset;
	auto children_list_offset = result.children_data[row].offset;
	auto values_list_offset = result.values_data[row].offset;

	auto &keys_offset = variant::OffsetData::GetKeys(offsets)[row];
	auto &children_offset = variant::OffsetData::GetChildren(offsets)[row];
	auto &values_offset = variant::OffsetData::GetValues(offsets)[row];
	auto &data_offset = variant::OffsetData::GetBlob(offsets)[row];

	switch (value.value_type) {
	case VariantValueType::OBJECT: {
		//! Write the count of the children
		auto &children = value.object_children;

		//! values
		result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::OBJECT);
		result.byte_offset_data[values_list_offset + values_offset] = data_offset;
		values_offset++;

		//! data
		VarintEncode<uint32_t>(children.size(), blob_data + data_offset);
		data_offset += GetVarintSize(children.size());

		if (!children.empty()) {
			//! Write the children offset
			VarintEncode<uint32_t>(children_offset, blob_data + data_offset);
			data_offset += GetVarintSize(children_offset);

			auto start_of_children = children_offset;
			children_offset += children.size();

			auto it = children.begin();
			for (idx_t i = 0; i < children.size(); i++) {
				//! children
				result.keys_index_data[children_list_offset + start_of_children + i] = keys_offset;
				result.values_index_data[children_list_offset + start_of_children + i] = values_offset;

				auto &child = *it;
				//! keys
				auto &child_key = child.first;
				auto dictionary_index = GetOrCreateIndex(dictionary, child_key);
				keys_selvec.set_index(keys_list_offset + keys_offset, dictionary_index);
				keys_offset++;

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
		result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::ARRAY);
		result.byte_offset_data[values_list_offset + values_offset] = data_offset;
		values_offset++;

		//! data
		VarintEncode<uint32_t>(children.size(), blob_data + data_offset);
		data_offset += GetVarintSize(children.size());

		if (!children.empty()) {
			//! Write the children offset
			VarintEncode<uint32_t>(children_offset, blob_data + data_offset);
			data_offset += GetVarintSize(children_offset);

			auto start_of_children = children_offset;
			children_offset += children.size();

			for (idx_t i = 0; i < children.size(); i++) {
				//! children
				result.keys_index_validity.SetInvalid(children_list_offset + start_of_children + i);
				result.values_index_data[children_list_offset + start_of_children + i] = values_offset;

				auto &child_value = children[i];
				ConvertValue(child_value, result, row, offsets, keys_selvec, dictionary);
			}
		}
		break;
	}
	case VariantValueType::PRIMITIVE: {
		auto &primitive = value.primitive_value;
		auto type_id = primitive.type().id();
		result.byte_offset_data[values_list_offset + values_offset] = data_offset;
		switch (type_id) {
		case LogicalTypeId::BOOLEAN: {
			if (primitive.GetValue<bool>()) {
				result.type_ids_data[values_list_offset + values_offset] =
				    static_cast<uint8_t>(VariantLogicalType::BOOL_TRUE);
			} else {
				result.type_ids_data[values_list_offset + values_offset] =
				    static_cast<uint8_t>(VariantLogicalType::BOOL_FALSE);
			}
			break;
		}
		case LogicalTypeId::SQLNULL: {
			result.type_ids_data[values_list_offset + values_offset] =
			    static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL);
			break;
		}
		case LogicalTypeId::TINYINT: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::INT8);
			Store(primitive.GetValueUnsafe<int8_t>(), blob_data + data_offset);
			data_offset += sizeof(int8_t);
			break;
		}
		case LogicalTypeId::SMALLINT: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::INT16);
			Store(primitive.GetValueUnsafe<int16_t>(), blob_data + data_offset);
			data_offset += sizeof(int16_t);
			break;
		}
		case LogicalTypeId::INTEGER: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::INT32);
			Store(primitive.GetValueUnsafe<int32_t>(), blob_data + data_offset);
			data_offset += sizeof(int32_t);
			break;
		}
		case LogicalTypeId::BIGINT: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::INT64);
			Store(primitive.GetValueUnsafe<int64_t>(), blob_data + data_offset);
			data_offset += sizeof(int64_t);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::DOUBLE);
			Store(primitive.GetValueUnsafe<double>(), blob_data + data_offset);
			data_offset += sizeof(double);
			break;
		}
		case LogicalTypeId::FLOAT: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::FLOAT);
			Store(primitive.GetValueUnsafe<float>(), blob_data + data_offset);
			data_offset += sizeof(float);
			break;
		}
		case LogicalTypeId::DATE: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::DATE);
			Store(primitive.GetValueUnsafe<date_t>(), blob_data + data_offset);
			data_offset += sizeof(date_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			result.type_ids_data[values_list_offset + values_offset] =
			    static_cast<uint8_t>(VariantLogicalType::TIMESTAMP_MICROS_TZ);
			Store(primitive.GetValueUnsafe<timestamp_tz_t>(), blob_data + data_offset);
			data_offset += sizeof(timestamp_tz_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			result.type_ids_data[values_list_offset + values_offset] =
			    static_cast<uint8_t>(VariantLogicalType::TIMESTAMP_MICROS);
			Store(primitive.GetValueUnsafe<timestamp_t>(), blob_data + data_offset);
			data_offset += sizeof(timestamp_t);
			break;
		}
		case LogicalTypeId::TIME: {
			result.type_ids_data[values_list_offset + values_offset] =
			    static_cast<uint8_t>(VariantLogicalType::TIME_MICROS);
			Store(primitive.GetValueUnsafe<dtime_t>(), blob_data + data_offset);
			data_offset += sizeof(dtime_t);
			break;
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			result.type_ids_data[values_list_offset + values_offset] =
			    static_cast<uint8_t>(VariantLogicalType::TIMESTAMP_NANOS);
			Store(primitive.GetValueUnsafe<timestamp_ns_t>(), blob_data + data_offset);
			data_offset += sizeof(timestamp_ns_t);
			break;
		}
		case LogicalTypeId::UUID: {
			result.type_ids_data[values_list_offset + values_offset] = static_cast<uint8_t>(VariantLogicalType::UUID);
			Store(primitive.GetValueUnsafe<hugeint_t>(), blob_data + data_offset);
			data_offset += sizeof(hugeint_t);
			break;
		}
		case LogicalTypeId::DECIMAL: {
			result.type_ids_data[values_list_offset + values_offset] =
			    static_cast<uint8_t>(VariantLogicalType::DECIMAL);
			auto &type = primitive.type();
			uint8_t width;
			uint8_t scale;
			type.GetDecimalProperties(width, scale);

			auto physical_type = type.InternalType();
			VarintEncode<uint32_t>(width, blob_data + data_offset);
			data_offset += GetVarintSize(width);
			VarintEncode<uint32_t>(scale, blob_data + data_offset);
			data_offset += GetVarintSize(scale);
			switch (physical_type) {
			case PhysicalType::INT32: {
				Store(primitive.GetValueUnsafe<int32_t>(), blob_data + data_offset);
				data_offset += sizeof(int32_t);
				break;
			}
			case PhysicalType::INT64: {
				Store(primitive.GetValueUnsafe<int64_t>(), blob_data + data_offset);
				data_offset += sizeof(int64_t);
				break;
			}
			case PhysicalType::INT128: {
				Store(primitive.GetValueUnsafe<hugeint_t>(), blob_data + data_offset);
				data_offset += sizeof(hugeint_t);
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
				result.type_ids_data[values_list_offset + values_offset] =
				    static_cast<uint8_t>(VariantLogicalType::BLOB);
			} else {
				result.type_ids_data[values_list_offset + values_offset] =
				    static_cast<uint8_t>(VariantLogicalType::VARCHAR);
			}
			auto string_data = primitive.GetValueUnsafe<string_t>();
			auto string_size = string_data.GetSize();
			VarintEncode<uint32_t>(string_size, blob_data + data_offset);
			data_offset += GetVarintSize(string_size);
			memcpy(blob_data + data_offset, string_data.GetData(), string_size);
			data_offset += string_size;
			break;
		}
		default:
			throw InternalException("Encountered unrecognized LogicalType in VariantValue::AnalyzeValue: %s",
			                        primitive.type().ToString());
		}
		values_offset++;
		break;
	}
	default:
		throw InternalException("VariantValueType not handled");
	}
}

//! Copied and modified from 'to_variant.cpp'
static void InitializeVariants(DataChunk &offsets, Vector &result, SelectionVector &keys_selvec, idx_t &selvec_size) {
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

	auto keys_sizes = variant::OffsetData::GetKeys(offsets);
	auto children_sizes = variant::OffsetData::GetChildren(offsets);
	auto values_sizes = variant::OffsetData::GetValues(offsets);
	auto blob_sizes = variant::OffsetData::GetBlob(offsets);

	auto count = offsets.size();
	for (idx_t i = 0; i < count; i++) {
		auto &keys_entry = keys_data[i];
		auto &children_entry = children_data[i];
		auto &values_entry = values_data[i];

		//! keys
		keys_entry.length = keys_sizes[i];
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
	selvec_size = keys_offset;
}

void VariantValue::ToVARIANT(vector<VariantValue> &input, Vector &result) {
	auto count = input.size();
	if (input.empty()) {
		return;
	}

	//! Keep track of all the offsets for each row.
	DataChunk analyze_offsets;
	analyze_offsets.Initialize(
	    Allocator::DefaultAllocator(),
	    {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER}, count);
	analyze_offsets.SetCardinality(count);
	InitializeOffsets(analyze_offsets, count);

	for (idx_t i = 0; i < count; i++) {
		auto &value = input[i];
		if (value.IsNull()) {
			continue;
		}
		AnalyzeValue(value, i, analyze_offsets);
	}

	SelectionVector keys_selvec;
	idx_t keys_selvec_size;
	InitializeVariants(analyze_offsets, result, keys_selvec, keys_selvec_size);

	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());

	DataChunk conversion_offsets;
	conversion_offsets.Initialize(
	    Allocator::DefaultAllocator(),
	    {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER}, count);
	conversion_offsets.SetCardinality(count);
	InitializeOffsets(conversion_offsets, count);

	VariantVectorData variant_data(result);
	for (idx_t i = 0; i < count; i++) {
		auto &value = input[i];
		if (value.IsNull()) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		ConvertValue(value, variant_data, i, conversion_offsets, keys_selvec, dictionary);
	}

#ifdef DEBUG
	{
		auto conversion_keys_offset = variant::OffsetData::GetKeys(conversion_offsets);
		auto conversion_children_offset = variant::OffsetData::GetChildren(conversion_offsets);
		auto conversion_values_offset = variant::OffsetData::GetValues(conversion_offsets);
		auto conversion_data_offset = variant::OffsetData::GetBlob(conversion_offsets);

		auto analyze_keys_offset = variant::OffsetData::GetKeys(analyze_offsets);
		auto analyze_children_offset = variant::OffsetData::GetChildren(analyze_offsets);
		auto analyze_values_offset = variant::OffsetData::GetValues(analyze_offsets);
		auto analyze_data_offset = variant::OffsetData::GetBlob(analyze_offsets);

		for (idx_t i = 0; i < count; i++) {
			D_ASSERT(conversion_keys_offset[i] == analyze_keys_offset[i]);
			D_ASSERT(conversion_children_offset[i] == analyze_children_offset[i]);
			D_ASSERT(conversion_values_offset[i] == analyze_values_offset[i]);
			D_ASSERT(conversion_data_offset[i] == analyze_data_offset[i]);
		}
	}

#endif

	//! Finalize the 'data' column of the VARIANT
	auto conversion_data_offsets = variant::OffsetData::GetBlob(conversion_offsets);
	for (idx_t i = 0; i < count; i++) {
		auto &data = variant_data.blob_data[i];
		data.SetSizeAndFinalize(conversion_data_offsets[i], conversion_data_offsets[i]);
	}

	VariantUtils::FinalizeVariantKeys(result, dictionary, keys_selvec, keys_selvec_size);

	keys_entry.Slice(keys_selvec, keys_selvec_size);
	keys_entry.Flatten(keys_selvec_size);

	if (input.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
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
