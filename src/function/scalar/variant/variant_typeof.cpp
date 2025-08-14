#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

static void VariantTypeofFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();

	D_ASSERT(input.ColumnCount() == 1);
	auto &variant = input.data[0];
	D_ASSERT(variant.GetType() == LogicalType::VARIANT());

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant, count, source_format);

	//! type_id
	auto &type_id = UnifiedVariantVector::GetValuesTypeId(source_format);
	auto type_id_data = type_id.GetData<uint8_t>(type_id);

	//! byte_offset
	auto &byte_offset = UnifiedVariantVector::GetValuesByteOffset(source_format);
	auto byte_offset_data = byte_offset.GetData<uint32_t>(byte_offset);

	//! values
	auto &values = UnifiedVariantVector::GetValues(source_format);
	auto values_data = values.GetData<list_entry_t>(values);

	//! children
	auto &children = UnifiedVariantVector::GetChildren(source_format);
	auto children_data = children.GetData<list_entry_t>(children);

	//! key_id
	auto &key_id = UnifiedVariantVector::GetChildrenKeyId(source_format);
	auto key_id_data = key_id.GetData<uint32_t>(key_id);

	//! keys
	auto &keys = UnifiedVariantVector::GetKeys(source_format);
	auto keys_data = keys.GetData<list_entry_t>(keys);

	//! keys entry
	auto &keys_entry = UnifiedVariantVector::GetKeysEntry(source_format);
	auto keys_entry_data = keys_entry.GetData<string_t>(keys_entry);

	//! value
	auto &value = UnifiedVariantVector::GetData(source_format);
	auto value_data = value.GetData<string_t>(value);

	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto values_list_entry = values_data[values.sel->get_index(i)];
		auto children_list_entry = children_data[children.sel->get_index(i)];
		auto keys_list_entry = keys_data[keys.sel->get_index(i)];
		auto type = static_cast<VariantLogicalType>(type_id_data[type_id.sel->get_index(values_list_entry.offset)]);

		auto blob_data = value_data[value.sel->get_index(i)].GetData();

		string type_str;
		if (type == VariantLogicalType::OBJECT) {
			auto value_byte_offset = byte_offset_data[byte_offset.sel->get_index(values_list_entry.offset)];
			auto ptr = const_data_ptr_cast(blob_data + value_byte_offset);
			auto child_count = VarintDecode<uint32_t>(ptr);
			auto children_index = VarintDecode<uint32_t>(ptr);

			vector<string> object_keys;
			for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
				auto child_key_id =
				    key_id_data[key_id.sel->get_index(children_list_entry.offset + children_index + child_idx)];
				object_keys.push_back(keys_entry_data[keys_list_entry.offset + child_key_id].GetString());
			}
			type_str = StringUtil::Format("OBJECT(%s)", StringUtil::Join(object_keys, ", "));
		} else if (type == VariantLogicalType::ARRAY) {
			auto value_byte_offset = byte_offset_data[byte_offset.sel->get_index(values_list_entry.offset)];
			auto ptr = const_data_ptr_cast(blob_data + value_byte_offset);
			auto child_count = VarintDecode<uint32_t>(ptr);
			type_str = StringUtil::Format("ARRAY(%d)", child_count);
		} else if (type == VariantLogicalType::DECIMAL) {
			auto value_byte_offset = byte_offset_data[byte_offset.sel->get_index(values_list_entry.offset)];
			auto ptr = const_data_ptr_cast(blob_data + value_byte_offset);
			auto width = VarintDecode<uint32_t>(ptr);
			auto scale = VarintDecode<uint32_t>(ptr);
			type_str = StringUtil::Format("DECIMAL(%d, %d)", width, scale);
		} else {
			type_str = EnumUtil::ToString(type);
		}
		result_data[i] = StringVector::AddString(result, type_str.c_str());
	}

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction VariantTypeofFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("variant_typeof", {variant_type}, LogicalType::VARCHAR, VariantTypeofFunction);
}

} // namespace duckdb
