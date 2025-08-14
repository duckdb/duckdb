#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

void VariantUtils::SortVariantKeys(Vector &dictionary, idx_t dictionary_size, SelectionVector &sel, idx_t sel_size) {
	auto &allocator = Allocator::DefaultAllocator();
	auto dictionary_data = FlatVector::GetData<string_t>(dictionary);

	//! string + unsorted_index
	vector<std::pair<reference<string_t>, idx_t>> strings;
	strings.reserve(dictionary_size);
	for (idx_t i = 0; i < dictionary_size; i++) {
		strings.emplace_back(dictionary_data[i], i);
	}

	//! Sort the unique strings
	std::sort(strings.begin(), strings.end(),
	          [](const std::pair<reference<string_t>, idx_t> &a, const std::pair<reference<string_t>, idx_t> &b) {
		          return a.first.get() < b.first.get();
	          });

	bool is_already_sorted = true;
	vector<idx_t> unsorted_to_sorted(strings.size());
	for (idx_t i = 0; i < strings.size(); i++) {
		if (i != strings[i].second) {
			is_already_sorted = false;
		}
		unsorted_to_sorted[strings[i].second] = i;
	}

	if (is_already_sorted) {
		return;
	}

	//! Adjust the selection vector to point to the right dictionary index
	for (idx_t i = 0; i < sel_size; i++) {
		auto old_dictionary_index = sel.get_index(i);
		auto new_dictionary_index = unsorted_to_sorted[old_dictionary_index];
		sel.set_index(i, new_dictionary_index);
	}

	//! Finally, rewrite the dictionary itself
	auto copied_dictionary = allocator.Allocate(sizeof(string_t) * dictionary_size);
	auto copied_dictionary_data = reinterpret_cast<string_t *>(copied_dictionary.get());
	memcpy(copied_dictionary_data, dictionary_data, sizeof(string_t) * dictionary_size);

	for (idx_t i = 0; i < dictionary_size; i++) {
		dictionary_data[i] = copied_dictionary_data[strings[i].second];
	}
}

bool VariantUtils::FindChildValues(RecursiveUnifiedVectorFormat &source, const VariantPathComponent &component,
                                   optional_idx row, uint32_t *res, VariantNestedData *nested_data, idx_t count) {
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
		if (component.lookup_mode == VariantChildLookupMode::BY_INDEX) {
			auto child_idx = component.payload.index;
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
			if (child_key == component.payload.key) {
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

bool VariantUtils::CollectNestedData(RecursiveUnifiedVectorFormat &variant, VariantLogicalType expected_type,
                                     uint32_t *value_indices, idx_t count, optional_idx row,
                                     VariantNestedData *child_data, string &error) {
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

		//! values
		auto values_index = values_format.sel->get_index(row_index);
		D_ASSERT(values_format.validity.RowIsValid(values_index));
		auto values_list_entry = values_data[values_index];

		//! Get the index into 'values'
		uint32_t value_index = value_indices[i];

		//! type_id + byte_offset
		auto type_id = static_cast<VariantLogicalType>(
		    type_id_data[type_id_format.sel->get_index(values_list_entry.offset + value_index)]);
		auto byte_offset = byte_offset_data[byte_offset_format.sel->get_index(values_list_entry.offset + value_index)];

		if (type_id != expected_type) {
			error = StringUtil::Format("'%s' was expected, found '%s', can't convert VARIANT",
			                           EnumUtil::ToString(expected_type), EnumUtil::ToString(type_id));
			return false;
		}

		auto blob_index = value_format.sel->get_index(row_index);
		auto blob_data = const_data_ptr_cast(value_data[blob_index].GetData());

		auto ptr = blob_data + byte_offset;
		child_data[i].child_count = VarintDecode<uint32_t>(ptr);
		child_data[i].children_idx = VarintDecode<uint32_t>(ptr);
	}
	return true;
}

} // namespace duckdb
