#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertStructToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                            idx_t source_size, optional_ptr<const SelectionVector> selvec,
                            optional_ptr<const SelectionVector> source_sel, SelectionVector &keys_selvec,
                            OrderedOwningStringMap<uint32_t> &dictionary,
                            optional_ptr<const SelectionVector> value_ids_selvec, const bool is_root) {
	auto keys_offset_data = OffsetData::GetKeys(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto children_offset_data = OffsetData::GetChildren(offsets);
	auto &type = source.GetType();

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_size, source_format);
	auto &source_validity = source_format.validity;

	auto &children = StructVector::GetEntries(source);

	//! Look up all the dictionary indices for the struct keys
	vector<uint32_t> dictionary_indices;
	dictionary_indices.reserve(children.size());

	ContainerSelectionVectors sel(count);
	for (idx_t i = 0; i < count; i++) {
		auto index = source_format.sel->get_index(source_sel ? source_sel->get_index(i) : i);
		auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];
		auto &children_list_entry = result.children_data[result_index];
		auto &keys_list_entry = result.keys_data[result_index];

		if (source_validity.RowIsValid(index)) {
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                                 VariantLogicalType::OBJECT);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, children.size(),
			                               children_offset_data[result_index]);

			if (WRITE_DATA && dictionary_indices.empty()) {
				auto &struct_children = StructType::GetChildTypes(type);
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					auto &struct_child = struct_children[child_idx];
					string_t struct_child_str(struct_child.first.c_str(),
					                          NumericCast<uint32_t>(struct_child.first.size()));
					auto dictionary_size = dictionary.size();
					dictionary_indices.push_back(
					    dictionary.emplace(std::make_pair(struct_child_str, dictionary_size)).first->second);
				}
			}

			//! children
			if (WRITE_DATA) {
				idx_t children_index = children_list_entry.offset + children_offset_data[result_index];
				idx_t keys_offset = keys_list_entry.offset + keys_offset_data[result_index];
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					result.key_id_data[children_index + child_idx] =
					    NumericCast<uint32_t>(keys_offset_data[result_index] + child_idx);
					keys_selvec.set_index(keys_offset + child_idx, dictionary_indices[child_idx]);
				}
				//! Map from index of the child to the children.value_ids of the parent
				//! NOTE: this maps to the first index, below we are forwarding this for each child Vector we process.
				sel.children_selection.set_index(sel.count, children_index);
			}
			sel.non_null_selection.set_index(sel.count, i);
			sel.new_selection.set_index(sel.count, result_index);
			keys_offset_data[result_index] += children.size();
			children_offset_data[result_index] += children.size();
			sel.count++;
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                              is_root);
		}
	}

	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];

		if (sel.count != count) {
			//! Some of the STRUCT rows are NULL entirely, we have to filter these rows out of the children
			Vector new_child(child.GetType(), nullptr);
			new_child.Dictionary(child, count, sel.non_null_selection, sel.count);
			if (!ConvertToVariant<WRITE_DATA>(new_child, result, offsets, sel.count, source_size, &sel.new_selection,
			                                  nullptr, keys_selvec, dictionary, &sel.children_selection, false)) {
				return false;
			}
		} else {
			if (!ConvertToVariant<WRITE_DATA>(child, result, offsets, sel.count, source_size, &sel.new_selection,
			                                  &sel.non_null_selection, keys_selvec, dictionary, &sel.children_selection,
			                                  false)) {
				return false;
			}
		}
		if (WRITE_DATA) {
			//! Now forward the selection to point to the next index in the children.value_ids
			for (idx_t i = 0; i < sel.count; i++) {
				sel.children_selection[i]++;
			}
		}
	}
	return true;
}

} // namespace variant
} // namespace duckdb
