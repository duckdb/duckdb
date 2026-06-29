#pragma once

#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

//! A TUPLE is an unnamed struct - it is serialized as a variant ARRAY (no keys), matching the JSON-array
//! representation produced for tuples. This mirrors ConvertStructToVariant but writes ARRAY metadata and omits keys.
template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertTupleToVariant(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count,
                           optional_ptr<const SelectionVector> selvec,
                           optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {
	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);
	auto children_offset_data = OffsetData::GetChildren(result.offsets);

	auto &source_format = source.source_format;
	auto &source_validity = source_format.validity;

	auto &children = StructVector::GetEntries(source.vec);

	auto &variant = result.variant;
	ContainerSelectionVectors sel(count);
	for (idx_t i = 0; i < count; i++) {
		auto index = source[i];
		auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];

		if (source_validity.RowIsValid(index)) {
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec,
			                                 i, VariantLogicalType::ARRAY);
			WriteContainerData<WRITE_DATA>(result.variant, result_index, blob_offset, children.size(),
			                               children_offset_data[result_index]);

			//! children
			if (WRITE_DATA) {
				auto &children_list_entry = variant.children_data[result_index];
				idx_t children_index = children_list_entry.offset + children_offset_data[result_index];
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					//! array children have no key
					variant.keys_index_validity.SetInvalid(children_index + child_idx);
				}
				//! Map from index of the child to the children.values_index of the parent
				//! NOTE: this maps to the first index, below we forward this for each child Vector we process.
				sel.children_selection.set_index(sel.count, children_index);
			}
			sel.non_null_selection.set_index(sel.count, source.GetMappedIndex(i));
			sel.new_selection.set_index(sel.count, result_index);
			children_offset_data[result_index] += children.size();
			sel.count++;
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec, i,
			                              is_root);
		}
	}

	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];

		if (sel.count != count) {
			//! Some of the TUPLE rows are NULL entirely, we have to filter these rows out of the children
			Vector new_child(child, sel.non_null_selection, sel.count);
			ToVariantSourceData child_source_data(new_child, sel.count);
			if (!ConvertToVariant<WRITE_DATA>(child_source_data, result, sel.count, &sel.new_selection,
			                                  &sel.children_selection, false)) {
				return false;
			}
		} else {
			ToVariantSourceData child_source_data(child, source.source_size, sel.non_null_selection);
			if (!ConvertToVariant<WRITE_DATA>(child_source_data, result, sel.count, &sel.new_selection,
			                                  &sel.children_selection, false)) {
				return false;
			}
		}
		if (WRITE_DATA) {
			//! Now move the selection forward to write the value_id for the next tuple child, for each row
			for (idx_t i = 0; i < sel.count; i++) {
				sel.children_selection[i]++;
			}
		}
	}
	return true;
}

} // namespace variant
} // namespace duckdb
