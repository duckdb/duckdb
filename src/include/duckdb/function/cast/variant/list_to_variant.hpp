#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertListToVariant(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count,
                          optional_ptr<const SelectionVector> selvec,
                          optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);
	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto children_offset_data = OffsetData::GetChildren(result.offsets);

	auto &source_format = source.source_format;
	auto &source_validity = source_format.validity;
	auto source_data = source_format.GetData<list_entry_t>(source_format);

	auto &variant = result.variant;
	idx_t list_size = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto index = source[i];
		if (!source_validity.RowIsValid(index)) {
			continue;
		}
		auto &entry = source_data[index];
		list_size += entry.length;
	}

	ContainerSelectionVectors sel(list_size);
	for (idx_t i = 0; i < count; i++) {
		const auto index = source[i];
		const auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];

		auto &children_list_entry = variant.children_data[result_index];
		if (source_validity.RowIsValid(index)) {
			auto &entry = source_data[index];
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec,
			                                 i, VariantLogicalType::ARRAY);
			WriteContainerData<WRITE_DATA>(result.variant, result_index, blob_offset, entry.length,
			                               children_offset_data[result_index]);
			WriteArrayChildren<WRITE_DATA>(result.variant, children_list_entry.offset,
			                               children_offset_data[result_index], entry, result_index, sel);
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec, i,
			                              is_root);
		}
	}
	//! Now write the child vector of the list (for all rows)
	auto &entry = ListVector::GetEntry(source.vec);
	auto child_size = ListVector::GetListSize(source.vec);
	if (sel.count != list_size) {
		Vector sliced_entry(entry.GetType(), nullptr);
		sliced_entry.Dictionary(entry, list_size, sel.non_null_selection, sel.count);
		ToVariantSourceData child_source_data(sliced_entry, sel.count);
		return ConvertToVariant<WRITE_DATA, false>(child_source_data, result, sel.count, &sel.new_selection,
		                                           &sel.children_selection, false);
	} else {
		//! All rows are valid, no need to slice the child
		ToVariantSourceData child_source_data(entry, child_size, sel.non_null_selection);
		return ConvertToVariant<WRITE_DATA, false>(child_source_data, result, sel.count, &sel.new_selection,
		                                           &sel.children_selection, false);
	}
}

} // namespace variant
} // namespace duckdb
