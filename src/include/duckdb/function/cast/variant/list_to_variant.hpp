#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertListToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                          optional_ptr<const SelectionVector> selvec, SelectionVector &keys_selvec,
                          OrderedOwningStringMap<uint32_t> &dictionary,
                          optional_ptr<const SelectionVector> value_ids_selvec, const bool is_root) {
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto children_offset_data = OffsetData::GetChildren(offsets);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(count, source_format);
	auto &source_validity = source_format.validity;
	auto source_data = source_format.GetData<list_entry_t>(source_format);

	auto list_size = ListVector::GetListSize(source);

	ContainerSelectionVectors sel(list_size);
	for (idx_t i = 0; i < count; i++) {
		const auto index = source_format.sel->get_index(i);
		const auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];
		auto &children_list_entry = result.children_data[result_index];

		if (source_validity.RowIsValid(index)) {
			auto &entry = source_data[index];
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                                 VariantLogicalType::ARRAY);
			WriteContainerData<WRITE_DATA>(result, result_index, blob_offset, entry.length,
			                               children_offset_data[result_index]);
			WriteArrayChildren<WRITE_DATA>(result, children_list_entry.offset, children_offset_data[result_index],
			                               entry, result_index, sel);
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
			                              is_root);
		}
	}
	//! Now write the child vector of the list (for all rows)
	auto &entry = ListVector::GetEntry(source);
	if (sel.count != list_size) {
		Vector sliced_entry(entry.GetType(), nullptr);
		sliced_entry.Dictionary(entry, list_size, sel.non_null_selection, sel.count);
		return ConvertToVariant<WRITE_DATA, false>(sliced_entry, result, offsets, sel.count, &sel.new_selection,
		                                           keys_selvec, dictionary, &sel.children_selection, false);
	} else {
		//! All rows are valid, no need to slice the child
		return ConvertToVariant<WRITE_DATA, false>(entry, result, offsets, sel.count, &sel.new_selection, keys_selvec,
		                                           dictionary, &sel.children_selection, false);
	}
}

} // namespace variant
} // namespace duckdb
