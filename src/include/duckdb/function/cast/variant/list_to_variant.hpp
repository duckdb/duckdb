#pragma once

#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
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
	auto &entry = ListVector::GetChildMutable(source.vec);
	auto child_size = ListVector::GetListSize(source.vec);
	if (sel.count != list_size) {
		Vector sliced_entry(entry, sel.non_null_selection, sel.count);
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

//! A MAP is physically a LIST(STRUCT(key, value)), but is written as a variant OBJECT - the keys are stringified into
//! the dictionary, matching how a MAP is converted directly to JSON.
template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertMapToVariant(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count,
                         optional_ptr<const SelectionVector> selvec,
                         optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {
	auto keys_offset_data = OffsetData::GetKeys(result.offsets);
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
		list_size += source_data[index].length;
	}

	auto &values_entry = MapVector::GetValues(source.vec);
	auto child_size = ListVector::GetListSize(source.vec);

	//! Only the second pass populates the dictionary, so the keys are only stringified there
	Vector key_strings(LogicalType::VARCHAR, WRITE_DATA ? child_size : 0);
	UnifiedVectorFormat key_format;
	if (WRITE_DATA) {
		VectorOperations::DefaultCast(MapVector::GetKeys(source.vec), key_strings, child_size);
		key_strings.ToUnifiedFormat(key_format);
	}

	ContainerSelectionVectors sel(list_size);
	for (idx_t i = 0; i < count; i++) {
		const auto index = source[i];
		const auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];

		if (!source_validity.RowIsValid(index)) {
			if (!IGNORE_NULLS) {
				HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset,
				                              values_index_selvec, i, is_root);
			}
			continue;
		}

		auto &entry = source_data[index];
		WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec, i,
		                                 VariantLogicalType::OBJECT);
		WriteContainerData<WRITE_DATA>(result.variant, result_index, blob_offset, entry.length,
		                               children_offset_data[result_index]);

		auto children_index = variant.children_data[result_index].offset + children_offset_data[result_index];
		auto keys_offset = variant.keys_data[result_index].offset + keys_offset_data[result_index];
		for (idx_t child_idx = 0; child_idx < entry.length; child_idx++) {
			sel.new_selection.set_index(sel.count + child_idx, result_index);
			sel.non_null_selection.set_index(sel.count + child_idx, entry.offset + child_idx);
			if (WRITE_DATA) {
				variant.keys_index_data[children_index + child_idx] =
				    NumericCast<uint32_t>(keys_offset_data[result_index] + child_idx);
				//! (the owning dictionary copies the key, so the cast strings are safe)
				auto key =
				    key_format.GetData<string_t>(key_format)[key_format.sel->get_index(entry.offset + child_idx)];
				result.keys_selvec.set_index(keys_offset + child_idx, result.GetOrCreateIndex(key));
				sel.children_selection.set_index(sel.count + child_idx, children_index + child_idx);
			}
		}
		keys_offset_data[result_index] += entry.length;
		children_offset_data[result_index] += entry.length;
		sel.count += entry.length;
	}

	//! Now write the value vector of the map (for all rows) - the keys live in the dictionary
	if (sel.count != list_size) {
		Vector sliced_entry(values_entry, sel.non_null_selection, sel.count);
		ToVariantSourceData child_source_data(sliced_entry, sel.count);
		return ConvertToVariant<WRITE_DATA, false>(child_source_data, result, sel.count, &sel.new_selection,
		                                           &sel.children_selection, false);
	} else {
		//! All rows are valid, no need to slice the child
		ToVariantSourceData child_source_data(values_entry, child_size, sel.non_null_selection);
		return ConvertToVariant<WRITE_DATA, false>(child_source_data, result, sel.count, &sel.new_selection,
		                                           &sel.children_selection, false);
	}
}

} // namespace variant
} // namespace duckdb
