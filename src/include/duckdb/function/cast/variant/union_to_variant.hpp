#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertUnionToVariant(Vector &source, VariantVectorData &result, DataChunk &offsets, idx_t count,
                           optional_ptr<SelectionVector> selvec, SelectionVector &keys_selvec,
                           OrderedOwningStringMap<uint32_t> &dictionary, optional_ptr<SelectionVector> value_ids_selvec,
                           const bool is_root) {
	auto &children = StructVector::GetEntries(source);

	UnifiedVectorFormat source_format;
	vector<UnifiedVectorFormat> member_formats(children.size());
	source.ToUnifiedFormat(count, source_format);
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		child.ToUnifiedFormat(count, member_formats[child_idx]);

		//! Convert all the children, ignore nulls, only write the non-null values
		//! UNION will have exactly 1 non-null value for each row
		if (!ConvertToVariant<WRITE_DATA, /*ignore_nulls = */ true>(child, result, offsets, count, selvec, keys_selvec,
		                                                            dictionary, value_ids_selvec, is_root)) {
			return false;
		}
	}

	if (IGNORE_NULLS) {
		return true;
	}

	//! For some reason we can have nulls in members, so we need this check
	//! So we are sure that we handled all nulls
	auto values_offset_data = OffsetData::GetValues(offsets);
	auto blob_offset_data = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		bool is_null = true;
		for (idx_t child_idx = 1; child_idx < children.size() && is_null; child_idx++) {
			auto &child = *children[child_idx];
			if (child.GetType().id() == LogicalTypeId::SQLNULL) {
				continue;
			}
			auto &member_format = member_formats[child_idx];
			auto &member_validity = member_format.validity;
			is_null = !member_validity.RowIsValid(member_format.sel->get_index(i));
		}
		if (!is_null) {
			continue;
		}
		//! This row is NULL entirely
		auto result_index = selvec ? selvec->get_index(i) : i;
		auto &blob_offset = blob_offset_data[result_index];
		HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, value_ids_selvec, i,
		                              is_root);
	}
	return true;
}

} // namespace variant
} // namespace duckdb
