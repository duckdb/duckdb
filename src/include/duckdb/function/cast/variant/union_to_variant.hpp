#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"

namespace duckdb {
namespace variant {

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertUnionToVariant(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count,
                           optional_ptr<const SelectionVector> selvec,
                           optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {
	auto &children = StructVector::GetEntries(source.vec);

	vector<ToVariantSourceData> member_data;
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];

		//! Convert all the children, ignore nulls, only write the non-null values
		//! UNION will have exactly 1 non-null value for each row
		member_data.emplace_back(child, source.source_size, source.source_sel);
		if (!ConvertToVariant<WRITE_DATA, /*ignore_nulls = */ true>(member_data.back(), result, count, selvec,
		                                                            values_index_selvec, is_root)) {
			return false;
		}
	}

	if (IGNORE_NULLS) {
		return true;
	}

	//! For some reason we can have nulls in members, so we need this check
	//! So we are sure that we handled all nulls
	auto values_offset_data = OffsetData::GetValues(result.offsets);
	auto blob_offset_data = OffsetData::GetBlob(result.offsets);
	for (idx_t i = 0; i < count; i++) {
		bool is_null = true;
		for (idx_t child_idx = 1; child_idx < children.size() && is_null; child_idx++) {
			auto &child = *children[child_idx];
			if (child.GetType().id() == LogicalTypeId::SQLNULL) {
				continue;
			}
			auto &member_format = member_data[child_idx - 1].source_format;
			auto &member_validity = member_format.validity;
			is_null = !member_validity.RowIsValid(member_format.sel->get_index(i));
		}
		if (!is_null) {
			continue;
		}
		//! This row is NULL entirely
		auto result_index = selvec ? selvec->get_index(i) : i;
		auto &blob_offset = blob_offset_data[result_index];
		HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec, i,
		                              is_root);
	}
	return true;
}

} // namespace variant
} // namespace duckdb
