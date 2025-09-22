#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "duckdb/function/cast/variant/to_variant.hpp"

namespace duckdb {

namespace variant {

static void InitializeOffsets(DataChunk &offsets, idx_t count) {
	auto keys = OffsetData::GetKeys(offsets);
	auto children = OffsetData::GetChildren(offsets);
	auto values = OffsetData::GetValues(offsets);
	auto blob = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		keys[i] = 0;
		children[i] = 0;
		values[i] = 0;
		blob[i] = 0;
	}
}

static void InitializeVariants(DataChunk &offsets, Vector &result, SelectionVector &keys_selvec, idx_t &selvec_size,
                               OrderedOwningStringMap<uint32_t> &dictionary) {
	auto &keys = VariantVector::GetKeys(result);
	auto keys_data = ListVector::GetData(keys);

	auto &children = VariantVector::GetChildren(result);
	auto children_data = ListVector::GetData(children);

	auto &values = VariantVector::GetValues(result);
	auto values_data = ListVector::GetData(values);

	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);

	idx_t keys_offset = 0;
	idx_t children_offset = 0;
	idx_t values_offset = 0;

	auto keys_sizes = OffsetData::GetKeys(offsets);
	auto children_sizes = OffsetData::GetChildren(offsets);
	auto values_sizes = OffsetData::GetValues(offsets);
	auto blob_sizes = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < offsets.size(); i++) {
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

static void FinalizeVariantKeys(Vector &variant, OrderedOwningStringMap<uint32_t> &dictionary, SelectionVector &sel,
                                idx_t sel_size) {
	auto &keys = VariantVector::GetKeys(variant);
	auto &keys_entry = ListVector::GetEntry(keys);
	auto keys_entry_data = FlatVector::GetData<string_t>(keys_entry);

	bool already_sorted = true;

	vector<uint32_t> unsorted_to_sorted(dictionary.size());
	auto it = dictionary.begin();
	for (uint32_t sorted_idx = 0; sorted_idx < dictionary.size(); sorted_idx++) {
		auto unsorted_idx = it->second;
		if (unsorted_idx != sorted_idx) {
			already_sorted = false;
		}
		unsorted_to_sorted[unsorted_idx] = sorted_idx;
		D_ASSERT(sorted_idx < ListVector::GetListSize(keys));
		keys_entry_data[sorted_idx] = it->first;
		auto size = static_cast<uint32_t>(keys_entry_data[sorted_idx].GetSize());
		keys_entry_data[sorted_idx].SetSizeAndFinalize(size, size);
		it++;
	}

	if (!already_sorted) {
		//! Adjust the selection vector to point to the right dictionary index
		for (idx_t i = 0; i < sel_size; i++) {
			auto &entry = sel[i];
			auto sorted_idx = unsorted_to_sorted[entry];
			entry = sorted_idx;
		}
	}
}

static bool GatherOffsetsAndSizes(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count) {
	InitializeOffsets(result.offsets, count);
	//! First pass - collect sizes/offsets
	return ConvertToVariant<false>(source, result, count, nullptr, nullptr, true);
}

static bool WriteVariantResultData(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count) {
	InitializeOffsets(result.offsets, count);
	//! Second pass - actually construct the variants
	return ConvertToVariant<true>(source, result, count, nullptr, nullptr, true);
}

static bool CastToVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	DataChunk offsets;
	offsets.Initialize(Allocator::DefaultAllocator(),
	                   {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER},
	                   count);
	offsets.SetCardinality(count);
	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);

	//! Initialize the dictionary
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());
	SelectionVector keys_selvec;
	ToVariantSourceData source_data(source, count);

	{
		VariantVectorData variant_data(result);
		ToVariantGlobalResultData result_data(variant_data, offsets, dictionary, keys_selvec);
		if (!GatherOffsetsAndSizes(source_data, result_data, count)) {
			return false;
		}
	}

	//! This resizes the lists, invalidating the "GetData" results stored in VariantVectorData
	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size, dictionary);

	{
		VariantVectorData variant_data(result);
		ToVariantGlobalResultData result_data(variant_data, offsets, dictionary, keys_selvec);
		if (!WriteVariantResultData(source_data, result_data, count)) {
			return false;
		}
	}

	FinalizeVariantKeys(result, dictionary, keys_selvec, keys_selvec_size);
	//! Finalize the 'data'
	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);
	auto blob_offsets = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		auto size = blob_offsets[i];
		blob_data[i].SetSizeAndFinalize(size, size);
	}

	keys_entry.Slice(keys_selvec, keys_selvec_size);
	keys_entry.Flatten(keys_selvec_size);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
	return true;
}

} // namespace variant

BoundCastInfo DefaultCasts::ImplicitToVariantCast(BindCastInput &input, const LogicalType &source,
                                                  const LogicalType &target) {
	return BoundCastInfo(variant::CastToVARIANT);
}

namespace variant {

template <bool WRITE_DATA, bool IGNORE_NULLS>
bool ConvertStructToSparseVariant(ToVariantSourceData &source, ToVariantGlobalResultData &result, idx_t count,
                                  optional_ptr<const SelectionVector> selvec,
                                  optional_ptr<const SelectionVector> values_index_selvec, const bool is_root) {
	const auto keys_offset_data = OffsetData::GetKeys(result.offsets);
	const auto values_offset_data = OffsetData::GetValues(result.offsets);
	const auto blob_offset_data = OffsetData::GetBlob(result.offsets);
	const auto children_offset_data = OffsetData::GetChildren(result.offsets);
	const auto &type = source.vec.GetType();

	const auto &source_format = source.source_format;
	const auto &source_validity = source_format.validity;

	auto &children = StructVector::GetEntries(source.vec);
	vector<ToVariantSourceData> child_source_data;
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		child_source_data.emplace_back(child, source.source_size);
	}

	idx_t non_null_child_counts[STANDARD_VECTOR_SIZE] = {0};
	unsafe_vector<bool> key_is_used(children.size(), false);
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		const auto &child_format = child_source_data[child_idx].source_format;
		for (idx_t i = 0; i < count; i++) {
			const auto index = source[i];
			if (!source_validity.RowIsValid(index)) {
				continue;
			}
			if (child_format.validity.RowIsValid(child_format.sel->get_index(index))) {
				non_null_child_counts[i]++;
				key_is_used[child_idx] = true;
			}
		}
	}

	//! Look up all the dictionary indices for the struct keys
	vector<uint32_t> dictionary_indices;
	dictionary_indices.reserve(children.size());

	auto &variant = result.variant;
	ContainerSelectionVectors sel(count);
	for (idx_t i = 0; i < count; i++) {
		const auto index = source[i];
		const auto result_index = selvec ? selvec->get_index(i) : i;

		auto &blob_offset = blob_offset_data[result_index];

		const auto &non_null_child_count = non_null_child_counts[i];
		if (source_validity.RowIsValid(index) && non_null_child_count != 0) {
			WriteVariantMetadata<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec,
			                                 i, VariantLogicalType::OBJECT);
			WriteContainerData<WRITE_DATA>(result.variant, result_index, blob_offset, non_null_child_count,
			                               children_offset_data[result_index]);

			if (WRITE_DATA && dictionary_indices.empty()) {
				if (StructType::IsUnnamed(type)) {
					throw ConversionException("Can't cast unnamed struct to VARIANT");
				}
				auto &struct_children = StructType::GetChildTypes(type);
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					if (key_is_used[child_idx]) {
						auto &struct_child = struct_children[child_idx];
						string_t struct_child_str(struct_child.first.c_str(),
						                          NumericCast<uint32_t>(struct_child.first.size()));
						dictionary_indices.push_back(result.GetOrCreateIndex(struct_child_str));
					} else {
						dictionary_indices.push_back(static_cast<uint32_t>(-1));
					}
				}
			}

			//! children
			if (WRITE_DATA) {
				auto &children_list_entry = variant.children_data[result_index];
				auto &keys_list_entry = variant.keys_data[result_index];

				idx_t children_index = children_list_entry.offset + children_offset_data[result_index];
				idx_t keys_offset = keys_list_entry.offset + keys_offset_data[result_index];

				idx_t non_null_idx = 0; // Keep track of non-NULL children because this is a sparse conversion
				for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
					const auto &child_format = child_source_data[child_idx].source_format;
					if (!child_format.validity.RowIsValid(child_format.sel->get_index(index))) {
						continue; // Sparse conversion: skip NULL
					}
					variant.keys_index_data[children_index + non_null_idx] =
					    NumericCast<uint32_t>(keys_offset_data[result_index] + non_null_idx);
					result.keys_selvec.set_index(keys_offset + non_null_idx, dictionary_indices[child_idx]);
					non_null_idx++;
				}
				//! Map from index of the child to the children.values_index of the parent
				//! NOTE: this maps to the first index, below we are forwarding this for each child Vector we process.
				sel.children_selection.set_index(sel.count, children_index);
			}
			sel.non_null_selection.set_index(sel.count, source.GetMappedIndex(i));
			sel.new_selection.set_index(sel.count, result_index);
			keys_offset_data[result_index] += non_null_child_count;
			children_offset_data[result_index] += non_null_child_count;
			sel.count++;
		} else if (!IGNORE_NULLS) {
			HandleVariantNull<WRITE_DATA>(result, result_index, values_offset_data, blob_offset, values_index_selvec, i,
			                              is_root);
		}
	}

	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];

		if (sel.count != count) {
			//! Some of the STRUCT rows are NULL entirely, we have to filter these rows out of the children
			Vector new_child(child.GetType(), nullptr);
			new_child.Dictionary(child, count, sel.non_null_selection, sel.count);
			ToVariantSourceData csd(new_child, source.source_size);
			if (!ConvertToVariant<WRITE_DATA, true>(csd, result, sel.count, &sel.new_selection, &sel.children_selection,
			                                        false)) {
				return false;
			}
		} else {
			auto &csd = child_source_data[child_idx];
			csd.source_sel = sel.non_null_selection;
			if (!ConvertToVariant<WRITE_DATA, true>(csd, result, sel.count, &sel.new_selection, &sel.children_selection,
			                                        false)) {
				return false;
			}
		}
		if (WRITE_DATA) {
			//! Now forward the selection to point to the next index in the children.values_index
			const auto &child_format = child_source_data[child_idx].source_format;
			for (idx_t i = 0; i < sel.count; i++) {
				const auto index = source[i];
				if (child_format.validity.RowIsValid(child_format.sel->get_index(index))) {
					sel.children_selection[i]++;
				}
			}
		}
	}

	return true;
}

static bool StructToSparseVariant(DataChunk &args, ExpressionState &, Vector &result) {
	auto &source = args.data[0];
	const auto count = args.size();

	DataChunk offsets;
	offsets.Initialize(Allocator::DefaultAllocator(),
	                   {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER},
	                   count);
	offsets.SetCardinality(count);
	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetEntry(keys);

	//! Initialize the dictionary
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());
	SelectionVector keys_selvec;
	ToVariantSourceData source_data(source, count);

	{
		VariantVectorData variant_data(result);
		ToVariantGlobalResultData result_data(variant_data, offsets, dictionary, keys_selvec);
		InitializeOffsets(result_data.offsets, count);
		if (!ConvertStructToSparseVariant<false, false>(source_data, result_data, count, nullptr, nullptr, true)) {
			return false;
		}
	}

	//! This resizes the lists, invalidating the "GetData" results stored in VariantVectorData
	idx_t keys_selvec_size;
	InitializeVariants(offsets, result, keys_selvec, keys_selvec_size, dictionary);

	{
		VariantVectorData variant_data(result);
		ToVariantGlobalResultData result_data(variant_data, offsets, dictionary, keys_selvec);
		InitializeOffsets(result_data.offsets, count);
		if (!ConvertStructToSparseVariant<true, false>(source_data, result_data, count, nullptr, nullptr, true)) {
			return false;
		}
	}

	FinalizeVariantKeys(result, dictionary, keys_selvec, keys_selvec_size);
	//! Finalize the 'data'
	auto &blob = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetData<string_t>(blob);
	auto blob_offsets = OffsetData::GetBlob(offsets);
	for (idx_t i = 0; i < count; i++) {
		auto size = blob_offsets[i];
		blob_data[i].SetSizeAndFinalize(size, size);
	}

	keys_entry.Slice(keys_selvec, keys_selvec_size);
	keys_entry.Flatten(keys_selvec_size);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
	return true;
}

} // namespace variant

ScalarFunction StructToSparseVariantFun::GetFunction() {
	ScalarFunction string_split({LogicalType::ANY}, LogicalType::VARIANT(), variant::StructToSparseVariant);
	return string_split;
}

} // namespace duckdb
