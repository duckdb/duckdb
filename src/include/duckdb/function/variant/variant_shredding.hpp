#pragma once

#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

template <class VARIANT_WRITER>
static void WriteTypedObjectValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                   const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                   idx_t count) {
	auto &type = result.GetType();
	D_ASSERT(type.id() == LogicalTypeId::STRUCT);

	auto &validity = FlatVector::Validity(result);
	(void)validity;

	//! Collect the nested data for the objects
	auto nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		//! When we're shredding an object, the top-level struct of it should always be valid
		D_ASSERT(validity.RowIsValid(result_sel[i]));
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.GetTypeId(row, value_index) == VariantLogicalType::OBJECT);
		nested_data[i] = VariantUtils::DecodeNestedData(variant, row, value_index);
	}

	auto &shredded_types = StructType::GetChildTypes(type);
	auto &shredded_fields = StructVector::GetEntries(result);
	D_ASSERT(shredded_types.size() == shredded_fields.size());

	SelectionVector child_values_indexes;
	SelectionVector child_row_sel;
	SelectionVector child_result_sel;
	child_values_indexes.Initialize(count);
	child_row_sel.Initialize(count);
	child_result_sel.Initialize(count);

	for (idx_t child_idx = 0; child_idx < shredded_types.size(); child_idx++) {
		auto &child_vec = *shredded_fields[child_idx];
		D_ASSERT(child_vec.GetType() == shredded_types[child_idx].second);

		//! Prepare the path component to perform the lookup for
		auto &key = shredded_types[child_idx].first;
		VariantPathComponent path_component;
		path_component.lookup_mode = VariantChildLookupMode::BY_KEY;
		path_component.key = key;

		ValidityMask lookup_validity(count);
		VariantUtils::FindChildValues(variant, path_component, sel, child_values_indexes, lookup_validity,
		                              nested_data.get(), count);

		if (!lookup_validity.AllValid()) {
			auto &child_variant_vectors = StructVector::GetEntries(child_vec);

			//! For some of the rows the field is missing, adjust the selection vector to exclude these rows.
			idx_t child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				if (!lookup_validity.RowIsValid(i)) {
					//! The field is missing, set it to null
					FlatVector::SetNull(*child_variant_vectors[0], result_sel[i], true);
					if (child_variant_vectors.size() >= 2) {
						FlatVector::SetNull(*child_variant_vectors[1], result_sel[i], true);
					}
					continue;
				}

				child_row_sel[child_count] = sel[i];
				child_values_indexes[child_count] = child_values_indexes[i];
				child_result_sel[child_count] = result_sel[i];
				child_count++;
			}

			if (child_count) {
				//! If not all rows are missing this field, write the values for it
				VARIANT_WRITER::WriteVariantValues(variant, child_vec, child_row_sel, child_values_indexes,
				                                   child_result_sel, child_count);
			}
		} else {
			VARIANT_WRITER::WriteVariantValues(variant, child_vec, &sel, child_values_indexes, result_sel, count);
		}
	}
}

template <class VARIANT_WRITER>
static void WriteTypedArrayValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                  const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                  idx_t count) {
	auto list_data = FlatVector::GetData<list_entry_t>(result);

	auto nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);

	idx_t total_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto value_index = value_index_sel[i];
		auto result_row = result_sel[i];

		D_ASSERT(variant.GetTypeId(row, value_index) == VariantLogicalType::ARRAY);
		nested_data[i] = VariantUtils::DecodeNestedData(variant, row, value_index);

		list_entry_t list_entry;
		list_entry.length = nested_data[i].child_count;
		list_entry.offset = total_offset;
		list_data[result_row] = list_entry;

		total_offset += nested_data[i].child_count;
	}
	ListVector::Reserve(result, total_offset);
	ListVector::SetListSize(result, total_offset);

	SelectionVector child_sel;
	child_sel.Initialize(total_offset);

	SelectionVector child_value_index_sel;
	child_value_index_sel.Initialize(total_offset);

	SelectionVector child_result_sel;
	child_result_sel.Initialize(total_offset);

	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];

		auto &array_data = nested_data[i];
		auto &entry = list_data[result_row];
		for (idx_t j = 0; j < entry.length; j++) {
			auto offset = entry.offset + j;
			child_sel[offset] = row;
			child_value_index_sel[offset] = variant.GetValuesIndex(row, array_data.children_idx + j);
			child_result_sel[offset] = offset;
		}
	}

	auto &child_vector = ListVector::GetEntry(result);
	VARIANT_WRITER::WriteVariantValues(variant, child_vector, child_sel, child_value_index_sel, child_result_sel,
	                                   total_offset);
}

struct VariantShredding {
public:
	template <class VARIANT_WRITER>
	static void WriteTypedValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
	                             const SelectionVector &value_index_sel, const SelectionVector &result_sel,
	                             idx_t count) {
		auto &type = result.GetType();

		if (type.id() == LogicalTypeId::STRUCT) {
			//! Shredded OBJECT
			WriteTypedObjectValues<VARIANT_WRITER>(variant, result, sel, value_index_sel, result_sel, count);
		} else if (type.id() == LogicalTypeId::LIST) {
			//! Shredded ARRAY
			WriteTypedArrayValues<VARIANT_WRITER>(variant, result, sel, value_index_sel, result_sel, count);
		} else {
			//! Primitive types
			WriteTypedPrimitiveValues(variant, result, sel, value_index_sel, result_sel, count);
		}
	}

private:
	static void WriteTypedPrimitiveValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
	                                      const SelectionVector &value_index_sel, const SelectionVector &result_sel,
	                                      idx_t count);
};

struct VariantShreddingState {
public:
	explicit VariantShreddingState(const LogicalType &type, idx_t total_count);

public:
	bool ValueIsShredded(UnifiedVariantVectorData &variant, idx_t row, idx_t values_index);
	void SetShredded(idx_t row, idx_t values_index, idx_t result_idx);
	case_insensitive_string_set_t ObjectFields();
	virtual const unordered_set<VariantLogicalType> &GetVariantTypes() = 0;

public:
	//! The type the field is shredded on
	const LogicalType &type;
	//! row that is shredded
	SelectionVector shredded_sel;
	//! 'values_index' of the shredded value
	SelectionVector values_index_sel;
	//! result row of the shredded value
	SelectionVector result_sel;
	//! The amount of rows that are shredded on
	idx_t count = 0;
};

} // namespace duckdb
