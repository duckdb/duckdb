#include "duckdb/function/variant/variant_shredding.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

static void WriteShreddedPrimitive(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                   const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                   idx_t count, idx_t type_size) {
	auto result_data = FlatVector::GetData(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row));

		auto byte_offset = variant.GetByteOffset(row, value_index);
		auto &data = variant.GetData(row);
		auto value_ptr = data.GetData();
		auto result_offset = type_size * result_row;
		memcpy(result_data + result_offset, value_ptr + byte_offset, type_size);
	}
}

template <class T>
static void WriteShreddedDecimal(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                 const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                 idx_t count) {
	auto result_data = FlatVector::GetData(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row) && variant.GetTypeId(row, value_index) == VariantLogicalType::DECIMAL);

		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, value_index);
		D_ASSERT(decimal_data.width <= DecimalWidth<T>::max);
		auto result_offset = sizeof(T) * result_row;
		memcpy(result_data + result_offset, decimal_data.value_ptr, sizeof(T));
	}
}

static void WriteShreddedString(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                idx_t count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row) && (variant.GetTypeId(row, value_index) == VariantLogicalType::VARCHAR ||
		                                     variant.GetTypeId(row, value_index) == VariantLogicalType::BLOB));

		auto string_data = VariantUtils::DecodeStringData(variant, row, value_index);
		result_data[result_row] = StringVector::AddStringOrBlob(result, string_data);
	}
}

static void WriteShreddedBoolean(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                 const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                 idx_t count) {
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row));
		auto type_id = variant.GetTypeId(row, value_index);
		D_ASSERT(type_id == VariantLogicalType::BOOL_FALSE || type_id == VariantLogicalType::BOOL_TRUE);

		result_data[result_row] = type_id == VariantLogicalType::BOOL_TRUE;
	}
}

void VariantShredding::WriteTypedPrimitiveValues(UnifiedVariantVectorData &variant, Vector &result,
                                                 const SelectionVector &sel, const SelectionVector &value_index_sel,
                                                 const SelectionVector &result_sel, idx_t count) {
	auto &type = result.GetType();
	D_ASSERT(!type.IsNested());
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::UUID: {
		const auto physical_type = type.InternalType();
		WriteShreddedPrimitive(variant, result, sel, value_index_sel, result_sel, count, GetTypeIdSize(physical_type));
		break;
	}
	case LogicalTypeId::DECIMAL: {
		const auto physical_type = type.InternalType();
		switch (physical_type) {
		//! DECIMAL4
		case PhysicalType::INT32:
			WriteShreddedDecimal<int32_t>(variant, result, sel, value_index_sel, result_sel, count);
			break;
		//! DECIMAL8
		case PhysicalType::INT64:
			WriteShreddedDecimal<int64_t>(variant, result, sel, value_index_sel, result_sel, count);
			break;
		//! DECIMAL16
		case PhysicalType::INT128:
			WriteShreddedDecimal<hugeint_t>(variant, result, sel, value_index_sel, result_sel, count);
			break;
		default:
			throw InvalidInputException("Can't shred on column of type '%s'", type.ToString());
		}
		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR: {
		WriteShreddedString(variant, result, sel, value_index_sel, result_sel, count);
		break;
	}
	case LogicalTypeId::BOOLEAN:
		WriteShreddedBoolean(variant, result, sel, value_index_sel, result_sel, count);
		break;
	default:
		throw InvalidInputException("Can't shred on type: %s", type.ToString());
	}
}

void VariantShredding::WriteTypedObjectValues(UnifiedVariantVectorData &variant, Vector &result,
                                              const SelectionVector &sel, const SelectionVector &value_index_sel,
                                              const SelectionVector &result_sel, idx_t count) {
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
				WriteVariantValues(variant, child_vec, child_row_sel, child_values_indexes, child_result_sel,
				                   child_count);
			}
		} else {
			WriteVariantValues(variant, child_vec, &sel, child_values_indexes, result_sel, count);
		}
	}
}

void VariantShredding::WriteTypedArrayValues(UnifiedVariantVectorData &variant, Vector &result,
                                             const SelectionVector &sel, const SelectionVector &value_index_sel,
                                             const SelectionVector &result_sel, idx_t count) {
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
	WriteVariantValues(variant, child_vector, child_sel, child_value_index_sel, child_result_sel, total_offset);
}

void VariantShredding::WriteTypedValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                        const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                        idx_t count) {
	auto &type = result.GetType();

	if (type.id() == LogicalTypeId::STRUCT) {
		//! Shredded OBJECT
		WriteTypedObjectValues(variant, result, sel, value_index_sel, result_sel, count);
	} else if (type.id() == LogicalTypeId::LIST) {
		//! Shredded ARRAY
		WriteTypedArrayValues(variant, result, sel, value_index_sel, result_sel, count);
	} else {
		//! Primitive types
		WriteTypedPrimitiveValues(variant, result, sel, value_index_sel, result_sel, count);
	}
}

VariantShreddingState::VariantShreddingState(const LogicalType &type, idx_t total_count)
    : type(type), shredded_sel(total_count), values_index_sel(total_count), result_sel(total_count) {
}

bool VariantShreddingState::ValueIsShredded(UnifiedVariantVectorData &variant, idx_t row, idx_t values_index) {
	auto type_id = variant.GetTypeId(row, values_index);
	if (!GetVariantTypes().count(type_id)) {
		return false;
	}
	if (type_id == VariantLogicalType::DECIMAL) {
		auto physical_type = type.InternalType();
		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, values_index);
		auto decimal_physical_type = decimal_data.GetPhysicalType();
		return physical_type == decimal_physical_type;
	}
	return true;
}

void VariantShreddingState::SetShredded(idx_t row, idx_t values_index, idx_t result_idx) {
	shredded_sel[count] = row;
	values_index_sel[count] = values_index;
	result_sel[count] = result_idx;
	count++;
}

case_insensitive_string_set_t VariantShreddingState::ObjectFields() {
	D_ASSERT(type.id() == LogicalTypeId::STRUCT);
	case_insensitive_string_set_t res;
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &entry : child_types) {
		auto &type = entry.first;
		res.emplace(string_t(type.c_str(), static_cast<uint32_t>(type.size())));
	}
	return res;
}

} // namespace duckdb
