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
		res.emplace(string_t(type.c_str(), type.size()));
	}
	return res;
}

} // namespace duckdb
