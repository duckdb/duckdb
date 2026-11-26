#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/variant_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

VariantVectorData::VariantVectorData(Vector &variant)
    : variant(variant), keys_index_validity(FlatVector::ValidityMutable(VariantVector::GetChildrenKeysIndex(variant))),
      keys(VariantVector::GetKeys(variant)) {
	blob_data = FlatVector::GetDataMutable<string_t>(VariantVector::GetData(variant));
	type_ids_data = FlatVector::GetDataMutable<uint8_t>(VariantVector::GetValuesTypeId(variant));
	byte_offset_data = FlatVector::GetDataMutable<uint32_t>(VariantVector::GetValuesByteOffset(variant));
	keys_index_data = FlatVector::GetDataMutable<uint32_t>(VariantVector::GetChildrenKeysIndex(variant));
	values_index_data = FlatVector::GetDataMutable<uint32_t>(VariantVector::GetChildrenValuesIndex(variant));
	values_data = FlatVector::GetDataMutable<list_entry_t>(VariantVector::GetValues(variant));
	children_data = FlatVector::GetDataMutable<list_entry_t>(VariantVector::GetChildren(variant));
	keys_data = FlatVector::GetDataMutable<list_entry_t>(keys);
}

LogicalTypeId VariantTypeToLogicalTypeId(VariantLogicalType type) {
	switch (type) {
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		return LogicalTypeId::BOOLEAN;
	case VariantLogicalType::VARIANT_NULL:
		return LogicalTypeId::SQLNULL;
	case VariantLogicalType::INT8:
		return LogicalTypeId::TINYINT;
	case VariantLogicalType::INT16:
		return LogicalTypeId::SMALLINT;
	case VariantLogicalType::INT32:
		return LogicalTypeId::INTEGER;
	case VariantLogicalType::INT64:
		return LogicalTypeId::BIGINT;
	case VariantLogicalType::INT128:
		return LogicalTypeId::HUGEINT;
	case VariantLogicalType::UINT8:
		return LogicalTypeId::UTINYINT;
	case VariantLogicalType::UINT16:
		return LogicalTypeId::USMALLINT;
	case VariantLogicalType::UINT32:
		return LogicalTypeId::UINTEGER;
	case VariantLogicalType::UINT64:
		return LogicalTypeId::UBIGINT;
	case VariantLogicalType::UINT128:
		return LogicalTypeId::UHUGEINT;
	case VariantLogicalType::FLOAT:
		return LogicalTypeId::FLOAT;
	case VariantLogicalType::DOUBLE:
		return LogicalTypeId::DOUBLE;
	case VariantLogicalType::DECIMAL:
		return LogicalTypeId::DECIMAL;
	case VariantLogicalType::VARCHAR:
		return LogicalTypeId::VARCHAR;
	case VariantLogicalType::BLOB:
		return LogicalTypeId::BLOB;
	case VariantLogicalType::UUID:
		return LogicalTypeId::UUID;
	case VariantLogicalType::DATE:
		return LogicalTypeId::DATE;
	case VariantLogicalType::TIME_MICROS:
		return LogicalTypeId::TIME;
	case VariantLogicalType::TIME_NANOS:
		return LogicalTypeId::TIME_NS;
	case VariantLogicalType::TIMESTAMP_SEC:
		return LogicalTypeId::TIMESTAMP_SEC;
	case VariantLogicalType::TIMESTAMP_MILIS:
		return LogicalTypeId::TIMESTAMP_MS;
	case VariantLogicalType::TIMESTAMP_MICROS:
		return LogicalTypeId::TIMESTAMP;
	case VariantLogicalType::TIMESTAMP_NANOS:
		return LogicalTypeId::TIMESTAMP_NS;
	case VariantLogicalType::TIME_MICROS_TZ:
		return LogicalTypeId::TIME_TZ;
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return LogicalTypeId::TIMESTAMP_TZ;
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return LogicalTypeId::TIMESTAMP_TZ_NS;
	case VariantLogicalType::INTERVAL:
		return LogicalTypeId::INTERVAL;
	case VariantLogicalType::OBJECT:
		return LogicalTypeId::STRUCT;
	case VariantLogicalType::ARRAY:
		return LogicalTypeId::LIST;
	case VariantLogicalType::BIGNUM:
		return LogicalTypeId::BIGNUM;
	case VariantLogicalType::BITSTRING:
		return LogicalTypeId::BIT;
	case VariantLogicalType::GEOMETRY:
		return LogicalTypeId::GEOMETRY;
	case VariantLogicalType::GEOGRAPHY:
		return LogicalTypeId::GEOGRAPHY;
	default:
		throw InternalException("Unrecognized VariantLogicalType %d", static_cast<int>(type));
	}
}

bool TryVariantTypeFromLogicalTypeId(LogicalTypeId id, VariantLogicalType &result, bool bool_value) {
	switch (id) {
	case LogicalTypeId::BOOLEAN:
		result = bool_value ? VariantLogicalType::BOOL_TRUE : VariantLogicalType::BOOL_FALSE;
		return true;
	case LogicalTypeId::SQLNULL:
		result = VariantLogicalType::VARIANT_NULL;
		return true;
	case LogicalTypeId::TINYINT:
		result = VariantLogicalType::INT8;
		return true;
	case LogicalTypeId::SMALLINT:
		result = VariantLogicalType::INT16;
		return true;
	case LogicalTypeId::INTEGER:
		result = VariantLogicalType::INT32;
		return true;
	case LogicalTypeId::BIGINT:
		result = VariantLogicalType::INT64;
		return true;
	case LogicalTypeId::HUGEINT:
		result = VariantLogicalType::INT128;
		return true;
	case LogicalTypeId::UTINYINT:
		result = VariantLogicalType::UINT8;
		return true;
	case LogicalTypeId::USMALLINT:
		result = VariantLogicalType::UINT16;
		return true;
	case LogicalTypeId::UINTEGER:
		result = VariantLogicalType::UINT32;
		return true;
	case LogicalTypeId::UBIGINT:
		result = VariantLogicalType::UINT64;
		return true;
	case LogicalTypeId::UHUGEINT:
		result = VariantLogicalType::UINT128;
		return true;
	case LogicalTypeId::FLOAT:
		result = VariantLogicalType::FLOAT;
		return true;
	case LogicalTypeId::DOUBLE:
		result = VariantLogicalType::DOUBLE;
		return true;
	case LogicalTypeId::DECIMAL:
		result = VariantLogicalType::DECIMAL;
		return true;
	case LogicalTypeId::VARCHAR:
		result = VariantLogicalType::VARCHAR;
		return true;
	case LogicalTypeId::BLOB:
		result = VariantLogicalType::BLOB;
		return true;
	case LogicalTypeId::UUID:
		result = VariantLogicalType::UUID;
		return true;
	case LogicalTypeId::DATE:
		result = VariantLogicalType::DATE;
		return true;
	case LogicalTypeId::TIME:
		result = VariantLogicalType::TIME_MICROS;
		return true;
	case LogicalTypeId::TIME_NS:
		result = VariantLogicalType::TIME_NANOS;
		return true;
	case LogicalTypeId::TIMESTAMP_SEC:
		result = VariantLogicalType::TIMESTAMP_SEC;
		return true;
	case LogicalTypeId::TIMESTAMP_MS:
		result = VariantLogicalType::TIMESTAMP_MILIS;
		return true;
	case LogicalTypeId::TIMESTAMP:
		result = VariantLogicalType::TIMESTAMP_MICROS;
		return true;
	case LogicalTypeId::TIMESTAMP_NS:
		result = VariantLogicalType::TIMESTAMP_NANOS;
		return true;
	case LogicalTypeId::TIME_TZ:
		result = VariantLogicalType::TIME_MICROS_TZ;
		return true;
	case LogicalTypeId::TIMESTAMP_TZ:
		result = VariantLogicalType::TIMESTAMP_MICROS_TZ;
		return true;
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		result = VariantLogicalType::TIMESTAMP_NANOS_TZ;
		return true;
	case LogicalTypeId::INTERVAL:
		result = VariantLogicalType::INTERVAL;
		return true;
	case LogicalTypeId::STRUCT:
		result = VariantLogicalType::OBJECT;
		return true;
	case LogicalTypeId::LIST:
		result = VariantLogicalType::ARRAY;
		return true;
	case LogicalTypeId::BIGNUM:
		result = VariantLogicalType::BIGNUM;
		return true;
	case LogicalTypeId::BIT:
		result = VariantLogicalType::BITSTRING;
		return true;
	case LogicalTypeId::GEOMETRY:
		result = VariantLogicalType::GEOMETRY;
		return true;
	case LogicalTypeId::GEOGRAPHY:
		result = VariantLogicalType::GEOGRAPHY;
		return true;
	default:
		return false;
	}
}

LogicalType VariantDefaultType(LogicalTypeId id) {
	switch (id) {
	case LogicalTypeId::STRUCT:
		return LogicalType::STRUCT(child_list_t<LogicalType>());
	case LogicalTypeId::LIST:
		return LogicalType::LIST(LogicalType::VARIANT());
	case LogicalTypeId::GEOMETRY:
		return LogicalType::GEOMETRY();
	case LogicalTypeId::GEOGRAPHY:
		return LogicalType::GEOGRAPHY();
	default:
		return LogicalType(id);
	}
}

VariantLogicalType VariantTypeFromLogicalTypeId(LogicalTypeId id, bool bool_value) {
	VariantLogicalType result;
	if (!TryVariantTypeFromLogicalTypeId(id, result, bool_value)) {
		throw InternalException("LogicalTypeId %s cannot be stored in a VARIANT", EnumUtil::ToString(id));
	}
	return result;
}

UnifiedVariantVectorData::UnifiedVariantVectorData(const RecursiveUnifiedVectorFormat &variant)
    : variant(variant), keys(UnifiedVariantVector::GetKeys(variant)),
      keys_entry(UnifiedVariantVector::GetKeysEntry(variant)), children(UnifiedVariantVector::GetChildren(variant)),
      keys_index(UnifiedVariantVector::GetChildrenKeysIndex(variant)),
      values_index(UnifiedVariantVector::GetChildrenValuesIndex(variant)),
      values(UnifiedVariantVector::GetValues(variant)), type_id(UnifiedVariantVector::GetValuesTypeId(variant)),
      byte_offset(UnifiedVariantVector::GetValuesByteOffset(variant)), data(UnifiedVariantVector::GetData(variant)),
      keys_index_validity(keys_index.validity) {
	blob_data = data.GetData<string_t>();
	type_id_data = type_id.GetData<uint8_t>();
	byte_offset_data = byte_offset.GetData<uint32_t>();
	keys_index_data = keys_index.GetData<uint32_t>();
	values_index_data = values_index.GetData<uint32_t>();
	values_data = values.GetData<list_entry_t>();
	children_data = children.GetData<list_entry_t>();
	keys_data = keys.GetData<list_entry_t>();
	keys_entry_data = keys_entry.GetData<string_t>();
}

bool UnifiedVariantVectorData::RowIsValid(idx_t row) const {
	return variant.unified.validity.RowIsValid(variant.unified.sel->get_index(row));
}
bool UnifiedVariantVectorData::KeysIndexIsValid(idx_t row, idx_t index) const {
	auto list_entry = GetChildrenListEntry(row);
	return keys_index_validity.RowIsValid(keys_index.sel->get_index(list_entry.offset + index));
}

list_entry_t UnifiedVariantVectorData::GetChildrenListEntry(idx_t row) const {
	return children_data[children.sel->get_index(row)];
}
list_entry_t UnifiedVariantVectorData::GetValuesListEntry(idx_t row) const {
	return values_data[values.sel->get_index(row)];
}
const string_t &UnifiedVariantVectorData::GetKey(idx_t row, idx_t index) const {
	auto list_entry = keys_data[keys.sel->get_index(row)];
	return keys_entry_data[keys_entry.sel->get_index(list_entry.offset + index)];
}
uint32_t UnifiedVariantVectorData::GetKeysIndex(idx_t row, idx_t child_index) const {
	auto list_entry = GetChildrenListEntry(row);
	return keys_index_data[keys_index.sel->get_index(list_entry.offset + child_index)];
}

idx_t UnifiedVariantVectorData::GetKeysCount(idx_t row) const {
	auto list_entry = keys_data[keys.sel->get_index(row)];
	return list_entry.length;
}

uint32_t UnifiedVariantVectorData::GetValuesIndex(idx_t row, idx_t child_index) const {
	auto list_entry = GetChildrenListEntry(row);
	return values_index_data[values_index.sel->get_index(list_entry.offset + child_index)];
}
VariantLogicalType UnifiedVariantVectorData::GetTypeId(idx_t row, idx_t value_index) const {
	auto list_entry = values_data[values.sel->get_index(row)];
	return static_cast<VariantLogicalType>(type_id_data[type_id.sel->get_index(list_entry.offset + value_index)]);
}
uint32_t UnifiedVariantVectorData::GetByteOffset(idx_t row, idx_t value_index) const {
	auto list_entry = values_data[values.sel->get_index(row)];
	return byte_offset_data[byte_offset.sel->get_index(list_entry.offset + value_index)];
}
const string_t &UnifiedVariantVectorData::GetData(idx_t row) const {
	return blob_data[data.sel->get_index(row)];
}

} // namespace duckdb
