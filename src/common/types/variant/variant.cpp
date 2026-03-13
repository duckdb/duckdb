#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

VariantVectorData::VariantVectorData(Vector &variant)
    : variant(variant), keys_index_validity(FlatVector::Validity(VariantVector::GetChildrenKeysIndex(variant))),
      keys(VariantVector::GetKeys(variant)) {
	blob_data = FlatVector::GetData<string_t>(VariantVector::GetData(variant));
	type_ids_data = FlatVector::GetData<uint8_t>(VariantVector::GetValuesTypeId(variant));
	byte_offset_data = FlatVector::GetData<uint32_t>(VariantVector::GetValuesByteOffset(variant));
	keys_index_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenKeysIndex(variant));
	values_index_data = FlatVector::GetData<uint32_t>(VariantVector::GetChildrenValuesIndex(variant));
	values_data = FlatVector::GetData<list_entry_t>(VariantVector::GetValues(variant));
	children_data = FlatVector::GetData<list_entry_t>(VariantVector::GetChildren(variant));
	keys_data = FlatVector::GetData<list_entry_t>(keys);
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
