#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

StorageIndex::StorageIndex()
    : has_index(true), index(COLUMN_IDENTIFIER_ROW_ID), index_type(StorageIndexType::FULL_READ) {
}
StorageIndex::StorageIndex(idx_t index) : has_index(true), index(index), index_type(StorageIndexType::FULL_READ) {
}
StorageIndex::StorageIndex(const string &field)
    : has_index(false), field(field), index_type(StorageIndexType::FULL_READ) {
}
StorageIndex::StorageIndex(idx_t index, vector<StorageIndex> child_indexes_p)
    : has_index(true), index(index), index_type(StorageIndexType::FULL_READ),
      child_indexes(std::move(child_indexes_p)) {
}
StorageIndex::StorageIndex(const string &field, vector<StorageIndex> child_indexes_p)
    : has_index(false), field(field), index_type(StorageIndexType::FULL_READ),
      child_indexes(std::move(child_indexes_p)) {
}

StorageIndex StorageIndex::FromColumnIndex(const ColumnIndex &column_id) {
	vector<StorageIndex> result;
	for (auto &child_id : column_id.GetChildIndexes()) {
		result.push_back(StorageIndex::FromColumnIndex(child_id));
	}
	StorageIndex storage_index;
	if (column_id.HasPrimaryIndex()) {
		storage_index = StorageIndex(column_id.GetPrimaryIndex(), std::move(result));
	} else {
		storage_index = StorageIndex(column_id.GetFieldName(), std::move(result));
	}
	if (column_id.HasType()) {
		storage_index.SetType(column_id.GetType());
	}
	if (column_id.IsPushdownExtract()) {
		storage_index.SetPushdownExtract();
	}
	return storage_index;
}

bool StorageIndex::HasPrimaryIndex() const {
	return has_index;
}
idx_t StorageIndex::GetPrimaryIndex() const {
	D_ASSERT(has_index);
	return index;
}
const string &StorageIndex::GetFieldName() const {
	D_ASSERT(!has_index);
	return field;
}
PhysicalIndex StorageIndex::ToPhysical() const {
	D_ASSERT(has_index);
	return PhysicalIndex(index);
}
bool StorageIndex::HasType() const {
	return type.id() != LogicalTypeId::INVALID;
}
const LogicalType &StorageIndex::GetScanType() const {
	D_ASSERT(HasType());
	if (IsPushdownExtract()) {
		return child_indexes[0].GetScanType();
	}
	return GetType();
}
const LogicalType &StorageIndex::GetType() const {
	return type;
}
bool StorageIndex::HasChildren() const {
	return !child_indexes.empty();
}
idx_t StorageIndex::ChildIndexCount() const {
	return child_indexes.size();
}
const StorageIndex &StorageIndex::GetChildIndex(idx_t idx) const {
	return child_indexes[idx];
}
StorageIndex &StorageIndex::GetChildIndex(idx_t idx) {
	return child_indexes[idx];
}
const vector<StorageIndex> &StorageIndex::GetChildIndexes() const {
	return child_indexes;
}
void StorageIndex::AddChildIndex(StorageIndex new_index) {
	this->child_indexes.push_back(std::move(new_index));
}
void StorageIndex::SetType(const LogicalType &type_information) {
	type = type_information;
}
void StorageIndex::SetPushdownExtract() {
	D_ASSERT(!IsPushdownExtract());
	index_type = StorageIndexType::PUSHDOWN_EXTRACT;
}
bool StorageIndex::IsPushdownExtract() const {
	return index_type == StorageIndexType::PUSHDOWN_EXTRACT;
}
void StorageIndex::SetIndex(idx_t new_index) {
	D_ASSERT(has_index);
	index = new_index;
}
bool StorageIndex::IsRowIdColumn() const {
	if (!has_index) {
		return false;
	}
	return index == DConstants::INVALID_INDEX;
}

} // namespace duckdb
