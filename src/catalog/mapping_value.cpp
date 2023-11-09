#include "duckdb/catalog/mapping_value.hpp"

namespace duckdb {

void MappingValue::SetTimestamp(transaction_t timestamp) {
	this->timestamp = timestamp;
}

transaction_t MappingValue::GetTimestamp() const {
	return timestamp;
}

EntryIndex &MappingValue::Index() {
	return index;
}

CatalogEntry &MappingValue::GetEntry() {
	return index.GetEntry();
}

catalog_entry_t MappingValue::GetIndex() {
	return index.GetIndex();
}

void MappingValue::SetEntry(unique_ptr<CatalogEntry> entry) {
	index.SetEntry(std::move(entry));
}

} // namespace duckdb
