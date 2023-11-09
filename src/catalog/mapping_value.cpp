#include "duckdb/catalog/mapping_value.hpp"

namespace duckdb {

void MappingValue::SetTimestamp(transaction_t timestamp) {
	this->timestamp = timestamp;
}

transaction_t MappingValue::GetTimestamp() const {
	return timestamp;
}

CatalogEntry &MappingValue::GetEntry() {
	auto &entry_value = GetEntryValue();
	return entry_value.Entry();
}

catalog_entry_t MappingValue::GetIndex() {
	return index;
}

void MappingValue::SetEntry(unique_ptr<CatalogEntry> entry) {
	auto &entry_value = GetEntryValue();
	entry_value.SetEntry(std::move(entry));
}

EntryValue &MappingValue::GetEntryValue() {
	auto entry = set.entries.find(index);
	if (entry == set.entries.end()) {
		throw InternalException("MappingValue - Catalog entry not found!?");
	}
	return entry->second;
}

} // namespace duckdb
