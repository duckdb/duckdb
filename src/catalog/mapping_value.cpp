#include "duckdb/catalog/mapping_value.hpp"

namespace duckdb {

void MappingValue::SetTimestamp(transaction_t timestamp) {
	this->timestamp = timestamp;
}

transaction_t MappingValue::GetTimestamp() const {
	return timestamp;
}

} // namespace duckdb
