#include "duckdb/catalog/catalog_entry/table_column_serial_type.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

const case_insensitive_map_t<LogicalType> SerialColumnType::serial_type_map = {
    {"smallserial", LogicalType::SMALLINT}, {"serial2", LogicalType::SMALLINT}, {"serial", LogicalType::INTEGER},
    {"serial4", LogicalType::INTEGER},      {"bigserial", LogicalType::BIGINT}, {"serial8", LogicalType::BIGINT}};

bool SerialColumnType::IsColumnSerial(const LogicalTypeId &type_id, const string &col_type_name) {
	if (type_id != LogicalTypeId::USER) {
		return false;
	}

	auto serial_find = SerialColumnType::serial_type_map.find(col_type_name);
	if (serial_find != SerialColumnType::serial_type_map.end()) {
		return true;
	}

	return false;
}

} // namespace duckdb
