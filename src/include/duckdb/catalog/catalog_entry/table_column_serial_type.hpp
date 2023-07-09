//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_column_serial_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

// The "Serial" family (e.g., smallserial, serial, bigserial) is not
// considered as actual types. Therefore, the SerialColumnType is primarily
// used to map the serial family to their corresponding actual types.
class SerialColumnType {
public:
	static const case_insensitive_map_t<LogicalType> serial_type_map;

public:
	static bool IsColumnSerial(const LogicalTypeId &type, const string &col_type_name);
};

} // namespace duckdb
