#include "duckdb/catalog/catalog_entry/table_column_serial_type.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

const case_insensitive_map_t<LogicalType> SerialColumnType::serial_type_map = {
    {"smallserial", LogicalType::SMALLINT}, {"serial2", LogicalType::SMALLINT}, {"serial", LogicalType::INTEGER},
    {"serial4", LogicalType::INTEGER},      {"bigserial", LogicalType::BIGINT}, {"serial8", LogicalType::BIGINT}};

} // namespace duckdb
