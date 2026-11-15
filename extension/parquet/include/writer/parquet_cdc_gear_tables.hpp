//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/parquet_cdc_gear_tables.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Number of gear hash tables. A page boundary needs one mask hit per table, in order; see
//! ParquetContentDefinedChunker.
static constexpr idx_t PARQUET_CDC_NUM_GEAR_TABLES = 8;

//! Arrow's gear hash tables; the values and the recipe that produces them are in
//! writer/parquet_cdc_gear_tables.cpp
extern const uint64_t PARQUET_CDC_GEAR_TABLES[PARQUET_CDC_NUM_GEAR_TABLES][256];

} // namespace duckdb
