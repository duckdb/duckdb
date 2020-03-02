//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// tpce.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb.hpp"

namespace tpce {
//! Adds the TPC-E tables filled with the given SF to the catalog. Suffix adds a
//! suffix to the table names, if given. SF=0 will only add the schema
//! information.
void dbgen(duckdb::DuckDB &database, uint32_t sf = 500, std::string schema = DEFAULT_SCHEMA, std::string suffix = "");

} // namespace tpce
