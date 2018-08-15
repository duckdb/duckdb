//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// dbgen.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.hpp"
#include "common/types/data_chunk.hpp"
#include "duckdb.hpp"

namespace tpch {
//! Adds the TPC-H tables filled with the given SF to the catalog. Suffix adds a
//! suffix to the table names, if given. SF=0 will only add the schema
//! information.
void dbgen(double sf, duckdb::Catalog &catalog,
           std::string schema = DEFAULT_SCHEMA, std::string suffix = "");

//! Gets the specified TPC-H Query number as a string
std::string get_query(int query);
//! Checks if the result for the specified query number are correct
bool check_result(double sf, int query, duckdb::DuckDBResult &result,
                  std::string &error_message);

//! Compares the result of a pipe-delimited CSV with the given DataChunk
//! Returns true if they are equal, and stores an error_message otherwise
bool compare_result(const char *csv, duckdb::DataChunk &result, bool has_header,
                    std::string &error_message);
} // namespace tpch