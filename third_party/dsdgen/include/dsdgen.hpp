#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb.hpp"

namespace tpcds {
//! Adds the TPC-DS tables filled with the given SF to the catalog. Suffix adds
//! a suffix to the table names, if given. SF=0 will only add the schema
//! information.
void dbgen(double sf, duckdb::DuckDB &database, std::string schema = DEFAULT_SCHEMA, std::string suffix = "");

//! Gets the specified TPC-DS Query number as a string
std::string get_query(int query);
//! Returns the CSV answer of a TPC-DS query
std::string get_answer(double sf, int query);
} // namespace tpcds
