#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb.hpp"

namespace imdb {
//! Adds the IMDB tables to the database
void dbgen(duckdb::DuckDB &database);

//! Gets the specified IMDB JOB Query number as a string
std::string get_query(int query);

} // namespace imdb
