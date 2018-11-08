
#include "duckdb.hpp"
#include "sqlite3.h"

namespace sqlite {

//! Transfer all data inside the DuckDB connection to the given sqlite database
bool TransferDatabase(duckdb::DuckDBConnection &con, sqlite3 *sqlite);

//! Fires a query against both DuckDB and sqlite and compares the results of the
//! two
bool CompareQuery(duckdb::DuckDBConnection &con, sqlite3 *sqlite,
                  std::string query);

}; // namespace sqlite
