
#include "duckdb.hpp"

#include "parser/parser.hpp"

DuckDB::DuckDB(const char *path) {}

DuckDBConnection::DuckDBConnection(DuckDB &database) {}

DuckDBResult DuckDBConnection::Query(const char *query) {
	Parser parser;
	parser.ParseQuery(query);
}
