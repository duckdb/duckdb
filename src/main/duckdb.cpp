
#include "duckdb.hpp"

#include "parser/parser.hpp"

DuckDB::DuckDB(const char *path) {}

DuckDBConnection::DuckDBConnection(DuckDB &database) {}

DuckDBResult DuckDBConnection::Query(const char *query) {
	Parser parser;
	if (!parser.ParseQuery(query)) {
		fprintf(stderr, "Failure to parse: %s\n",
		        parser.GetErrorMessage().c_str());
		return DuckDBResult(parser.GetErrorMessage());
	}
	return DuckDBResult();
}

DuckDBResult::DuckDBResult() : success(true) {}

DuckDBResult::DuckDBResult(std::string error) : success(false), error(error) {}