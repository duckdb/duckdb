//===----------------------------------------------------------------------===//
//                         DuckDB
//
// postgres_parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include "nodes/pg_list.hpp"
#include "pg_simplified_token.hpp"

namespace duckdb {
class PostgresParser {
public:
	PostgresParser();
	~PostgresParser();

	bool success;
	duckdb_libpgquery::PGList *parse_tree;
	std::string error_message;
	int error_location;
public:
	void Parse(std::string query);
	static std::vector<duckdb_libpgquery::PGSimplifiedToken> Tokenize(std::string query);

	static bool IsKeyword(const std::string &text);
};
}
