#pragma once

#include <string>
#include "nodes/pg_list.hpp"

namespace duckdb {
class PostgresParser {
public:
	PostgresParser();
	void Parse(std::string query);
	~PostgresParser();

	bool success;
	duckdb_libpgquery::PGList *parse_tree;
	std::string error_message;
	int error_location;

	static std::string FormatErrorMessage(std::string query, std::string error_message, int error_location);
};
}
