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
	PGList *parse_tree;
	std::string error_message;
	int error_location;
};
}
