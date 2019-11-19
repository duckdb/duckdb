#pragma once

#include <string>
#include "nodes/pg_list.hpp"

namespace postgres {

class PostgresParser {
public:
	PostgresParser();
	void Parse(std::string query);
	~PostgresParser();

	bool success;
	List *parse_tree;
	std::string error_message;
	int error_location;
};
} // namespace postgres
