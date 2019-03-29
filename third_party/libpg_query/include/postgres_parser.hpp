#pragma once

#include <string>

extern "C" {
typedef uint32_t Oid;
typedef int16_t int16;

#include "nodes/pg_list.h"
}

namespace postgres {

class PostgresParser {
public:
	PostgresParser();
	void Parse(std::string query);
	~PostgresParser();

	bool success;
	List* parse_tree;
	std::string error_message;
	int error_location;
};
}
