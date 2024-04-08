#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct HTTPFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterHTTPRequestFunction(db);
	}

private:
	//! Register HTTPRequest functions
	static void RegisterHTTPRequestFunction(DatabaseInstance &db);
};

} // namespace duckdb
