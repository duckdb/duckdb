#ifndef DUCKDB_CONNECT_HELPERS_H
#define DUCKDB_CONNECT_HELPERS_H

#include "common.h"

// Checks if config is correctly set
void CheckConfig(SQLHANDLE &dbc, const std::string &setting, const std::string &expected_content);

// Check if database is correctly set
void CheckDatabase(SQLHANDLE &dbc);

#endif // DUCKDB_CONNECT_HELPERS_H
