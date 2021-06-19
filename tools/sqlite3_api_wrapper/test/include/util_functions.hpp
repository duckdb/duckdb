#pragma once

#include "sqlite3.h"
#include <string>
#include <vector>

#include "duckdb/common/constants.hpp"

static int concatenate_results(void *arg, int ncols, char **vals, char **colnames) {
	auto &results = *((std::vector<std::vector<std::string>> *)arg);
	if (results.size() == 0) {
		results.resize(ncols);
	}
	for (int i = 0; i < ncols; i++) {
		results[i].push_back(vals[i] ? vals[i] : "NULL");
	}
	return SQLITE_OK;
}

static void print_result(std::vector<std::vector<std::string>> &results) {
	if (results.empty()) {
		return;
	}
	for (duckdb::idx_t row_idx = 0; row_idx < results[0].size(); row_idx++) {
		for (duckdb::idx_t col_idx = 0; col_idx < results.size(); col_idx++) {
			printf("%s|", results[col_idx][row_idx].c_str());
		}
		printf("\n");
	}
}
