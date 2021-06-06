#pragma once

#include "sqlite3.h"
#include <string>
#include <vector>

#include "duckdb/common/constants.hpp"

using namespace std;
using namespace duckdb;

static int concatenate_results(void *arg, int ncols, char **vals, char **colnames) {
	auto &results = *((vector<vector<string>> *)arg);
	if (results.size() == 0) {
		results.resize(ncols);
	}
	for (int i = 0; i < ncols; i++) {
		results[i].push_back(vals[i] ? vals[i] : "");
	}
	return SQLITE_OK;
}

static void print_result(vector<vector<string>> &results) {
	if(results.empty()) {
		return;
	}
	for (idx_t row_idx = 0; row_idx < results[0].size(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < results.size(); col_idx++) {
			printf("%s|", results[col_idx][row_idx].c_str());
		}
		printf("\n");
	}
}
