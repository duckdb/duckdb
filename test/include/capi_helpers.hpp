
#pragma once

#include "duckdb.h"
#include <cmath>
#include <memory>
#include <vector>
#include <string>

template <class T>
T get_numeric(duckdb_column column, size_t index);

int64_t get_numeric(duckdb_column column, size_t row);

extern int64_t NULL_NUMERIC;
extern double NULL_DECIMAL;

bool CHECK_NUMERIC_COLUMN(duckdb_result result, size_t column,
                                 std::vector<int64_t> values);
bool CHECK_DECIMAL_COLUMN(duckdb_result result, size_t column,
                                 std::vector<double> values);
bool CHECK_NUMERIC(duckdb_result result, size_t row, size_t column,
                          int64_t value);
bool CHECK_STRING(duckdb_result result, size_t row, size_t column,
                         std::string value);
bool CHECK_STRING_COLUMN(duckdb_result result, size_t column,
                                std::vector<std::string> values);
