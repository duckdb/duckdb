//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"

namespace duckdb {

void gather_finalize(Vector &payloads, Vector &result);

index_t get_bigint_type_size(TypeId return_type);
void bigint_payload_initialize(data_ptr_t payload, TypeId return_type);
Value bigint_simple_initialize();
SQLType get_bigint_return_type(vector<SQLType> &arguments);

index_t get_return_type_size(TypeId return_type);

void null_state_initialize(data_ptr_t state, TypeId return_type);
Value null_simple_initialize();

SQLType get_same_return_type(vector<SQLType> &arguments);

// count(*)
void countstar_update(Vector inputs[], index_t input_count, Vector &result);
void countstar_simple_update(Vector inputs[], index_t input_count, Value &result);
// count
void count_update(Vector inputs[], index_t input_count, Vector &result);
void count_simple_update(Vector inputs[], index_t input_count, Value &result);
// first
void first_update(Vector inputs[], index_t input_count, Vector &result);
// max
void max_update(Vector inputs[], index_t input_count, Vector &result);
void max_simple_update(Vector inputs[], index_t input_count, Value &result);
// min
void min_update(Vector inputs[], index_t input_count, Vector &result);
void min_simple_update(Vector inputs[], index_t input_count, Value &result);
// sum
void sum_update(Vector inputs[], index_t input_count, Vector &result);
void sum_simple_update(Vector inputs[], index_t input_count, Value &result);
SQLType sum_get_return_type(vector<SQLType> &arguments);
// string_agg
void string_agg_update(Vector inputs[], index_t input_count, Vector &result);
SQLType string_agg_get_return_type(vector<SQLType> &arguments);

}
