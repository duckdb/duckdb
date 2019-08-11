//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate/algebraic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function.hpp"

namespace duckdb {

// avg
index_t avg_payload_size(TypeId return_type);
void avg_initialize(data_ptr_t payload, TypeId return_type);
void avg_update(Vector inputs[], index_t input_count, Vector &result);
void avg_finalize(Vector &payloads, Vector &result);
SQLType avg_get_return_type(vector<SQLType> &arguments);

// covar_samp/covar_pop
void covar_update(Vector inputs[], index_t input_count, Vector &result);
void covarpop_finalize(Vector &payloads, Vector &result);
void covarsamp_finalize(Vector &payloads, Vector &result);
SQLType covar_get_return_type(vector<SQLType> &arguments);
index_t covar_state_size(TypeId return_type);
void covar_initialize(data_ptr_t payload, TypeId return_type);

// stddev_samp, stddev_pop, var_pop, var_samp
void stddevsamp_update(Vector inputs[], index_t input_count, Vector &result);
void stddevsamp_finalize(Vector &payloads, Vector &result);
void stddevpop_finalize(Vector &payloads, Vector &result);
void varsamp_finalize(Vector &payloads, Vector &result);
void varpop_finalize(Vector &payloads, Vector &result);
SQLType stddev_get_return_type(vector<SQLType> &arguments);
index_t stddev_state_size(TypeId return_type);
void stddevsamp_initialize(data_ptr_t payload, TypeId return_type);


}
