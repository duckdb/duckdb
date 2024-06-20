//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb_extension_api.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// This is a demo of how a function-pointer + header file based API would allow C API extensions
typedef struct {
  idx_t (*duckdb_data_chunk_get_size)(duckdb_data_chunk chunk);
  duckdb_vector (*duckdb_data_chunk_get_vector)(duckdb_data_chunk chunk, idx_t col_idx);
  void *(*duckdb_vector_get_data)(duckdb_vector vector);
  uint64_t *(*duckdb_vector_get_validity)(duckdb_vector vector);
  void (*duckdb_vector_ensure_validity_writable)(duckdb_vector vector);
  bool (*duckdb_validity_row_is_valid)(uint64_t *validity, idx_t row);
  void (*duckdb_validity_set_row_invalid)(uint64_t *validity, idx_t row);
  duckdb_scalar_function (*duckdb_create_scalar_function)();
  void (*duckdb_scalar_function_set_name)(duckdb_scalar_function scalar_function, const char *name);
  duckdb_logical_type (*duckdb_create_logical_type)(duckdb_type type);
  void (*duckdb_scalar_function_add_parameter)(duckdb_scalar_function scalar_function, duckdb_logical_type type);
  void (*duckdb_scalar_function_set_return_type)(duckdb_scalar_function scalar_function, duckdb_logical_type type);
  void (*duckdb_destroy_logical_type)(duckdb_logical_type *type);
  void (*duckdb_scalar_function_set_function)(duckdb_scalar_function scalar_function, duckdb_scalar_function_t function);
  duckdb_state (*duckdb_register_scalar_function)(duckdb_connection con, duckdb_scalar_function scalar_function);
  void (*duckdb_destroy_scalar_function)(duckdb_scalar_function *scalar_function);
} duckdb_extension_api;

#ifdef __cplusplus
}
#endif
