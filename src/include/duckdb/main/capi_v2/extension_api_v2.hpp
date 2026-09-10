#pragma once

#include "duckdb_v2.h"

//===--------------------------------------------------------------------===//
// Function pointer struct
//===--------------------------------------------------------------------===//
typedef struct {
	// v2.0.0
	DUCKDB_V2_ERROR(*duckdb_v2_arena_allocate)
	(duckdb_v2_arena_handle arena, idx_t byte_len, uint8_t **out_ptr, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_bignum_decode)
	(const uint8_t *in_data, idx_t in_length, uint8_t *out_data, idx_t out_capacity, idx_t *out_length,
	 bool *out_is_negative, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_bignum_encode)
	(const uint8_t *in_data, idx_t in_length, bool is_negative, uint8_t *out_data, idx_t out_capacity,
	 idx_t *out_length, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_close)(duckdb_v2_database_handle *db);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_append)
	(duckdb_v2_column_data_collection_handle collection, duckdb_v2_column_data_collection_append_state_handle state,
	 duckdb_v2_data_chunk_handle chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_append_state_create)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_append_state_handle *out_state, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_append_state_destroy)
	(duckdb_v2_column_data_collection_append_state_handle *state);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_combine)
	(duckdb_v2_column_data_collection_handle target, duckdb_v2_column_data_collection_handle *source,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_create_with_connection)
	(duckdb_v2_connection_handle conn, const duckdb_v2_logical_type_handle *types_array, idx_t types_count,
	 duckdb_v2_column_data_collection_handle *out_collection, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_create_with_context)
	(duckdb_v2_context_handle context, const duckdb_v2_logical_type_handle *types_array, idx_t types_count,
	 duckdb_v2_column_data_collection_handle *out_collection, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_column_data_collection_destroy)(duckdb_v2_column_data_collection_handle *collection);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_reset)
	(duckdb_v2_column_data_collection_handle collection, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_row_count)
	(duckdb_v2_column_data_collection_handle collection, idx_t *out_row_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_scan)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_shared_scan_state_handle shared_state,
	 duckdb_v2_column_data_collection_worker_scan_state_handle worker_state, duckdb_v2_data_chunk_handle out_chunk,
	 bool *did_produce_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_shared_scan_state_create)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_shared_scan_state_handle *out_state, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_shared_scan_state_destroy)
	(duckdb_v2_column_data_collection_shared_scan_state_handle *state);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_worker_scan_state_create)
	(duckdb_v2_column_data_collection_handle collection,
	 duckdb_v2_column_data_collection_worker_scan_state_handle *out_state, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_worker_scan_state_destroy)
	(duckdb_v2_column_data_collection_worker_scan_state_handle *state);
	DUCKDB_V2_ERROR(*duckdb_v2_connect)
	(duckdb_v2_database_handle db, duckdb_v2_connection_handle *out_conn, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_create_type_from_id)
	(duckdb_v2_connection_handle conn, DUCKDB_V2_LOGICAL_TYPE_ID type_id, const duckdb_v2_identifier_t *param_names,
	 const duckdb_v2_value_handle *param_values, idx_t param_count, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_create_type_from_name)
	(duckdb_v2_connection_handle conn, duckdb_v2_qname_handle name, const duckdb_v2_identifier_t *param_names,
	 const duckdb_v2_value_handle *param_values, idx_t param_count, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_create_type_from_text)
	(duckdb_v2_connection_handle conn, duckdb_v2_str text, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_create_type_with_alias)
	(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle base_type, duckdb_v2_identifier_t alias_name,
	 duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_interrupt)
	(duckdb_v2_connection_handle conn, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_get)
	(duckdb_v2_connection_handle conn, duckdb_v2_identifier_t name, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_get_by_index)
	(duckdb_v2_connection_handle conn, idx_t index, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_get_count)
	(duckdb_v2_connection_handle conn, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_option_set)
	(duckdb_v2_connection_handle conn, duckdb_v2_option_handle option, DUCKDB_V2_SETTING_SCOPE scope,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_query_progress)
	(duckdb_v2_connection_handle conn, duckdb_v2_query_progress_handle *out_progress, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_context_create_type_from_id)
	(duckdb_v2_context_handle ctx, DUCKDB_V2_LOGICAL_TYPE_ID type_id, const duckdb_v2_identifier_t *param_names,
	 const duckdb_v2_value_handle *param_values, idx_t param_count, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_context_create_type_from_name)
	(duckdb_v2_context_handle ctx, duckdb_v2_qname_handle name, const duckdb_v2_identifier_t *param_names,
	 const duckdb_v2_value_handle *param_values, idx_t param_count, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_context_create_type_from_text)
	(duckdb_v2_context_handle ctx, duckdb_v2_str text, duckdb_v2_logical_type_handle *out_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_context_create_type_with_alias)
	(duckdb_v2_context_handle ctx, duckdb_v2_logical_type_handle base_type, duckdb_v2_identifier_t alias_name,
	 duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_create_environment)
	(duckdb_v2_environment_handle *out_env, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_copy_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_data_chunk_handle chunk, duckdb_v2_data_chunk_handle *out_chunk,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_copy_with_context)
	(duckdb_v2_context_handle context, duckdb_v2_data_chunk_handle chunk, duckdb_v2_data_chunk_handle *out_chunk,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_create)
	(const duckdb_v2_logical_type_handle *types, idx_t column_count, duckdb_v2_data_chunk_handle *out_chunk,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_create_with_connection)
	(duckdb_v2_connection_handle conn, const duckdb_v2_logical_type_handle *types, idx_t column_count,
	 duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_create_with_context)
	(duckdb_v2_context_handle context, const duckdb_v2_logical_type_handle *types, idx_t column_count,
	 duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_data_chunk_destroy)(duckdb_v2_data_chunk_handle *chunk);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_get_size)
	(duckdb_v2_data_chunk_handle chunk, idx_t *out_size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_get_vector)
	(duckdb_v2_data_chunk_handle chunk, idx_t index, duckdb_v2_vector_handle *out_vector,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_data_chunk_get_vector_count)
	(duckdb_v2_data_chunk_handle chunk, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_get)
	(duckdb_v2_database_handle db, duckdb_v2_identifier_t name, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_get_by_index)
	(duckdb_v2_database_handle db, idx_t index, duckdb_v2_option_handle *out_option, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_get_count)
	(duckdb_v2_database_handle db, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_database_option_set)
	(duckdb_v2_database_handle db, duckdb_v2_option_handle option, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_destroy_environment)(duckdb_v2_environment_handle *env);
	DUCKDB_V2_ERROR (*duckdb_v2_disconnect)(duckdb_v2_connection_handle *conn);
	DUCKDB_V2_ERROR(*duckdb_v2_environment_database_count)
	(duckdb_v2_environment_handle env, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_destroy)(duckdb_v2_error_info_handle *info);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_get_code)(duckdb_v2_error_info_handle info, DUCKDB_V2_ERROR *out_code);
	DUCKDB_V2_ERROR(*duckdb_v2_error_info_get_raw_message)
	(duckdb_v2_error_info_handle info, duckdb_v2_str *out_raw_message);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_get_text)(duckdb_v2_error_info_handle info, duckdb_v2_str *out_text);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_set_code)(duckdb_v2_error_info_handle info, DUCKDB_V2_ERROR code);
	DUCKDB_V2_ERROR (*duckdb_v2_error_info_set_text)(duckdb_v2_error_info_handle info, duckdb_v2_str text);
	DUCKDB_V2_ERROR (*duckdb_v2_library_version)(duckdb_v2_str *out_version, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_copy)
	(duckdb_v2_logical_type_handle type, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_logical_type_destroy)(duckdb_v2_logical_type_handle *type);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_id)
	(duckdb_v2_logical_type_handle type, DUCKDB_V2_LOGICAL_TYPE_ID *out_id, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_name)
	(duckdb_v2_logical_type_handle type, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_param)
	(duckdb_v2_logical_type_handle type, idx_t index, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_get_param_count)
	(duckdb_v2_logical_type_handle type, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_is_equal)
	(duckdb_v2_logical_type_handle left, duckdb_v2_logical_type_handle right, bool *result,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_logical_type_to_text)
	(duckdb_v2_logical_type_handle type, char *out_text, idx_t out_capacity, idx_t *out_length,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_open)
	(duckdb_v2_environment_handle env, duckdb_v2_str path, duckdb_v2_option_handle *options, idx_t option_count,
	 duckdb_v2_database_handle *out_db, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_create)
	(duckdb_v2_identifier_t name, duckdb_v2_str setting, duckdb_v2_option_handle *out_option,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_option_destroy)(duckdb_v2_option_handle *option);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_alias)
	(duckdb_v2_option_handle option, idx_t index, duckdb_v2_identifier_t *out_alias, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_alias_count)
	(duckdb_v2_option_handle option, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_default_setting)
	(duckdb_v2_option_handle option, duckdb_v2_str *out_default_setting, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_description)
	(duckdb_v2_option_handle option, duckdb_v2_str *out_description, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_name)
	(duckdb_v2_option_handle option, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_setting)
	(duckdb_v2_option_handle option, duckdb_v2_str *out_setting, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_option_get_target_scope)
	(duckdb_v2_option_handle option, DUCKDB_V2_OPTION_TARGET_SCOPE *out_target_scope, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_parse_sql)
	(duckdb_v2_connection_handle conn, const char *sql, duckdb_v2_statement_iterator_handle *out_iterator,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_query_progress_destroy)(duckdb_v2_query_progress_handle *progress);
	DUCKDB_V2_ERROR(*duckdb_v2_query_progress_get_percentage)
	(duckdb_v2_query_progress_handle progress, double *out_percentage, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_query_progress_get_rows_processed)
	(duckdb_v2_query_progress_handle progress, uint64_t *out_rows_processed, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_query_progress_get_total_rows_to_process)
	(duckdb_v2_query_progress_handle progress, uint64_t *out_total_rows_to_process, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_result_destroy)(duckdb_v2_result_handle *result);
	DUCKDB_V2_ERROR(*duckdb_v2_result_drain)
	(duckdb_v2_result_handle result, idx_t *out_rows_changed, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_fetch_chunk)
	(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_get_result_type)
	(duckdb_v2_result_handle result, DUCKDB_V2_RESULT_TYPE *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_get_schema)
	(duckdb_v2_result_handle result, duckdb_v2_schema_handle *out_schema, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_get_statement_type)
	(duckdb_v2_result_handle result, DUCKDB_V2_STATEMENT_TYPE *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_render_box)
	(duckdb_v2_result_handle *result, idx_t max_rows, idx_t max_width, idx_t max_col_width, duckdb_v2_str null_value,
	 idx_t render_mode, idx_t limit, duckdb_v2_text_sink_fn sink, void *user_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_step)
	(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk, DUCKDB_V2_RESULT_STEP_STATUS *out_status,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_result_wait)(duckdb_v2_result_handle result, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_schema_destroy)(duckdb_v2_schema_handle *schema);
	DUCKDB_V2_ERROR(*duckdb_v2_schema_get_count)
	(duckdb_v2_schema_handle schema, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_schema_get_field)
	(duckdb_v2_schema_handle schema, idx_t index, duckdb_v2_identifier_t *out_name,
	 duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_sql_statement_destroy)(duckdb_v2_sql_statement_handle *statement);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_bind)
	(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement, duckdb_v2_schema_handle *out_schema,
	 duckdb_v2_schema_handle *out_parameters, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_execute)
	(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement,
	 const duckdb_v2_identifier_t *parameter_names, const duckdb_v2_value_handle *parameter_values,
	 idx_t parameter_count, duckdb_v2_result_handle *out_result, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_statement_iterator_destroy)(duckdb_v2_statement_iterator_handle *iterator);
	DUCKDB_V2_ERROR(*duckdb_v2_statement_iterator_next)
	(duckdb_v2_statement_iterator_handle iterator, duckdb_v2_sql_statement_handle *out_statement,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_cast_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_value_handle value, duckdb_v2_logical_type_handle target_type,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_cast_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_value_handle value, duckdb_v2_logical_type_handle target_type,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_array_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle child_type, const duckdb_v2_value_handle *children,
	 idx_t child_count, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_array_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_logical_type_handle child_type, const duckdb_v2_value_handle *children,
	 idx_t child_count, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bigint_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bigint_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bignum_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bignum_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bit_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bit_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_blob_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_blob_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bool_with_connection)
	(duckdb_v2_connection_handle conn, bool in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_bool_with_context)
	(duckdb_v2_context_handle ctx, bool in_value, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_date_with_connection)
	(duckdb_v2_connection_handle conn, int32_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_date_with_context)
	(duckdb_v2_context_handle ctx, int32_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_decimal_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_hugeint_t in_value, uint8_t width, uint8_t scale,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_decimal_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_hugeint_t in_value, uint8_t width, uint8_t scale,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_double_with_connection)
	(duckdb_v2_connection_handle conn, double in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_double_with_context)
	(duckdb_v2_context_handle ctx, double in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_float_with_connection)
	(duckdb_v2_connection_handle conn, float in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_float_with_context)
	(duckdb_v2_context_handle ctx, float in_value, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_hugeint_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_hugeint_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_hugeint_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_hugeint_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_int_with_connection)
	(duckdb_v2_connection_handle conn, int32_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_int_with_context)
	(duckdb_v2_context_handle ctx, int32_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_interval_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_interval_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_interval_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_interval_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_list_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle child_type, const duckdb_v2_value_handle *children,
	 idx_t child_count, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_list_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_logical_type_handle child_type, const duckdb_v2_value_handle *children,
	 idx_t child_count, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_map_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle key_type, duckdb_v2_logical_type_handle value_type,
	 const duckdb_v2_value_handle *keys, const duckdb_v2_value_handle *values, idx_t entry_count,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_map_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_logical_type_handle key_type, duckdb_v2_logical_type_handle value_type,
	 const duckdb_v2_value_handle *keys, const duckdb_v2_value_handle *values, idx_t entry_count,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_null)
	(duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_null_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_null_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_smallint_with_connection)
	(duckdb_v2_connection_handle conn, int16_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_smallint_with_context)
	(duckdb_v2_context_handle ctx, int16_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_struct_with_connection)
	(duckdb_v2_connection_handle conn, const duckdb_v2_identifier_t *names, const duckdb_v2_value_handle *children,
	 idx_t field_count, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_struct_with_context)
	(duckdb_v2_context_handle ctx, const duckdb_v2_identifier_t *names, const duckdb_v2_value_handle *children,
	 idx_t field_count, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_time_ns_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_time_ns_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_time_tz_with_connection)
	(duckdb_v2_connection_handle conn, uint64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_time_tz_with_context)
	(duckdb_v2_context_handle ctx, uint64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_time_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_time_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_ms_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_ms_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_ns_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_ns_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_sec_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_sec_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_tz_ns_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_tz_ns_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_tz_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_tz_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_with_connection)
	(duckdb_v2_connection_handle conn, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_timestamp_with_context)
	(duckdb_v2_context_handle ctx, int64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_tinyint_with_connection)
	(duckdb_v2_connection_handle conn, int8_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_tinyint_with_context)
	(duckdb_v2_context_handle ctx, int8_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_tuple_with_connection)
	(duckdb_v2_connection_handle conn, const duckdb_v2_value_handle *children, idx_t field_count,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_tuple_with_context)
	(duckdb_v2_context_handle ctx, const duckdb_v2_value_handle *children, idx_t field_count,
	 duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_type_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_logical_type_handle type, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_type_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_logical_type_handle in_type, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_ubigint_with_connection)
	(duckdb_v2_connection_handle conn, uint64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_ubigint_with_context)
	(duckdb_v2_context_handle ctx, uint64_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_uhugeint_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_uhugeint_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_uhugeint_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_uhugeint_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_uint_with_connection)
	(duckdb_v2_connection_handle conn, uint32_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_uint_with_context)
	(duckdb_v2_context_handle ctx, uint32_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_usmallint_with_connection)
	(duckdb_v2_connection_handle conn, uint16_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_usmallint_with_context)
	(duckdb_v2_context_handle ctx, uint16_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_utinyint_with_connection)
	(duckdb_v2_connection_handle conn, uint8_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_utinyint_with_context)
	(duckdb_v2_context_handle ctx, uint8_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_uuid_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_hugeint_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_uuid_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_hugeint_t in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_varchar_with_connection)
	(duckdb_v2_connection_handle conn, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_create_varchar_with_context)
	(duckdb_v2_context_handle ctx, duckdb_v2_str in_value, duckdb_v2_value_handle *out_value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_value_destroy)(duckdb_v2_value_handle *value);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_bigint)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_blob)
	(duckdb_v2_value_handle value, duckdb_v2_str *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_bool)
	(duckdb_v2_value_handle value, bool *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_child)
	(duckdb_v2_value_handle value, idx_t index, duckdb_v2_value_handle *out_child, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_child_count)
	(duckdb_v2_value_handle value, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_date)
	(duckdb_v2_value_handle value, int32_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_decimal)
	(duckdb_v2_value_handle value, duckdb_v2_hugeint_t *out, uint8_t *out_width, uint8_t *out_scale,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_double)
	(duckdb_v2_value_handle value, double *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_float)
	(duckdb_v2_value_handle value, float *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_hugeint)
	(duckdb_v2_value_handle value, duckdb_v2_hugeint_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_int)
	(duckdb_v2_value_handle value, int32_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_interval)
	(duckdb_v2_value_handle value, duckdb_v2_interval_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_logical_type)
	(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_smallint)
	(duckdb_v2_value_handle value, int16_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_time)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_time_ns)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_time_tz)
	(duckdb_v2_value_handle value, uint64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_timestamp)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_timestamp_ms)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_timestamp_ns)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_timestamp_sec)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_timestamp_tz)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_timestamp_tz_ns)
	(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_tinyint)
	(duckdb_v2_value_handle value, int8_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_type)
	(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_ubigint)
	(duckdb_v2_value_handle value, uint64_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_uhugeint)
	(duckdb_v2_value_handle value, duckdb_v2_uhugeint_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_uint)
	(duckdb_v2_value_handle value, uint32_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_usmallint)
	(duckdb_v2_value_handle value, uint16_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_utinyint)
	(duckdb_v2_value_handle value, uint8_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_uuid)
	(duckdb_v2_value_handle value, duckdb_v2_hugeint_t *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_get_varchar)
	(duckdb_v2_value_handle value, duckdb_v2_str *out, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_is_null)
	(duckdb_v2_value_handle value, bool *out_is_null, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_value_to_string)
	(duckdb_v2_value_handle value, char *out_string, idx_t out_capacity, idx_t *out_length,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_constant_set_valid)
	(duckdb_v2_vector_handle vector, bool validity, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_flat_get_validity_mutable)
	(duckdb_v2_vector_handle vector, uint64_t **out_validity, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_vector_flatten)(duckdb_v2_vector_handle vector, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_arena)
	(duckdb_v2_vector_handle vector, duckdb_v2_arena_handle *out_arena, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_child)
	(duckdb_v2_vector_handle vector, idx_t index, duckdb_v2_vector_handle *out_child, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_child_count)
	(duckdb_v2_vector_handle vector, idx_t *out_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_data_mutable)
	(duckdb_v2_vector_handle vector, void **out_data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_logical_type)
	(duckdb_v2_vector_handle vector, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_size)
	(duckdb_v2_vector_handle vector, idx_t *out_size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_value)
	(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_vector_type)
	(duckdb_v2_vector_handle vector, DUCKDB_V2_VECTOR_TYPE *out_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_get_view)
	(duckdb_v2_vector_handle vector, duckdb_v2_vector_view *out_view, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_make_constant)
	(duckdb_v2_vector_handle vector, duckdb_v2_value_handle value, idx_t count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_make_sequence)
	(duckdb_v2_vector_handle vector, int64_t start, int64_t increment, idx_t count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_reference)
	(duckdb_v2_vector_handle vector, duckdb_v2_vector_handle source, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_set_null)
	(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_set_size)
	(duckdb_v2_vector_handle vector, idx_t size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_vector_set_value)
	(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_get_arg_count)
	(duckdb_v2_aggregate_function_bind_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_get_arg_type)
	(duckdb_v2_aggregate_function_bind_info_handle info, idx_t index, duckdb_v2_logical_type_handle *type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_get_arg_value)
	(duckdb_v2_aggregate_function_bind_info_handle info, idx_t index, duckdb_v2_value_handle *value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_context_log)
	(duckdb_v2_context_handle ctx, DUCKDB_V2_LOG_LEVEL level, duckdb_v2_str log_type, duckdb_v2_str message,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_add_parameter)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_identifier_t name, duckdb_v2_logical_type_handle type,
	 duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_set_return_type)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_logical_type_handle type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_function_signature_set_varargs)
	(duckdb_v2_function_signature_handle sig, duckdb_v2_logical_type_handle type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_get_arg_count)
	(duckdb_v2_scalar_function_bind_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_get_arg_type)
	(duckdb_v2_scalar_function_bind_info_handle info, idx_t index, duckdb_v2_logical_type_handle *type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_get_arg_value)
	(duckdb_v2_scalar_function_bind_info_handle info, idx_t index, duckdb_v2_value_handle *value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_get_user_data)
	(duckdb_v2_scalar_function_bind_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_set_bind_data)
	(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_bind_set_return_type)
	(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_logical_type_handle return_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_create_with_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_scalar_function_handle *function,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_create_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_scalar_function_handle *function,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_scalar_function_destroy)(duckdb_v2_scalar_function_handle *function);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_arg)
	(duckdb_v2_scalar_function_exec_info_handle info, uint32_t index, duckdb_v2_vector_handle *vector,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_arg_count)
	(duckdb_v2_scalar_function_exec_info_handle info, uint32_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_bind_data)
	(duckdb_v2_scalar_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_init_data)
	(duckdb_v2_scalar_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_result)
	(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_vector_handle *vector,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_row_count)
	(duckdb_v2_scalar_function_exec_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_exec_get_user_data)
	(duckdb_v2_scalar_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_get_signature)
	(duckdb_v2_scalar_function_handle function, duckdb_v2_function_signature_handle *sig,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_init_get_bind_data)
	(duckdb_v2_scalar_function_init_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_init_get_user_data)
	(duckdb_v2_scalar_function_init_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_init_set_init_data)
	(duckdb_v2_scalar_function_init_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_register)
	(duckdb_v2_scalar_function_handle function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_set_bind_callback)
	(duckdb_v2_scalar_function_handle function, duckdb_v2_scalar_function_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_set_exec_callback)
	(duckdb_v2_scalar_function_handle function, duckdb_v2_scalar_function_exec_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_set_init_callback)
	(duckdb_v2_scalar_function_handle function, duckdb_v2_scalar_function_init_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_set_name)
	(duckdb_v2_scalar_function_handle function, duckdb_v2_str *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_set_user_data)
	(duckdb_v2_scalar_function_handle function, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_get_user_data)
	(duckdb_v2_aggregate_function_bind_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_set_bind_data)
	(duckdb_v2_aggregate_function_bind_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_bind_set_return_type)
	(duckdb_v2_aggregate_function_bind_info_handle info, duckdb_v2_logical_type_handle return_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_bind_data)
	(duckdb_v2_aggregate_function_combine_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_sources)
	(duckdb_v2_aggregate_function_combine_info_handle info, void ***states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_state_count)
	(duckdb_v2_aggregate_function_combine_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_targets)
	(duckdb_v2_aggregate_function_combine_info_handle info, void ***states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_combine_get_user_data)
	(duckdb_v2_aggregate_function_combine_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_create_with_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_aggregate_function_handle *function,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_create_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_aggregate_function_handle *function,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_aggregate_function_destroy)(duckdb_v2_aggregate_function_handle *function);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_bind_data)
	(duckdb_v2_aggregate_function_destroy_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_state_count)
	(duckdb_v2_aggregate_function_destroy_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_states)
	(duckdb_v2_aggregate_function_destroy_info_handle info, void ***states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_destroy_get_user_data)
	(duckdb_v2_aggregate_function_destroy_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_bind_data)
	(duckdb_v2_aggregate_function_finalize_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_result)
	(duckdb_v2_aggregate_function_finalize_info_handle info, duckdb_v2_vector_handle *vector,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_result_offset)
	(duckdb_v2_aggregate_function_finalize_info_handle info, idx_t *offset, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_state_count)
	(duckdb_v2_aggregate_function_finalize_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_states)
	(duckdb_v2_aggregate_function_finalize_info_handle info, void ***states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_finalize_get_user_data)
	(duckdb_v2_aggregate_function_finalize_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_get_signature)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_function_signature_handle *sig,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_init_get_bind_data)
	(duckdb_v2_aggregate_function_init_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_init_get_state_count)
	(duckdb_v2_aggregate_function_init_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_init_get_states)
	(duckdb_v2_aggregate_function_init_info_handle info, void ***states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_init_get_user_data)
	(duckdb_v2_aggregate_function_init_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_register)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_bind_callback)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_aggregate_function_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_combine_callback)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_aggregate_function_combine_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_destroy_callback)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_aggregate_function_destroy_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_finalize_callback)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_aggregate_function_finalize_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_init_callback)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_aggregate_function_init_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_name)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_str *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_size_callback)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_aggregate_function_size_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_update_callback)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_aggregate_function_update_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_user_data)
	(duckdb_v2_aggregate_function_handle function, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_size_get_bind_data)
	(duckdb_v2_aggregate_function_size_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_size_get_user_data)
	(duckdb_v2_aggregate_function_size_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_size_set_state_size)
	(duckdb_v2_aggregate_function_size_info_handle info, idx_t size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_arg)
	(duckdb_v2_aggregate_function_update_info_handle info, uint32_t index, duckdb_v2_vector_handle *vector,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_arg_count)
	(duckdb_v2_aggregate_function_update_info_handle info, uint32_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_bind_data)
	(duckdb_v2_aggregate_function_update_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_row_count)
	(duckdb_v2_aggregate_function_update_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_states)
	(duckdb_v2_aggregate_function_update_info_handle info, void ***states, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_update_get_user_data)
	(duckdb_v2_aggregate_function_update_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_aggregate_function_set_property)
	(duckdb_v2_aggregate_function_handle function, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
	 DUCKDB_V2_FUNCTION_PROPERTY_VALUE value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_scalar_function_set_property)
	(duckdb_v2_scalar_function_handle function, DUCKDB_V2_FUNCTION_PROPERTY_KEY key,
	 DUCKDB_V2_FUNCTION_PROPERTY_VALUE value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_add_result_column)
	(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_identifier_t name, duckdb_v2_logical_type_handle type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_get_arg_count)
	(duckdb_v2_table_function_bind_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_get_arg_type)
	(duckdb_v2_table_function_bind_info_handle info, idx_t index, duckdb_v2_logical_type_handle *type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_get_arg_value)
	(duckdb_v2_table_function_bind_info_handle info, idx_t index, duckdb_v2_value_handle *value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_get_user_data)
	(duckdb_v2_table_function_bind_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_set_bind_data)
	(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_bind_set_cardinality)
	(duckdb_v2_table_function_bind_info_handle info, idx_t cardinality, bool is_exact,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_create_with_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_table_function_handle *function,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_create_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_table_function_handle *function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_table_function_destroy)(duckdb_v2_table_function_handle *function);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_bind_data)
	(duckdb_v2_table_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_global_state)
	(duckdb_v2_table_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_local_state)
	(duckdb_v2_table_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_output_chunk)
	(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_data_chunk_handle *chunk,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_user_data)
	(duckdb_v2_table_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_get_signature)
	(duckdb_v2_table_function_handle function, duckdb_v2_function_signature_handle *sig,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_global_get_bind_data)
	(duckdb_v2_table_function_init_global_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_global_get_user_data)
	(duckdb_v2_table_function_init_global_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_global_set_global_state)
	(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_global_set_max_threads)
	(duckdb_v2_table_function_init_global_info_handle info, idx_t max_threads, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_local_get_bind_data)
	(duckdb_v2_table_function_init_local_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_local_get_global_state)
	(duckdb_v2_table_function_init_local_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_local_get_user_data)
	(duckdb_v2_table_function_init_local_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_local_set_local_state)
	(duckdb_v2_table_function_init_local_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_progress_get_bind_data)
	(duckdb_v2_table_function_progress_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_progress_get_global_state)
	(duckdb_v2_table_function_progress_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_progress_get_user_data)
	(duckdb_v2_table_function_progress_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_progress_set_progress)
	(duckdb_v2_table_function_progress_info_handle info, double progress, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_register)
	(duckdb_v2_table_function_handle function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_bind_callback)
	(duckdb_v2_table_function_handle function, duckdb_v2_table_function_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_exec_callback)
	(duckdb_v2_table_function_handle function, duckdb_v2_table_function_exec_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_init_global_callback)
	(duckdb_v2_table_function_handle function, duckdb_v2_table_function_init_global_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_init_local_callback)
	(duckdb_v2_table_function_handle function, duckdb_v2_table_function_init_local_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_name)
	(duckdb_v2_table_function_handle function, duckdb_v2_str *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_progress_callback)
	(duckdb_v2_table_function_handle function, duckdb_v2_table_function_progress_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_user_data)
	(duckdb_v2_table_function_handle function, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_create_with_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_cast_function_handle *function,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_create_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_cast_function_handle *function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_cast_function_destroy)(duckdb_v2_cast_function_handle *function);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_input)
	(duckdb_v2_cast_function_exec_info_handle info, duckdb_v2_vector_handle *vector, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_mode)
	(duckdb_v2_cast_function_exec_info_handle info, DUCKDB_V2_CAST_MODE *mode, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_output)
	(duckdb_v2_cast_function_exec_info_handle info, duckdb_v2_vector_handle *vector, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_row_count)
	(duckdb_v2_cast_function_exec_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_exec_get_user_data)
	(duckdb_v2_cast_function_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_register)
	(duckdb_v2_cast_function_handle function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_set_exec_callback)
	(duckdb_v2_cast_function_handle function, duckdb_v2_cast_function_exec_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_set_implicit_cast_cost)
	(duckdb_v2_cast_function_handle function, int64_t cost, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_set_source_type)
	(duckdb_v2_cast_function_handle function, duckdb_v2_logical_type_handle source_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_set_target_type)
	(duckdb_v2_cast_function_handle function, duckdb_v2_logical_type_handle target_type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_cast_function_set_user_data)
	(duckdb_v2_cast_function_handle function, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_data_collection_clear)
	(duckdb_v2_column_data_collection_handle collection, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_create_with_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_custom_type_handle *type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_create_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_custom_type_handle *type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_custom_type_destroy)(duckdb_v2_custom_type_handle *type);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_register)
	(duckdb_v2_custom_type_handle type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_set_base_type)
	(duckdb_v2_custom_type_handle type, duckdb_v2_logical_type_handle base_type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_custom_type_set_name)
	(duckdb_v2_custom_type_handle type, duckdb_v2_identifier_t name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_file_close)(duckdb_v2_file_handle file, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_file_destroy)(duckdb_v2_file_handle *file);
	DUCKDB_V2_ERROR(*duckdb_v2_file_open_options_create)
	(duckdb_v2_file_system_handle file_system, duckdb_v2_file_open_options_handle *options,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_file_open_options_destroy)(duckdb_v2_file_open_options_handle *options);
	DUCKDB_V2_ERROR(*duckdb_v2_file_open_options_set_flag)
	(duckdb_v2_file_open_options_handle options, DUCKDB_V2_FILE_FLAG flag, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_open_options_set_value)
	(duckdb_v2_file_open_options_handle options, duckdb_v2_str name, duckdb_v2_value_handle value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_read)
	(duckdb_v2_file_handle file, void *buffer, idx_t buffer_size, idx_t *bytes_read, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_read_at)
	(duckdb_v2_file_handle file, void *buffer, idx_t buffer_size, idx_t location, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_seek)
	(duckdb_v2_file_handle file, idx_t position, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_file_size)(duckdb_v2_file_handle file, idx_t *size, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_file_sync)(duckdb_v2_file_handle file, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_system_get_from_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_file_system_handle *file_system,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_system_get_from_context)
	(duckdb_v2_context_handle context, duckdb_v2_file_system_handle *file_system, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_system_open)
	(duckdb_v2_file_system_handle file_system, duckdb_v2_str file_path, duckdb_v2_file_open_options_handle options,
	 duckdb_v2_file_handle *file, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_tell)
	(duckdb_v2_file_handle file, idx_t *position, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_write)
	(duckdb_v2_file_handle file, const void *buffer, idx_t buffer_size, idx_t *bytes_written,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_file_write_at)
	(duckdb_v2_file_handle file, const void *buffer, idx_t buffer_size, idx_t location,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_create)
	(const duckdb_v2_identifier_t *parts, idx_t part_count, duckdb_v2_qname_handle *name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_qname_destroy)(duckdb_v2_qname_handle *name);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_equals)
	(duckdb_v2_qname_handle left, duckdb_v2_qname_handle right, bool *result, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_get_part)
	(duckdb_v2_qname_handle name, idx_t index, duckdb_v2_identifier_t *part, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_get_part_count)
	(duckdb_v2_qname_handle name, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_hash)
	(duckdb_v2_qname_handle name, uint64_t *hash, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_parse)
	(duckdb_v2_str text, duckdb_v2_qname_handle *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_qname_render)
	(duckdb_v2_qname_handle name, char *out_text, idx_t out_capacity, idx_t *out_length,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_add_argument)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_add_named_argument)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_identifier_t name, duckdb_v2_value_handle value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_create_with_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_replacement_scan_handle *scan, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_create_with_database)
	(duckdb_v2_database_handle database, duckdb_v2_replacement_scan_handle *scan, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_create_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_replacement_scan_handle *scan, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_replacement_scan_destroy)(duckdb_v2_replacement_scan_handle *scan);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_get_name)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_qname_handle *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_get_user_data)
	(duckdb_v2_replacement_scan_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_register)
	(duckdb_v2_replacement_scan_handle scan, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_set_alias)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_identifier_t alias, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_set_callback)
	(duckdb_v2_replacement_scan_handle scan, duckdb_v2_replacement_scan_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_set_collection)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_column_data_collection_handle collection,
	 const duckdb_v2_identifier_t *column_names, idx_t column_count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_set_function_name)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_qname_handle name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_set_subquery)
	(duckdb_v2_replacement_scan_info_handle info, duckdb_v2_str sql, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_replacement_scan_set_user_data)
	(duckdb_v2_replacement_scan_handle scan, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_exporter_append)
	(duckdb_v2_arrow_exporter_handle exporter, duckdb_v2_data_chunk_handle *chunk, bool consume, bool flush,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_exporter_create)
	(duckdb_v2_context_handle context, const duckdb_v2_logical_type_handle *types, const duckdb_v2_str *names,
	 idx_t count, idx_t batch_size, duckdb_v2_arrow_exporter_handle *out_exporter, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_arrow_exporter_destroy)(duckdb_v2_arrow_exporter_handle *exporter);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_exporter_get_schema)
	(duckdb_v2_arrow_exporter_handle exporter, struct ArrowSchema *out_schema, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_exporter_next_array)
	(duckdb_v2_arrow_exporter_handle exporter, struct ArrowArray *out_array, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_importer_append)
	(duckdb_v2_arrow_importer_handle importer, struct ArrowArray *array, bool consume, bool flush,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_importer_create)
	(duckdb_v2_context_handle context, struct ArrowSchema *schema, idx_t batch_size,
	 duckdb_v2_arrow_importer_handle *out_importer, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_arrow_importer_destroy)(duckdb_v2_arrow_importer_handle *importer);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_importer_get_schema)
	(duckdb_v2_arrow_importer_handle importer, duckdb_v2_schema_handle *out_schema, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_arrow_importer_next_chunk)
	(duckdb_v2_arrow_importer_handle importer, duckdb_v2_data_chunk_handle *out_chunk,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_column_description_destroy)(duckdb_v2_column_description_handle *column);
	DUCKDB_V2_ERROR(*duckdb_v2_column_description_get_name)
	(duckdb_v2_column_description_handle column, duckdb_v2_identifier_t *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_description_get_type)
	(duckdb_v2_column_description_handle column, duckdb_v2_logical_type_handle *type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_description_has_default)
	(duckdb_v2_column_description_handle column, bool *has_default, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_column_description_has_generated)
	(duckdb_v2_column_description_handle column, bool *has_generated, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_connection_describe_table)
	(duckdb_v2_connection_handle conn, duckdb_v2_qname_handle name, duckdb_v2_table_description_handle *desc,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_column_count)
	(duckdb_v2_copy_from_bind_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_column_name)
	(duckdb_v2_copy_from_bind_info_handle info, idx_t index, duckdb_v2_identifier_t *name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_column_type)
	(duckdb_v2_copy_from_bind_info_handle info, idx_t index, duckdb_v2_logical_type_handle *type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_file_path)
	(duckdb_v2_copy_from_bind_info_handle info, duckdb_v2_str *path, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_option_count)
	(duckdb_v2_copy_from_bind_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_option_name)
	(duckdb_v2_copy_from_bind_info_handle info, idx_t index, duckdb_v2_identifier_t *name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_option_value)
	(duckdb_v2_copy_from_bind_info_handle info, idx_t index, duckdb_v2_value_handle *value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_get_user_data)
	(duckdb_v2_copy_from_bind_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_set_bind_data)
	(duckdb_v2_copy_from_bind_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_bind_set_cardinality)
	(duckdb_v2_copy_from_bind_info_handle info, idx_t cardinality, bool is_exact, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_exec_get_bind_data)
	(duckdb_v2_copy_from_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_exec_get_global_state)
	(duckdb_v2_copy_from_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_exec_get_local_state)
	(duckdb_v2_copy_from_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_exec_get_output_chunk)
	(duckdb_v2_copy_from_exec_info_handle info, duckdb_v2_data_chunk_handle *chunk, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_exec_get_user_data)
	(duckdb_v2_copy_from_exec_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_global_get_bind_data)
	(duckdb_v2_copy_from_init_global_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_global_get_user_data)
	(duckdb_v2_copy_from_init_global_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_global_set_global_state)
	(duckdb_v2_copy_from_init_global_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_global_set_max_threads)
	(duckdb_v2_copy_from_init_global_info_handle info, idx_t max_threads, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_local_get_bind_data)
	(duckdb_v2_copy_from_init_local_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_local_get_global_state)
	(duckdb_v2_copy_from_init_local_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_local_get_user_data)
	(duckdb_v2_copy_from_init_local_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_init_local_set_local_state)
	(duckdb_v2_copy_from_init_local_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_progress_get_bind_data)
	(duckdb_v2_copy_from_progress_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_progress_get_global_state)
	(duckdb_v2_copy_from_progress_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_progress_get_user_data)
	(duckdb_v2_copy_from_progress_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_progress_set_progress)
	(duckdb_v2_copy_from_progress_info_handle info, double progress, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_set_bind_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_from_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_set_exec_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_from_exec_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_set_init_global_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_from_init_global_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_set_init_local_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_from_init_local_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_from_set_progress_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_from_progress_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_create_with_connection)
	(duckdb_v2_connection_handle connection, duckdb_v2_copy_function_handle *function,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_create_with_extension)
	(duckdb_v2_extension_handle extension, duckdb_v2_copy_function_handle *function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_copy_function_destroy)(duckdb_v2_copy_function_handle *function);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_register)
	(duckdb_v2_copy_function_handle function, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_set_name)
	(duckdb_v2_copy_function_handle function, duckdb_v2_str *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_function_set_user_data)
	(duckdb_v2_copy_function_handle function, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_get_bind_data)
	(duckdb_v2_copy_to_batch_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_get_init_data)
	(duckdb_v2_copy_to_batch_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_get_user_data)
	(duckdb_v2_copy_to_batch_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_set_batch_data)
	(duckdb_v2_copy_to_batch_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_size_get_bind_data)
	(duckdb_v2_copy_to_batch_size_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_size_get_user_data)
	(duckdb_v2_copy_to_batch_size_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_size_set_target)
	(duckdb_v2_copy_to_batch_size_info_handle info, idx_t rows, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_batch_take_input)
	(duckdb_v2_copy_to_batch_info_handle info, duckdb_v2_column_data_collection_handle *collection,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_column_count)
	(duckdb_v2_copy_to_bind_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_column_name)
	(duckdb_v2_copy_to_bind_info_handle info, idx_t index, duckdb_v2_identifier_t *name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_column_type)
	(duckdb_v2_copy_to_bind_info_handle info, idx_t index, duckdb_v2_logical_type_handle *type,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_file_path)
	(duckdb_v2_copy_to_bind_info_handle info, duckdb_v2_str *path, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_option_count)
	(duckdb_v2_copy_to_bind_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_option_name)
	(duckdb_v2_copy_to_bind_info_handle info, idx_t index, duckdb_v2_identifier_t *name,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_option_value)
	(duckdb_v2_copy_to_bind_info_handle info, idx_t index, duckdb_v2_value_handle *value,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_get_user_data)
	(duckdb_v2_copy_to_bind_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_bind_set_bind_data)
	(duckdb_v2_copy_to_bind_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_finalize_get_bind_data)
	(duckdb_v2_copy_to_finalize_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_finalize_get_init_data)
	(duckdb_v2_copy_to_finalize_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_finalize_get_user_data)
	(duckdb_v2_copy_to_finalize_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_flush_get_batch_data)
	(duckdb_v2_copy_to_flush_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_flush_get_bind_data)
	(duckdb_v2_copy_to_flush_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_flush_get_init_data)
	(duckdb_v2_copy_to_flush_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_flush_get_user_data)
	(duckdb_v2_copy_to_flush_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_init_get_bind_data)
	(duckdb_v2_copy_to_init_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_init_get_file_path)
	(duckdb_v2_copy_to_init_info_handle info, duckdb_v2_str *path, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_init_get_user_data)
	(duckdb_v2_copy_to_init_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_init_set_init_data)
	(duckdb_v2_copy_to_init_info_handle info, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_set_batch_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_to_batch_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_set_batch_size_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_to_batch_size_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_set_bind_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_to_bind_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_set_finalize_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_to_finalize_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_set_flush_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_to_flush_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_copy_to_set_init_callback)
	(duckdb_v2_copy_function_handle function, duckdb_v2_copy_to_init_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_cast_get_mode)
	(duckdb_v2_expression_handle expression, DUCKDB_V2_CAST_MODE *mode, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_column_ref_get_index)
	(duckdb_v2_expression_handle expression, idx_t *index, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_constant_get_value)
	(duckdb_v2_expression_handle expression, duckdb_v2_value_handle *value, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_function_get_name)
	(duckdb_v2_expression_handle expression, duckdb_v2_identifier_t *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_function_get_qname)
	(duckdb_v2_expression_handle expression, duckdb_v2_qname_handle *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_child)
	(duckdb_v2_expression_handle expression, idx_t index, duckdb_v2_expression_handle *child,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_child_count)
	(duckdb_v2_expression_handle expression, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_return_type)
	(duckdb_v2_expression_handle expression, duckdb_v2_logical_type_handle *type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_expression_get_type)
	(duckdb_v2_expression_handle expression, DUCKDB_V2_EXPRESSION_TYPE *type, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_identifier_render_quoted)
	(duckdb_v2_identifier_t name, char *out_text, idx_t out_capacity, idx_t *out_length,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_prepared_statement_create)
	(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement, bool require_cacheable,
	 duckdb_v2_prepared_statement_handle *out_prepared, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_prepared_statement_destroy)(duckdb_v2_prepared_statement_handle *prepared);
	DUCKDB_V2_ERROR(*duckdb_v2_prepared_statement_execute)
	(duckdb_v2_prepared_statement_handle prepared, const duckdb_v2_identifier_t *parameter_names,
	 const duckdb_v2_value_handle *parameter_values, idx_t parameter_count, duckdb_v2_result_handle *out_result,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_prepared_statement_reuses_plan)
	(duckdb_v2_prepared_statement_handle prepared, bool *out_reuses, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_result_to_arrow_stream)
	(duckdb_v2_result_handle *result, idx_t batch_size, struct ArrowArrayStream *out_stream,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR (*duckdb_v2_table_description_destroy)(duckdb_v2_table_description_handle *desc);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_get_column)
	(duckdb_v2_table_description_handle desc, idx_t index, duckdb_v2_column_description_handle *column,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_get_column_count)
	(duckdb_v2_table_description_handle desc, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_get_qname)
	(duckdb_v2_table_description_handle desc, duckdb_v2_qname_handle *name, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_description_is_readonly)
	(duckdb_v2_table_description_handle desc, bool *readonly, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_column_count)
	(duckdb_v2_table_function_exec_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_exec_get_column_index)
	(duckdb_v2_table_function_exec_info_handle info, idx_t index, idx_t *column_index,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_pushdown_accept)
	(duckdb_v2_table_function_filter_pushdown_info_handle info, idx_t index, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_pushdown_get_bind_data)
	(duckdb_v2_table_function_filter_pushdown_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_pushdown_get_column_count)
	(duckdb_v2_table_function_filter_pushdown_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_pushdown_get_column_index)
	(duckdb_v2_table_function_filter_pushdown_info_handle info, idx_t index, idx_t *column_index,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_pushdown_get_filter)
	(duckdb_v2_table_function_filter_pushdown_info_handle info, idx_t index, duckdb_v2_expression_handle *filter,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_pushdown_get_filter_count)
	(duckdb_v2_table_function_filter_pushdown_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_filter_pushdown_get_user_data)
	(duckdb_v2_table_function_filter_pushdown_info_handle info, void **data, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_global_get_column_count)
	(duckdb_v2_table_function_init_global_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_global_get_column_index)
	(duckdb_v2_table_function_init_global_info_handle info, idx_t index, idx_t *column_index,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_local_get_column_count)
	(duckdb_v2_table_function_init_local_info_handle info, idx_t *count, duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_init_local_get_column_index)
	(duckdb_v2_table_function_init_local_info_handle info, idx_t index, idx_t *column_index,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_filter_pushdown_callback)
	(duckdb_v2_table_function_handle function, duckdb_v2_table_function_filter_pushdown_callback_fn callback,
	 duckdb_v2_error_info_handle *err);
	DUCKDB_V2_ERROR(*duckdb_v2_table_function_set_projection_pushdown)
	(duckdb_v2_table_function_handle function, bool enable, duckdb_v2_error_info_handle *err);
} duckdb_ext_api_v2;

//===--------------------------------------------------------------------===//
// Struct Create Method
//===--------------------------------------------------------------------===//
inline duckdb_ext_api_v2 CreateAPIv2(void) {
	duckdb_ext_api_v2 result;
	result.duckdb_v2_arena_allocate = duckdb_v2_arena_allocate;
	result.duckdb_v2_bignum_decode = duckdb_v2_bignum_decode;
	result.duckdb_v2_bignum_encode = duckdb_v2_bignum_encode;
	result.duckdb_v2_close = duckdb_v2_close;
	result.duckdb_v2_column_data_collection_append = duckdb_v2_column_data_collection_append;
	result.duckdb_v2_column_data_collection_append_state_create = duckdb_v2_column_data_collection_append_state_create;
	result.duckdb_v2_column_data_collection_append_state_destroy =
	    duckdb_v2_column_data_collection_append_state_destroy;
	result.duckdb_v2_column_data_collection_combine = duckdb_v2_column_data_collection_combine;
	result.duckdb_v2_column_data_collection_create_with_connection =
	    duckdb_v2_column_data_collection_create_with_connection;
	result.duckdb_v2_column_data_collection_create_with_context = duckdb_v2_column_data_collection_create_with_context;
	result.duckdb_v2_column_data_collection_destroy = duckdb_v2_column_data_collection_destroy;
	result.duckdb_v2_column_data_collection_reset = duckdb_v2_column_data_collection_reset;
	result.duckdb_v2_column_data_collection_row_count = duckdb_v2_column_data_collection_row_count;
	result.duckdb_v2_column_data_collection_scan = duckdb_v2_column_data_collection_scan;
	result.duckdb_v2_column_data_collection_shared_scan_state_create =
	    duckdb_v2_column_data_collection_shared_scan_state_create;
	result.duckdb_v2_column_data_collection_shared_scan_state_destroy =
	    duckdb_v2_column_data_collection_shared_scan_state_destroy;
	result.duckdb_v2_column_data_collection_worker_scan_state_create =
	    duckdb_v2_column_data_collection_worker_scan_state_create;
	result.duckdb_v2_column_data_collection_worker_scan_state_destroy =
	    duckdb_v2_column_data_collection_worker_scan_state_destroy;
	result.duckdb_v2_connect = duckdb_v2_connect;
	result.duckdb_v2_connection_create_type_from_id = duckdb_v2_connection_create_type_from_id;
	result.duckdb_v2_connection_create_type_from_name = duckdb_v2_connection_create_type_from_name;
	result.duckdb_v2_connection_create_type_from_text = duckdb_v2_connection_create_type_from_text;
	result.duckdb_v2_connection_create_type_with_alias = duckdb_v2_connection_create_type_with_alias;
	result.duckdb_v2_connection_interrupt = duckdb_v2_connection_interrupt;
	result.duckdb_v2_connection_option_get = duckdb_v2_connection_option_get;
	result.duckdb_v2_connection_option_get_by_index = duckdb_v2_connection_option_get_by_index;
	result.duckdb_v2_connection_option_get_count = duckdb_v2_connection_option_get_count;
	result.duckdb_v2_connection_option_set = duckdb_v2_connection_option_set;
	result.duckdb_v2_connection_query_progress = duckdb_v2_connection_query_progress;
	result.duckdb_v2_context_create_type_from_id = duckdb_v2_context_create_type_from_id;
	result.duckdb_v2_context_create_type_from_name = duckdb_v2_context_create_type_from_name;
	result.duckdb_v2_context_create_type_from_text = duckdb_v2_context_create_type_from_text;
	result.duckdb_v2_context_create_type_with_alias = duckdb_v2_context_create_type_with_alias;
	result.duckdb_v2_create_environment = duckdb_v2_create_environment;
	result.duckdb_v2_data_chunk_copy_with_connection = duckdb_v2_data_chunk_copy_with_connection;
	result.duckdb_v2_data_chunk_copy_with_context = duckdb_v2_data_chunk_copy_with_context;
	result.duckdb_v2_data_chunk_create = duckdb_v2_data_chunk_create;
	result.duckdb_v2_data_chunk_create_with_connection = duckdb_v2_data_chunk_create_with_connection;
	result.duckdb_v2_data_chunk_create_with_context = duckdb_v2_data_chunk_create_with_context;
	result.duckdb_v2_data_chunk_destroy = duckdb_v2_data_chunk_destroy;
	result.duckdb_v2_data_chunk_get_size = duckdb_v2_data_chunk_get_size;
	result.duckdb_v2_data_chunk_get_vector = duckdb_v2_data_chunk_get_vector;
	result.duckdb_v2_data_chunk_get_vector_count = duckdb_v2_data_chunk_get_vector_count;
	result.duckdb_v2_database_option_get = duckdb_v2_database_option_get;
	result.duckdb_v2_database_option_get_by_index = duckdb_v2_database_option_get_by_index;
	result.duckdb_v2_database_option_get_count = duckdb_v2_database_option_get_count;
	result.duckdb_v2_database_option_set = duckdb_v2_database_option_set;
	result.duckdb_v2_destroy_environment = duckdb_v2_destroy_environment;
	result.duckdb_v2_disconnect = duckdb_v2_disconnect;
	result.duckdb_v2_environment_database_count = duckdb_v2_environment_database_count;
	result.duckdb_v2_error_info_destroy = duckdb_v2_error_info_destroy;
	result.duckdb_v2_error_info_get_code = duckdb_v2_error_info_get_code;
	result.duckdb_v2_error_info_get_raw_message = duckdb_v2_error_info_get_raw_message;
	result.duckdb_v2_error_info_get_text = duckdb_v2_error_info_get_text;
	result.duckdb_v2_error_info_set_code = duckdb_v2_error_info_set_code;
	result.duckdb_v2_error_info_set_text = duckdb_v2_error_info_set_text;
	result.duckdb_v2_library_version = duckdb_v2_library_version;
	result.duckdb_v2_logical_type_copy = duckdb_v2_logical_type_copy;
	result.duckdb_v2_logical_type_destroy = duckdb_v2_logical_type_destroy;
	result.duckdb_v2_logical_type_get_id = duckdb_v2_logical_type_get_id;
	result.duckdb_v2_logical_type_get_name = duckdb_v2_logical_type_get_name;
	result.duckdb_v2_logical_type_get_param = duckdb_v2_logical_type_get_param;
	result.duckdb_v2_logical_type_get_param_count = duckdb_v2_logical_type_get_param_count;
	result.duckdb_v2_logical_type_is_equal = duckdb_v2_logical_type_is_equal;
	result.duckdb_v2_logical_type_to_text = duckdb_v2_logical_type_to_text;
	result.duckdb_v2_open = duckdb_v2_open;
	result.duckdb_v2_option_create = duckdb_v2_option_create;
	result.duckdb_v2_option_destroy = duckdb_v2_option_destroy;
	result.duckdb_v2_option_get_alias = duckdb_v2_option_get_alias;
	result.duckdb_v2_option_get_alias_count = duckdb_v2_option_get_alias_count;
	result.duckdb_v2_option_get_default_setting = duckdb_v2_option_get_default_setting;
	result.duckdb_v2_option_get_description = duckdb_v2_option_get_description;
	result.duckdb_v2_option_get_name = duckdb_v2_option_get_name;
	result.duckdb_v2_option_get_setting = duckdb_v2_option_get_setting;
	result.duckdb_v2_option_get_target_scope = duckdb_v2_option_get_target_scope;
	result.duckdb_v2_parse_sql = duckdb_v2_parse_sql;
	result.duckdb_v2_query_progress_destroy = duckdb_v2_query_progress_destroy;
	result.duckdb_v2_query_progress_get_percentage = duckdb_v2_query_progress_get_percentage;
	result.duckdb_v2_query_progress_get_rows_processed = duckdb_v2_query_progress_get_rows_processed;
	result.duckdb_v2_query_progress_get_total_rows_to_process = duckdb_v2_query_progress_get_total_rows_to_process;
	result.duckdb_v2_result_destroy = duckdb_v2_result_destroy;
	result.duckdb_v2_result_drain = duckdb_v2_result_drain;
	result.duckdb_v2_result_fetch_chunk = duckdb_v2_result_fetch_chunk;
	result.duckdb_v2_result_get_result_type = duckdb_v2_result_get_result_type;
	result.duckdb_v2_result_get_schema = duckdb_v2_result_get_schema;
	result.duckdb_v2_result_get_statement_type = duckdb_v2_result_get_statement_type;
	result.duckdb_v2_result_render_box = duckdb_v2_result_render_box;
	result.duckdb_v2_result_step = duckdb_v2_result_step;
	result.duckdb_v2_result_wait = duckdb_v2_result_wait;
	result.duckdb_v2_schema_destroy = duckdb_v2_schema_destroy;
	result.duckdb_v2_schema_get_count = duckdb_v2_schema_get_count;
	result.duckdb_v2_schema_get_field = duckdb_v2_schema_get_field;
	result.duckdb_v2_sql_statement_destroy = duckdb_v2_sql_statement_destroy;
	result.duckdb_v2_statement_bind = duckdb_v2_statement_bind;
	result.duckdb_v2_statement_execute = duckdb_v2_statement_execute;
	result.duckdb_v2_statement_iterator_destroy = duckdb_v2_statement_iterator_destroy;
	result.duckdb_v2_statement_iterator_next = duckdb_v2_statement_iterator_next;
	result.duckdb_v2_value_cast_with_connection = duckdb_v2_value_cast_with_connection;
	result.duckdb_v2_value_cast_with_context = duckdb_v2_value_cast_with_context;
	result.duckdb_v2_value_create_array_with_connection = duckdb_v2_value_create_array_with_connection;
	result.duckdb_v2_value_create_array_with_context = duckdb_v2_value_create_array_with_context;
	result.duckdb_v2_value_create_bigint_with_connection = duckdb_v2_value_create_bigint_with_connection;
	result.duckdb_v2_value_create_bigint_with_context = duckdb_v2_value_create_bigint_with_context;
	result.duckdb_v2_value_create_bignum_with_connection = duckdb_v2_value_create_bignum_with_connection;
	result.duckdb_v2_value_create_bignum_with_context = duckdb_v2_value_create_bignum_with_context;
	result.duckdb_v2_value_create_bit_with_connection = duckdb_v2_value_create_bit_with_connection;
	result.duckdb_v2_value_create_bit_with_context = duckdb_v2_value_create_bit_with_context;
	result.duckdb_v2_value_create_blob_with_connection = duckdb_v2_value_create_blob_with_connection;
	result.duckdb_v2_value_create_blob_with_context = duckdb_v2_value_create_blob_with_context;
	result.duckdb_v2_value_create_bool_with_connection = duckdb_v2_value_create_bool_with_connection;
	result.duckdb_v2_value_create_bool_with_context = duckdb_v2_value_create_bool_with_context;
	result.duckdb_v2_value_create_date_with_connection = duckdb_v2_value_create_date_with_connection;
	result.duckdb_v2_value_create_date_with_context = duckdb_v2_value_create_date_with_context;
	result.duckdb_v2_value_create_decimal_with_connection = duckdb_v2_value_create_decimal_with_connection;
	result.duckdb_v2_value_create_decimal_with_context = duckdb_v2_value_create_decimal_with_context;
	result.duckdb_v2_value_create_double_with_connection = duckdb_v2_value_create_double_with_connection;
	result.duckdb_v2_value_create_double_with_context = duckdb_v2_value_create_double_with_context;
	result.duckdb_v2_value_create_float_with_connection = duckdb_v2_value_create_float_with_connection;
	result.duckdb_v2_value_create_float_with_context = duckdb_v2_value_create_float_with_context;
	result.duckdb_v2_value_create_hugeint_with_connection = duckdb_v2_value_create_hugeint_with_connection;
	result.duckdb_v2_value_create_hugeint_with_context = duckdb_v2_value_create_hugeint_with_context;
	result.duckdb_v2_value_create_int_with_connection = duckdb_v2_value_create_int_with_connection;
	result.duckdb_v2_value_create_int_with_context = duckdb_v2_value_create_int_with_context;
	result.duckdb_v2_value_create_interval_with_connection = duckdb_v2_value_create_interval_with_connection;
	result.duckdb_v2_value_create_interval_with_context = duckdb_v2_value_create_interval_with_context;
	result.duckdb_v2_value_create_list_with_connection = duckdb_v2_value_create_list_with_connection;
	result.duckdb_v2_value_create_list_with_context = duckdb_v2_value_create_list_with_context;
	result.duckdb_v2_value_create_map_with_connection = duckdb_v2_value_create_map_with_connection;
	result.duckdb_v2_value_create_map_with_context = duckdb_v2_value_create_map_with_context;
	result.duckdb_v2_value_create_null = duckdb_v2_value_create_null;
	result.duckdb_v2_value_create_null_with_connection = duckdb_v2_value_create_null_with_connection;
	result.duckdb_v2_value_create_null_with_context = duckdb_v2_value_create_null_with_context;
	result.duckdb_v2_value_create_smallint_with_connection = duckdb_v2_value_create_smallint_with_connection;
	result.duckdb_v2_value_create_smallint_with_context = duckdb_v2_value_create_smallint_with_context;
	result.duckdb_v2_value_create_struct_with_connection = duckdb_v2_value_create_struct_with_connection;
	result.duckdb_v2_value_create_struct_with_context = duckdb_v2_value_create_struct_with_context;
	result.duckdb_v2_value_create_time_ns_with_connection = duckdb_v2_value_create_time_ns_with_connection;
	result.duckdb_v2_value_create_time_ns_with_context = duckdb_v2_value_create_time_ns_with_context;
	result.duckdb_v2_value_create_time_tz_with_connection = duckdb_v2_value_create_time_tz_with_connection;
	result.duckdb_v2_value_create_time_tz_with_context = duckdb_v2_value_create_time_tz_with_context;
	result.duckdb_v2_value_create_time_with_connection = duckdb_v2_value_create_time_with_connection;
	result.duckdb_v2_value_create_time_with_context = duckdb_v2_value_create_time_with_context;
	result.duckdb_v2_value_create_timestamp_ms_with_connection = duckdb_v2_value_create_timestamp_ms_with_connection;
	result.duckdb_v2_value_create_timestamp_ms_with_context = duckdb_v2_value_create_timestamp_ms_with_context;
	result.duckdb_v2_value_create_timestamp_ns_with_connection = duckdb_v2_value_create_timestamp_ns_with_connection;
	result.duckdb_v2_value_create_timestamp_ns_with_context = duckdb_v2_value_create_timestamp_ns_with_context;
	result.duckdb_v2_value_create_timestamp_sec_with_connection = duckdb_v2_value_create_timestamp_sec_with_connection;
	result.duckdb_v2_value_create_timestamp_sec_with_context = duckdb_v2_value_create_timestamp_sec_with_context;
	result.duckdb_v2_value_create_timestamp_tz_ns_with_connection =
	    duckdb_v2_value_create_timestamp_tz_ns_with_connection;
	result.duckdb_v2_value_create_timestamp_tz_ns_with_context = duckdb_v2_value_create_timestamp_tz_ns_with_context;
	result.duckdb_v2_value_create_timestamp_tz_with_connection = duckdb_v2_value_create_timestamp_tz_with_connection;
	result.duckdb_v2_value_create_timestamp_tz_with_context = duckdb_v2_value_create_timestamp_tz_with_context;
	result.duckdb_v2_value_create_timestamp_with_connection = duckdb_v2_value_create_timestamp_with_connection;
	result.duckdb_v2_value_create_timestamp_with_context = duckdb_v2_value_create_timestamp_with_context;
	result.duckdb_v2_value_create_tinyint_with_connection = duckdb_v2_value_create_tinyint_with_connection;
	result.duckdb_v2_value_create_tinyint_with_context = duckdb_v2_value_create_tinyint_with_context;
	result.duckdb_v2_value_create_tuple_with_connection = duckdb_v2_value_create_tuple_with_connection;
	result.duckdb_v2_value_create_tuple_with_context = duckdb_v2_value_create_tuple_with_context;
	result.duckdb_v2_value_create_type_with_connection = duckdb_v2_value_create_type_with_connection;
	result.duckdb_v2_value_create_type_with_context = duckdb_v2_value_create_type_with_context;
	result.duckdb_v2_value_create_ubigint_with_connection = duckdb_v2_value_create_ubigint_with_connection;
	result.duckdb_v2_value_create_ubigint_with_context = duckdb_v2_value_create_ubigint_with_context;
	result.duckdb_v2_value_create_uhugeint_with_connection = duckdb_v2_value_create_uhugeint_with_connection;
	result.duckdb_v2_value_create_uhugeint_with_context = duckdb_v2_value_create_uhugeint_with_context;
	result.duckdb_v2_value_create_uint_with_connection = duckdb_v2_value_create_uint_with_connection;
	result.duckdb_v2_value_create_uint_with_context = duckdb_v2_value_create_uint_with_context;
	result.duckdb_v2_value_create_usmallint_with_connection = duckdb_v2_value_create_usmallint_with_connection;
	result.duckdb_v2_value_create_usmallint_with_context = duckdb_v2_value_create_usmallint_with_context;
	result.duckdb_v2_value_create_utinyint_with_connection = duckdb_v2_value_create_utinyint_with_connection;
	result.duckdb_v2_value_create_utinyint_with_context = duckdb_v2_value_create_utinyint_with_context;
	result.duckdb_v2_value_create_uuid_with_connection = duckdb_v2_value_create_uuid_with_connection;
	result.duckdb_v2_value_create_uuid_with_context = duckdb_v2_value_create_uuid_with_context;
	result.duckdb_v2_value_create_varchar_with_connection = duckdb_v2_value_create_varchar_with_connection;
	result.duckdb_v2_value_create_varchar_with_context = duckdb_v2_value_create_varchar_with_context;
	result.duckdb_v2_value_destroy = duckdb_v2_value_destroy;
	result.duckdb_v2_value_get_bigint = duckdb_v2_value_get_bigint;
	result.duckdb_v2_value_get_blob = duckdb_v2_value_get_blob;
	result.duckdb_v2_value_get_bool = duckdb_v2_value_get_bool;
	result.duckdb_v2_value_get_child = duckdb_v2_value_get_child;
	result.duckdb_v2_value_get_child_count = duckdb_v2_value_get_child_count;
	result.duckdb_v2_value_get_date = duckdb_v2_value_get_date;
	result.duckdb_v2_value_get_decimal = duckdb_v2_value_get_decimal;
	result.duckdb_v2_value_get_double = duckdb_v2_value_get_double;
	result.duckdb_v2_value_get_float = duckdb_v2_value_get_float;
	result.duckdb_v2_value_get_hugeint = duckdb_v2_value_get_hugeint;
	result.duckdb_v2_value_get_int = duckdb_v2_value_get_int;
	result.duckdb_v2_value_get_interval = duckdb_v2_value_get_interval;
	result.duckdb_v2_value_get_logical_type = duckdb_v2_value_get_logical_type;
	result.duckdb_v2_value_get_smallint = duckdb_v2_value_get_smallint;
	result.duckdb_v2_value_get_time = duckdb_v2_value_get_time;
	result.duckdb_v2_value_get_time_ns = duckdb_v2_value_get_time_ns;
	result.duckdb_v2_value_get_time_tz = duckdb_v2_value_get_time_tz;
	result.duckdb_v2_value_get_timestamp = duckdb_v2_value_get_timestamp;
	result.duckdb_v2_value_get_timestamp_ms = duckdb_v2_value_get_timestamp_ms;
	result.duckdb_v2_value_get_timestamp_ns = duckdb_v2_value_get_timestamp_ns;
	result.duckdb_v2_value_get_timestamp_sec = duckdb_v2_value_get_timestamp_sec;
	result.duckdb_v2_value_get_timestamp_tz = duckdb_v2_value_get_timestamp_tz;
	result.duckdb_v2_value_get_timestamp_tz_ns = duckdb_v2_value_get_timestamp_tz_ns;
	result.duckdb_v2_value_get_tinyint = duckdb_v2_value_get_tinyint;
	result.duckdb_v2_value_get_type = duckdb_v2_value_get_type;
	result.duckdb_v2_value_get_ubigint = duckdb_v2_value_get_ubigint;
	result.duckdb_v2_value_get_uhugeint = duckdb_v2_value_get_uhugeint;
	result.duckdb_v2_value_get_uint = duckdb_v2_value_get_uint;
	result.duckdb_v2_value_get_usmallint = duckdb_v2_value_get_usmallint;
	result.duckdb_v2_value_get_utinyint = duckdb_v2_value_get_utinyint;
	result.duckdb_v2_value_get_uuid = duckdb_v2_value_get_uuid;
	result.duckdb_v2_value_get_varchar = duckdb_v2_value_get_varchar;
	result.duckdb_v2_value_is_null = duckdb_v2_value_is_null;
	result.duckdb_v2_value_to_string = duckdb_v2_value_to_string;
	result.duckdb_v2_vector_constant_set_valid = duckdb_v2_vector_constant_set_valid;
	result.duckdb_v2_vector_flat_get_validity_mutable = duckdb_v2_vector_flat_get_validity_mutable;
	result.duckdb_v2_vector_flatten = duckdb_v2_vector_flatten;
	result.duckdb_v2_vector_get_arena = duckdb_v2_vector_get_arena;
	result.duckdb_v2_vector_get_child = duckdb_v2_vector_get_child;
	result.duckdb_v2_vector_get_child_count = duckdb_v2_vector_get_child_count;
	result.duckdb_v2_vector_get_data_mutable = duckdb_v2_vector_get_data_mutable;
	result.duckdb_v2_vector_get_logical_type = duckdb_v2_vector_get_logical_type;
	result.duckdb_v2_vector_get_size = duckdb_v2_vector_get_size;
	result.duckdb_v2_vector_get_value = duckdb_v2_vector_get_value;
	result.duckdb_v2_vector_get_vector_type = duckdb_v2_vector_get_vector_type;
	result.duckdb_v2_vector_get_view = duckdb_v2_vector_get_view;
	result.duckdb_v2_vector_make_constant = duckdb_v2_vector_make_constant;
	result.duckdb_v2_vector_make_sequence = duckdb_v2_vector_make_sequence;
	result.duckdb_v2_vector_reference = duckdb_v2_vector_reference;
	result.duckdb_v2_vector_set_null = duckdb_v2_vector_set_null;
	result.duckdb_v2_vector_set_size = duckdb_v2_vector_set_size;
	result.duckdb_v2_vector_set_value = duckdb_v2_vector_set_value;
	result.duckdb_v2_aggregate_function_bind_get_arg_count = duckdb_v2_aggregate_function_bind_get_arg_count;
	result.duckdb_v2_aggregate_function_bind_get_arg_type = duckdb_v2_aggregate_function_bind_get_arg_type;
	result.duckdb_v2_aggregate_function_bind_get_arg_value = duckdb_v2_aggregate_function_bind_get_arg_value;
	result.duckdb_v2_context_log = duckdb_v2_context_log;
	result.duckdb_v2_function_signature_add_parameter = duckdb_v2_function_signature_add_parameter;
	result.duckdb_v2_function_signature_set_return_type = duckdb_v2_function_signature_set_return_type;
	result.duckdb_v2_function_signature_set_varargs = duckdb_v2_function_signature_set_varargs;
	result.duckdb_v2_scalar_function_bind_get_arg_count = duckdb_v2_scalar_function_bind_get_arg_count;
	result.duckdb_v2_scalar_function_bind_get_arg_type = duckdb_v2_scalar_function_bind_get_arg_type;
	result.duckdb_v2_scalar_function_bind_get_arg_value = duckdb_v2_scalar_function_bind_get_arg_value;
	result.duckdb_v2_scalar_function_bind_get_user_data = duckdb_v2_scalar_function_bind_get_user_data;
	result.duckdb_v2_scalar_function_bind_set_bind_data = duckdb_v2_scalar_function_bind_set_bind_data;
	result.duckdb_v2_scalar_function_bind_set_return_type = duckdb_v2_scalar_function_bind_set_return_type;
	result.duckdb_v2_scalar_function_create_with_connection = duckdb_v2_scalar_function_create_with_connection;
	result.duckdb_v2_scalar_function_create_with_extension = duckdb_v2_scalar_function_create_with_extension;
	result.duckdb_v2_scalar_function_destroy = duckdb_v2_scalar_function_destroy;
	result.duckdb_v2_scalar_function_exec_get_arg = duckdb_v2_scalar_function_exec_get_arg;
	result.duckdb_v2_scalar_function_exec_get_arg_count = duckdb_v2_scalar_function_exec_get_arg_count;
	result.duckdb_v2_scalar_function_exec_get_bind_data = duckdb_v2_scalar_function_exec_get_bind_data;
	result.duckdb_v2_scalar_function_exec_get_init_data = duckdb_v2_scalar_function_exec_get_init_data;
	result.duckdb_v2_scalar_function_exec_get_result = duckdb_v2_scalar_function_exec_get_result;
	result.duckdb_v2_scalar_function_exec_get_row_count = duckdb_v2_scalar_function_exec_get_row_count;
	result.duckdb_v2_scalar_function_exec_get_user_data = duckdb_v2_scalar_function_exec_get_user_data;
	result.duckdb_v2_scalar_function_get_signature = duckdb_v2_scalar_function_get_signature;
	result.duckdb_v2_scalar_function_init_get_bind_data = duckdb_v2_scalar_function_init_get_bind_data;
	result.duckdb_v2_scalar_function_init_get_user_data = duckdb_v2_scalar_function_init_get_user_data;
	result.duckdb_v2_scalar_function_init_set_init_data = duckdb_v2_scalar_function_init_set_init_data;
	result.duckdb_v2_scalar_function_register = duckdb_v2_scalar_function_register;
	result.duckdb_v2_scalar_function_set_bind_callback = duckdb_v2_scalar_function_set_bind_callback;
	result.duckdb_v2_scalar_function_set_exec_callback = duckdb_v2_scalar_function_set_exec_callback;
	result.duckdb_v2_scalar_function_set_init_callback = duckdb_v2_scalar_function_set_init_callback;
	result.duckdb_v2_scalar_function_set_name = duckdb_v2_scalar_function_set_name;
	result.duckdb_v2_scalar_function_set_user_data = duckdb_v2_scalar_function_set_user_data;
	result.duckdb_v2_aggregate_function_bind_get_user_data = duckdb_v2_aggregate_function_bind_get_user_data;
	result.duckdb_v2_aggregate_function_bind_set_bind_data = duckdb_v2_aggregate_function_bind_set_bind_data;
	result.duckdb_v2_aggregate_function_bind_set_return_type = duckdb_v2_aggregate_function_bind_set_return_type;
	result.duckdb_v2_aggregate_function_combine_get_bind_data = duckdb_v2_aggregate_function_combine_get_bind_data;
	result.duckdb_v2_aggregate_function_combine_get_sources = duckdb_v2_aggregate_function_combine_get_sources;
	result.duckdb_v2_aggregate_function_combine_get_state_count = duckdb_v2_aggregate_function_combine_get_state_count;
	result.duckdb_v2_aggregate_function_combine_get_targets = duckdb_v2_aggregate_function_combine_get_targets;
	result.duckdb_v2_aggregate_function_combine_get_user_data = duckdb_v2_aggregate_function_combine_get_user_data;
	result.duckdb_v2_aggregate_function_create_with_connection = duckdb_v2_aggregate_function_create_with_connection;
	result.duckdb_v2_aggregate_function_create_with_extension = duckdb_v2_aggregate_function_create_with_extension;
	result.duckdb_v2_aggregate_function_destroy = duckdb_v2_aggregate_function_destroy;
	result.duckdb_v2_aggregate_function_destroy_get_bind_data = duckdb_v2_aggregate_function_destroy_get_bind_data;
	result.duckdb_v2_aggregate_function_destroy_get_state_count = duckdb_v2_aggregate_function_destroy_get_state_count;
	result.duckdb_v2_aggregate_function_destroy_get_states = duckdb_v2_aggregate_function_destroy_get_states;
	result.duckdb_v2_aggregate_function_destroy_get_user_data = duckdb_v2_aggregate_function_destroy_get_user_data;
	result.duckdb_v2_aggregate_function_finalize_get_bind_data = duckdb_v2_aggregate_function_finalize_get_bind_data;
	result.duckdb_v2_aggregate_function_finalize_get_result = duckdb_v2_aggregate_function_finalize_get_result;
	result.duckdb_v2_aggregate_function_finalize_get_result_offset =
	    duckdb_v2_aggregate_function_finalize_get_result_offset;
	result.duckdb_v2_aggregate_function_finalize_get_state_count =
	    duckdb_v2_aggregate_function_finalize_get_state_count;
	result.duckdb_v2_aggregate_function_finalize_get_states = duckdb_v2_aggregate_function_finalize_get_states;
	result.duckdb_v2_aggregate_function_finalize_get_user_data = duckdb_v2_aggregate_function_finalize_get_user_data;
	result.duckdb_v2_aggregate_function_get_signature = duckdb_v2_aggregate_function_get_signature;
	result.duckdb_v2_aggregate_function_init_get_bind_data = duckdb_v2_aggregate_function_init_get_bind_data;
	result.duckdb_v2_aggregate_function_init_get_state_count = duckdb_v2_aggregate_function_init_get_state_count;
	result.duckdb_v2_aggregate_function_init_get_states = duckdb_v2_aggregate_function_init_get_states;
	result.duckdb_v2_aggregate_function_init_get_user_data = duckdb_v2_aggregate_function_init_get_user_data;
	result.duckdb_v2_aggregate_function_register = duckdb_v2_aggregate_function_register;
	result.duckdb_v2_aggregate_function_set_bind_callback = duckdb_v2_aggregate_function_set_bind_callback;
	result.duckdb_v2_aggregate_function_set_combine_callback = duckdb_v2_aggregate_function_set_combine_callback;
	result.duckdb_v2_aggregate_function_set_destroy_callback = duckdb_v2_aggregate_function_set_destroy_callback;
	result.duckdb_v2_aggregate_function_set_finalize_callback = duckdb_v2_aggregate_function_set_finalize_callback;
	result.duckdb_v2_aggregate_function_set_init_callback = duckdb_v2_aggregate_function_set_init_callback;
	result.duckdb_v2_aggregate_function_set_name = duckdb_v2_aggregate_function_set_name;
	result.duckdb_v2_aggregate_function_set_size_callback = duckdb_v2_aggregate_function_set_size_callback;
	result.duckdb_v2_aggregate_function_set_update_callback = duckdb_v2_aggregate_function_set_update_callback;
	result.duckdb_v2_aggregate_function_set_user_data = duckdb_v2_aggregate_function_set_user_data;
	result.duckdb_v2_aggregate_function_size_get_bind_data = duckdb_v2_aggregate_function_size_get_bind_data;
	result.duckdb_v2_aggregate_function_size_get_user_data = duckdb_v2_aggregate_function_size_get_user_data;
	result.duckdb_v2_aggregate_function_size_set_state_size = duckdb_v2_aggregate_function_size_set_state_size;
	result.duckdb_v2_aggregate_function_update_get_arg = duckdb_v2_aggregate_function_update_get_arg;
	result.duckdb_v2_aggregate_function_update_get_arg_count = duckdb_v2_aggregate_function_update_get_arg_count;
	result.duckdb_v2_aggregate_function_update_get_bind_data = duckdb_v2_aggregate_function_update_get_bind_data;
	result.duckdb_v2_aggregate_function_update_get_row_count = duckdb_v2_aggregate_function_update_get_row_count;
	result.duckdb_v2_aggregate_function_update_get_states = duckdb_v2_aggregate_function_update_get_states;
	result.duckdb_v2_aggregate_function_update_get_user_data = duckdb_v2_aggregate_function_update_get_user_data;
	result.duckdb_v2_aggregate_function_set_property = duckdb_v2_aggregate_function_set_property;
	result.duckdb_v2_scalar_function_set_property = duckdb_v2_scalar_function_set_property;
	result.duckdb_v2_table_function_bind_add_result_column = duckdb_v2_table_function_bind_add_result_column;
	result.duckdb_v2_table_function_bind_get_arg_count = duckdb_v2_table_function_bind_get_arg_count;
	result.duckdb_v2_table_function_bind_get_arg_type = duckdb_v2_table_function_bind_get_arg_type;
	result.duckdb_v2_table_function_bind_get_arg_value = duckdb_v2_table_function_bind_get_arg_value;
	result.duckdb_v2_table_function_bind_get_user_data = duckdb_v2_table_function_bind_get_user_data;
	result.duckdb_v2_table_function_bind_set_bind_data = duckdb_v2_table_function_bind_set_bind_data;
	result.duckdb_v2_table_function_bind_set_cardinality = duckdb_v2_table_function_bind_set_cardinality;
	result.duckdb_v2_table_function_create_with_connection = duckdb_v2_table_function_create_with_connection;
	result.duckdb_v2_table_function_create_with_extension = duckdb_v2_table_function_create_with_extension;
	result.duckdb_v2_table_function_destroy = duckdb_v2_table_function_destroy;
	result.duckdb_v2_table_function_exec_get_bind_data = duckdb_v2_table_function_exec_get_bind_data;
	result.duckdb_v2_table_function_exec_get_global_state = duckdb_v2_table_function_exec_get_global_state;
	result.duckdb_v2_table_function_exec_get_local_state = duckdb_v2_table_function_exec_get_local_state;
	result.duckdb_v2_table_function_exec_get_output_chunk = duckdb_v2_table_function_exec_get_output_chunk;
	result.duckdb_v2_table_function_exec_get_user_data = duckdb_v2_table_function_exec_get_user_data;
	result.duckdb_v2_table_function_get_signature = duckdb_v2_table_function_get_signature;
	result.duckdb_v2_table_function_init_global_get_bind_data = duckdb_v2_table_function_init_global_get_bind_data;
	result.duckdb_v2_table_function_init_global_get_user_data = duckdb_v2_table_function_init_global_get_user_data;
	result.duckdb_v2_table_function_init_global_set_global_state =
	    duckdb_v2_table_function_init_global_set_global_state;
	result.duckdb_v2_table_function_init_global_set_max_threads = duckdb_v2_table_function_init_global_set_max_threads;
	result.duckdb_v2_table_function_init_local_get_bind_data = duckdb_v2_table_function_init_local_get_bind_data;
	result.duckdb_v2_table_function_init_local_get_global_state = duckdb_v2_table_function_init_local_get_global_state;
	result.duckdb_v2_table_function_init_local_get_user_data = duckdb_v2_table_function_init_local_get_user_data;
	result.duckdb_v2_table_function_init_local_set_local_state = duckdb_v2_table_function_init_local_set_local_state;
	result.duckdb_v2_table_function_progress_get_bind_data = duckdb_v2_table_function_progress_get_bind_data;
	result.duckdb_v2_table_function_progress_get_global_state = duckdb_v2_table_function_progress_get_global_state;
	result.duckdb_v2_table_function_progress_get_user_data = duckdb_v2_table_function_progress_get_user_data;
	result.duckdb_v2_table_function_progress_set_progress = duckdb_v2_table_function_progress_set_progress;
	result.duckdb_v2_table_function_register = duckdb_v2_table_function_register;
	result.duckdb_v2_table_function_set_bind_callback = duckdb_v2_table_function_set_bind_callback;
	result.duckdb_v2_table_function_set_exec_callback = duckdb_v2_table_function_set_exec_callback;
	result.duckdb_v2_table_function_set_init_global_callback = duckdb_v2_table_function_set_init_global_callback;
	result.duckdb_v2_table_function_set_init_local_callback = duckdb_v2_table_function_set_init_local_callback;
	result.duckdb_v2_table_function_set_name = duckdb_v2_table_function_set_name;
	result.duckdb_v2_table_function_set_progress_callback = duckdb_v2_table_function_set_progress_callback;
	result.duckdb_v2_table_function_set_user_data = duckdb_v2_table_function_set_user_data;
	result.duckdb_v2_cast_function_create_with_connection = duckdb_v2_cast_function_create_with_connection;
	result.duckdb_v2_cast_function_create_with_extension = duckdb_v2_cast_function_create_with_extension;
	result.duckdb_v2_cast_function_destroy = duckdb_v2_cast_function_destroy;
	result.duckdb_v2_cast_function_exec_get_input = duckdb_v2_cast_function_exec_get_input;
	result.duckdb_v2_cast_function_exec_get_mode = duckdb_v2_cast_function_exec_get_mode;
	result.duckdb_v2_cast_function_exec_get_output = duckdb_v2_cast_function_exec_get_output;
	result.duckdb_v2_cast_function_exec_get_row_count = duckdb_v2_cast_function_exec_get_row_count;
	result.duckdb_v2_cast_function_exec_get_user_data = duckdb_v2_cast_function_exec_get_user_data;
	result.duckdb_v2_cast_function_register = duckdb_v2_cast_function_register;
	result.duckdb_v2_cast_function_set_exec_callback = duckdb_v2_cast_function_set_exec_callback;
	result.duckdb_v2_cast_function_set_implicit_cast_cost = duckdb_v2_cast_function_set_implicit_cast_cost;
	result.duckdb_v2_cast_function_set_source_type = duckdb_v2_cast_function_set_source_type;
	result.duckdb_v2_cast_function_set_target_type = duckdb_v2_cast_function_set_target_type;
	result.duckdb_v2_cast_function_set_user_data = duckdb_v2_cast_function_set_user_data;
	result.duckdb_v2_column_data_collection_clear = duckdb_v2_column_data_collection_clear;
	result.duckdb_v2_custom_type_create_with_connection = duckdb_v2_custom_type_create_with_connection;
	result.duckdb_v2_custom_type_create_with_extension = duckdb_v2_custom_type_create_with_extension;
	result.duckdb_v2_custom_type_destroy = duckdb_v2_custom_type_destroy;
	result.duckdb_v2_custom_type_register = duckdb_v2_custom_type_register;
	result.duckdb_v2_custom_type_set_base_type = duckdb_v2_custom_type_set_base_type;
	result.duckdb_v2_custom_type_set_name = duckdb_v2_custom_type_set_name;
	result.duckdb_v2_file_close = duckdb_v2_file_close;
	result.duckdb_v2_file_destroy = duckdb_v2_file_destroy;
	result.duckdb_v2_file_open_options_create = duckdb_v2_file_open_options_create;
	result.duckdb_v2_file_open_options_destroy = duckdb_v2_file_open_options_destroy;
	result.duckdb_v2_file_open_options_set_flag = duckdb_v2_file_open_options_set_flag;
	result.duckdb_v2_file_open_options_set_value = duckdb_v2_file_open_options_set_value;
	result.duckdb_v2_file_read = duckdb_v2_file_read;
	result.duckdb_v2_file_read_at = duckdb_v2_file_read_at;
	result.duckdb_v2_file_seek = duckdb_v2_file_seek;
	result.duckdb_v2_file_size = duckdb_v2_file_size;
	result.duckdb_v2_file_sync = duckdb_v2_file_sync;
	result.duckdb_v2_file_system_get_from_connection = duckdb_v2_file_system_get_from_connection;
	result.duckdb_v2_file_system_get_from_context = duckdb_v2_file_system_get_from_context;
	result.duckdb_v2_file_system_open = duckdb_v2_file_system_open;
	result.duckdb_v2_file_tell = duckdb_v2_file_tell;
	result.duckdb_v2_file_write = duckdb_v2_file_write;
	result.duckdb_v2_file_write_at = duckdb_v2_file_write_at;
	result.duckdb_v2_qname_create = duckdb_v2_qname_create;
	result.duckdb_v2_qname_destroy = duckdb_v2_qname_destroy;
	result.duckdb_v2_qname_equals = duckdb_v2_qname_equals;
	result.duckdb_v2_qname_get_part = duckdb_v2_qname_get_part;
	result.duckdb_v2_qname_get_part_count = duckdb_v2_qname_get_part_count;
	result.duckdb_v2_qname_hash = duckdb_v2_qname_hash;
	result.duckdb_v2_qname_parse = duckdb_v2_qname_parse;
	result.duckdb_v2_qname_render = duckdb_v2_qname_render;
	result.duckdb_v2_replacement_scan_add_argument = duckdb_v2_replacement_scan_add_argument;
	result.duckdb_v2_replacement_scan_add_named_argument = duckdb_v2_replacement_scan_add_named_argument;
	result.duckdb_v2_replacement_scan_create_with_connection = duckdb_v2_replacement_scan_create_with_connection;
	result.duckdb_v2_replacement_scan_create_with_database = duckdb_v2_replacement_scan_create_with_database;
	result.duckdb_v2_replacement_scan_create_with_extension = duckdb_v2_replacement_scan_create_with_extension;
	result.duckdb_v2_replacement_scan_destroy = duckdb_v2_replacement_scan_destroy;
	result.duckdb_v2_replacement_scan_get_name = duckdb_v2_replacement_scan_get_name;
	result.duckdb_v2_replacement_scan_get_user_data = duckdb_v2_replacement_scan_get_user_data;
	result.duckdb_v2_replacement_scan_register = duckdb_v2_replacement_scan_register;
	result.duckdb_v2_replacement_scan_set_alias = duckdb_v2_replacement_scan_set_alias;
	result.duckdb_v2_replacement_scan_set_callback = duckdb_v2_replacement_scan_set_callback;
	result.duckdb_v2_replacement_scan_set_collection = duckdb_v2_replacement_scan_set_collection;
	result.duckdb_v2_replacement_scan_set_function_name = duckdb_v2_replacement_scan_set_function_name;
	result.duckdb_v2_replacement_scan_set_subquery = duckdb_v2_replacement_scan_set_subquery;
	result.duckdb_v2_replacement_scan_set_user_data = duckdb_v2_replacement_scan_set_user_data;
	result.duckdb_v2_arrow_exporter_append = duckdb_v2_arrow_exporter_append;
	result.duckdb_v2_arrow_exporter_create = duckdb_v2_arrow_exporter_create;
	result.duckdb_v2_arrow_exporter_destroy = duckdb_v2_arrow_exporter_destroy;
	result.duckdb_v2_arrow_exporter_get_schema = duckdb_v2_arrow_exporter_get_schema;
	result.duckdb_v2_arrow_exporter_next_array = duckdb_v2_arrow_exporter_next_array;
	result.duckdb_v2_arrow_importer_append = duckdb_v2_arrow_importer_append;
	result.duckdb_v2_arrow_importer_create = duckdb_v2_arrow_importer_create;
	result.duckdb_v2_arrow_importer_destroy = duckdb_v2_arrow_importer_destroy;
	result.duckdb_v2_arrow_importer_get_schema = duckdb_v2_arrow_importer_get_schema;
	result.duckdb_v2_arrow_importer_next_chunk = duckdb_v2_arrow_importer_next_chunk;
	result.duckdb_v2_column_description_destroy = duckdb_v2_column_description_destroy;
	result.duckdb_v2_column_description_get_name = duckdb_v2_column_description_get_name;
	result.duckdb_v2_column_description_get_type = duckdb_v2_column_description_get_type;
	result.duckdb_v2_column_description_has_default = duckdb_v2_column_description_has_default;
	result.duckdb_v2_column_description_has_generated = duckdb_v2_column_description_has_generated;
	result.duckdb_v2_connection_describe_table = duckdb_v2_connection_describe_table;
	result.duckdb_v2_copy_from_bind_get_column_count = duckdb_v2_copy_from_bind_get_column_count;
	result.duckdb_v2_copy_from_bind_get_column_name = duckdb_v2_copy_from_bind_get_column_name;
	result.duckdb_v2_copy_from_bind_get_column_type = duckdb_v2_copy_from_bind_get_column_type;
	result.duckdb_v2_copy_from_bind_get_file_path = duckdb_v2_copy_from_bind_get_file_path;
	result.duckdb_v2_copy_from_bind_get_option_count = duckdb_v2_copy_from_bind_get_option_count;
	result.duckdb_v2_copy_from_bind_get_option_name = duckdb_v2_copy_from_bind_get_option_name;
	result.duckdb_v2_copy_from_bind_get_option_value = duckdb_v2_copy_from_bind_get_option_value;
	result.duckdb_v2_copy_from_bind_get_user_data = duckdb_v2_copy_from_bind_get_user_data;
	result.duckdb_v2_copy_from_bind_set_bind_data = duckdb_v2_copy_from_bind_set_bind_data;
	result.duckdb_v2_copy_from_bind_set_cardinality = duckdb_v2_copy_from_bind_set_cardinality;
	result.duckdb_v2_copy_from_exec_get_bind_data = duckdb_v2_copy_from_exec_get_bind_data;
	result.duckdb_v2_copy_from_exec_get_global_state = duckdb_v2_copy_from_exec_get_global_state;
	result.duckdb_v2_copy_from_exec_get_local_state = duckdb_v2_copy_from_exec_get_local_state;
	result.duckdb_v2_copy_from_exec_get_output_chunk = duckdb_v2_copy_from_exec_get_output_chunk;
	result.duckdb_v2_copy_from_exec_get_user_data = duckdb_v2_copy_from_exec_get_user_data;
	result.duckdb_v2_copy_from_init_global_get_bind_data = duckdb_v2_copy_from_init_global_get_bind_data;
	result.duckdb_v2_copy_from_init_global_get_user_data = duckdb_v2_copy_from_init_global_get_user_data;
	result.duckdb_v2_copy_from_init_global_set_global_state = duckdb_v2_copy_from_init_global_set_global_state;
	result.duckdb_v2_copy_from_init_global_set_max_threads = duckdb_v2_copy_from_init_global_set_max_threads;
	result.duckdb_v2_copy_from_init_local_get_bind_data = duckdb_v2_copy_from_init_local_get_bind_data;
	result.duckdb_v2_copy_from_init_local_get_global_state = duckdb_v2_copy_from_init_local_get_global_state;
	result.duckdb_v2_copy_from_init_local_get_user_data = duckdb_v2_copy_from_init_local_get_user_data;
	result.duckdb_v2_copy_from_init_local_set_local_state = duckdb_v2_copy_from_init_local_set_local_state;
	result.duckdb_v2_copy_from_progress_get_bind_data = duckdb_v2_copy_from_progress_get_bind_data;
	result.duckdb_v2_copy_from_progress_get_global_state = duckdb_v2_copy_from_progress_get_global_state;
	result.duckdb_v2_copy_from_progress_get_user_data = duckdb_v2_copy_from_progress_get_user_data;
	result.duckdb_v2_copy_from_progress_set_progress = duckdb_v2_copy_from_progress_set_progress;
	result.duckdb_v2_copy_from_set_bind_callback = duckdb_v2_copy_from_set_bind_callback;
	result.duckdb_v2_copy_from_set_exec_callback = duckdb_v2_copy_from_set_exec_callback;
	result.duckdb_v2_copy_from_set_init_global_callback = duckdb_v2_copy_from_set_init_global_callback;
	result.duckdb_v2_copy_from_set_init_local_callback = duckdb_v2_copy_from_set_init_local_callback;
	result.duckdb_v2_copy_from_set_progress_callback = duckdb_v2_copy_from_set_progress_callback;
	result.duckdb_v2_copy_function_create_with_connection = duckdb_v2_copy_function_create_with_connection;
	result.duckdb_v2_copy_function_create_with_extension = duckdb_v2_copy_function_create_with_extension;
	result.duckdb_v2_copy_function_destroy = duckdb_v2_copy_function_destroy;
	result.duckdb_v2_copy_function_register = duckdb_v2_copy_function_register;
	result.duckdb_v2_copy_function_set_name = duckdb_v2_copy_function_set_name;
	result.duckdb_v2_copy_function_set_user_data = duckdb_v2_copy_function_set_user_data;
	result.duckdb_v2_copy_to_batch_get_bind_data = duckdb_v2_copy_to_batch_get_bind_data;
	result.duckdb_v2_copy_to_batch_get_init_data = duckdb_v2_copy_to_batch_get_init_data;
	result.duckdb_v2_copy_to_batch_get_user_data = duckdb_v2_copy_to_batch_get_user_data;
	result.duckdb_v2_copy_to_batch_set_batch_data = duckdb_v2_copy_to_batch_set_batch_data;
	result.duckdb_v2_copy_to_batch_size_get_bind_data = duckdb_v2_copy_to_batch_size_get_bind_data;
	result.duckdb_v2_copy_to_batch_size_get_user_data = duckdb_v2_copy_to_batch_size_get_user_data;
	result.duckdb_v2_copy_to_batch_size_set_target = duckdb_v2_copy_to_batch_size_set_target;
	result.duckdb_v2_copy_to_batch_take_input = duckdb_v2_copy_to_batch_take_input;
	result.duckdb_v2_copy_to_bind_get_column_count = duckdb_v2_copy_to_bind_get_column_count;
	result.duckdb_v2_copy_to_bind_get_column_name = duckdb_v2_copy_to_bind_get_column_name;
	result.duckdb_v2_copy_to_bind_get_column_type = duckdb_v2_copy_to_bind_get_column_type;
	result.duckdb_v2_copy_to_bind_get_file_path = duckdb_v2_copy_to_bind_get_file_path;
	result.duckdb_v2_copy_to_bind_get_option_count = duckdb_v2_copy_to_bind_get_option_count;
	result.duckdb_v2_copy_to_bind_get_option_name = duckdb_v2_copy_to_bind_get_option_name;
	result.duckdb_v2_copy_to_bind_get_option_value = duckdb_v2_copy_to_bind_get_option_value;
	result.duckdb_v2_copy_to_bind_get_user_data = duckdb_v2_copy_to_bind_get_user_data;
	result.duckdb_v2_copy_to_bind_set_bind_data = duckdb_v2_copy_to_bind_set_bind_data;
	result.duckdb_v2_copy_to_finalize_get_bind_data = duckdb_v2_copy_to_finalize_get_bind_data;
	result.duckdb_v2_copy_to_finalize_get_init_data = duckdb_v2_copy_to_finalize_get_init_data;
	result.duckdb_v2_copy_to_finalize_get_user_data = duckdb_v2_copy_to_finalize_get_user_data;
	result.duckdb_v2_copy_to_flush_get_batch_data = duckdb_v2_copy_to_flush_get_batch_data;
	result.duckdb_v2_copy_to_flush_get_bind_data = duckdb_v2_copy_to_flush_get_bind_data;
	result.duckdb_v2_copy_to_flush_get_init_data = duckdb_v2_copy_to_flush_get_init_data;
	result.duckdb_v2_copy_to_flush_get_user_data = duckdb_v2_copy_to_flush_get_user_data;
	result.duckdb_v2_copy_to_init_get_bind_data = duckdb_v2_copy_to_init_get_bind_data;
	result.duckdb_v2_copy_to_init_get_file_path = duckdb_v2_copy_to_init_get_file_path;
	result.duckdb_v2_copy_to_init_get_user_data = duckdb_v2_copy_to_init_get_user_data;
	result.duckdb_v2_copy_to_init_set_init_data = duckdb_v2_copy_to_init_set_init_data;
	result.duckdb_v2_copy_to_set_batch_callback = duckdb_v2_copy_to_set_batch_callback;
	result.duckdb_v2_copy_to_set_batch_size_callback = duckdb_v2_copy_to_set_batch_size_callback;
	result.duckdb_v2_copy_to_set_bind_callback = duckdb_v2_copy_to_set_bind_callback;
	result.duckdb_v2_copy_to_set_finalize_callback = duckdb_v2_copy_to_set_finalize_callback;
	result.duckdb_v2_copy_to_set_flush_callback = duckdb_v2_copy_to_set_flush_callback;
	result.duckdb_v2_copy_to_set_init_callback = duckdb_v2_copy_to_set_init_callback;
	result.duckdb_v2_expression_cast_get_mode = duckdb_v2_expression_cast_get_mode;
	result.duckdb_v2_expression_column_ref_get_index = duckdb_v2_expression_column_ref_get_index;
	result.duckdb_v2_expression_constant_get_value = duckdb_v2_expression_constant_get_value;
	result.duckdb_v2_expression_function_get_name = duckdb_v2_expression_function_get_name;
	result.duckdb_v2_expression_function_get_qname = duckdb_v2_expression_function_get_qname;
	result.duckdb_v2_expression_get_child = duckdb_v2_expression_get_child;
	result.duckdb_v2_expression_get_child_count = duckdb_v2_expression_get_child_count;
	result.duckdb_v2_expression_get_return_type = duckdb_v2_expression_get_return_type;
	result.duckdb_v2_expression_get_type = duckdb_v2_expression_get_type;
	result.duckdb_v2_identifier_render_quoted = duckdb_v2_identifier_render_quoted;
	result.duckdb_v2_prepared_statement_create = duckdb_v2_prepared_statement_create;
	result.duckdb_v2_prepared_statement_destroy = duckdb_v2_prepared_statement_destroy;
	result.duckdb_v2_prepared_statement_execute = duckdb_v2_prepared_statement_execute;
	result.duckdb_v2_prepared_statement_reuses_plan = duckdb_v2_prepared_statement_reuses_plan;
	result.duckdb_v2_result_to_arrow_stream = duckdb_v2_result_to_arrow_stream;
	result.duckdb_v2_table_description_destroy = duckdb_v2_table_description_destroy;
	result.duckdb_v2_table_description_get_column = duckdb_v2_table_description_get_column;
	result.duckdb_v2_table_description_get_column_count = duckdb_v2_table_description_get_column_count;
	result.duckdb_v2_table_description_get_qname = duckdb_v2_table_description_get_qname;
	result.duckdb_v2_table_description_is_readonly = duckdb_v2_table_description_is_readonly;
	result.duckdb_v2_table_function_exec_get_column_count = duckdb_v2_table_function_exec_get_column_count;
	result.duckdb_v2_table_function_exec_get_column_index = duckdb_v2_table_function_exec_get_column_index;
	result.duckdb_v2_table_function_filter_pushdown_accept = duckdb_v2_table_function_filter_pushdown_accept;
	result.duckdb_v2_table_function_filter_pushdown_get_bind_data =
	    duckdb_v2_table_function_filter_pushdown_get_bind_data;
	result.duckdb_v2_table_function_filter_pushdown_get_column_count =
	    duckdb_v2_table_function_filter_pushdown_get_column_count;
	result.duckdb_v2_table_function_filter_pushdown_get_column_index =
	    duckdb_v2_table_function_filter_pushdown_get_column_index;
	result.duckdb_v2_table_function_filter_pushdown_get_filter = duckdb_v2_table_function_filter_pushdown_get_filter;
	result.duckdb_v2_table_function_filter_pushdown_get_filter_count =
	    duckdb_v2_table_function_filter_pushdown_get_filter_count;
	result.duckdb_v2_table_function_filter_pushdown_get_user_data =
	    duckdb_v2_table_function_filter_pushdown_get_user_data;
	result.duckdb_v2_table_function_init_global_get_column_count =
	    duckdb_v2_table_function_init_global_get_column_count;
	result.duckdb_v2_table_function_init_global_get_column_index =
	    duckdb_v2_table_function_init_global_get_column_index;
	result.duckdb_v2_table_function_init_local_get_column_count = duckdb_v2_table_function_init_local_get_column_count;
	result.duckdb_v2_table_function_init_local_get_column_index = duckdb_v2_table_function_init_local_get_column_index;
	result.duckdb_v2_table_function_set_filter_pushdown_callback =
	    duckdb_v2_table_function_set_filter_pushdown_callback;
	result.duckdb_v2_table_function_set_projection_pushdown = duckdb_v2_table_function_set_projection_pushdown;
	return result;
}

#define DUCKDB_EXTENSION_API_V2_VERSION_MAJOR  2
#define DUCKDB_EXTENSION_API_V2_VERSION_MINOR  0
#define DUCKDB_EXTENSION_API_V2_VERSION_PATCH  0
#define DUCKDB_EXTENSION_API_V2_VERSION_STRING "v2.0.0"
