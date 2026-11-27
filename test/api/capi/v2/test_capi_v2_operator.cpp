#include "test_capi_v2.hpp"

#include <atomic>
#include <string>

// ---------------------------------------------------------------------------
// V2 operator tests: define a streaming operator, hand it to a table function
// with table_function_set_operator, and run the resulting table in-out
// function through SQL - with constant arguments, LATERAL arguments, and a
// table argument.
//
// Callbacks avoid Catch assertions: a REQUIRE would throw through the C
// callback boundary into the engine. On failure they populate the error slot
// and return, and the test asserts on the query error.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

// Named rather than anonymous: the unity build shares one anonymous namespace with the other test files
namespace operator_test {

duckdb_v2_table_function_handle MakeTable(duckdb_v2_connection_handle conn, std::string_view name) {
	duckdb_v2_table_function_handle function = nullptr;
	REQUIRE(duckdb_v2_table_function_create(FactoryOf(conn), &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_name(function, name.data(), name.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	return function;
}

duckdb_v2_operator_handle MakeOperator(duckdb_v2_connection_handle conn, duckdb_v2_operator_execute_callback_fn exec) {
	duckdb_v2_operator_handle op = nullptr;
	REQUIRE(duckdb_v2_operator_create(FactoryOf(conn), &op, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_operator_set_execute_callback(op, exec, nullptr) == DUCKDB_V2_ERROR_NONE);
	return op;
}

void AddParam(duckdb_v2_table_function_handle function, std::string_view name, duckdb_v2_logical_type_handle type,
              DUCKDB_V2_FUNCTION_PARAMETER_KIND kind = DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD) {
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_table_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, name.data(), name.size(), type, nullptr, kind, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
}

// Registers `function` with `op` producing its rows, then destroys both handles: the function keeps a copy.
DUCKDB_V2_ERROR Register(duckdb_v2_connection_handle conn, duckdb_v2_table_function_handle &function,
                         duckdb_v2_operator_handle &op, DUCKDB_V2_TABLE_OPERATOR_INPUT input,
                         std::string *error = nullptr) {
	REQUIRE(duckdb_v2_table_function_set_operator(function, op, input, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_operator_destroy(&op);
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = duckdb_v2_connection_register_table_function(conn, function, &err);
	if (err && error) {
		const char *text = nullptr;
		idx_t length = 0;
		duckdb_v2_error_info_get_text(err, &text, &length);
		*error = std::string(text, length);
	}
	duckdb_v2_error_info_destroy(&err);
	duckdb_v2_table_function_destroy(&function);
	return rc;
}

std::string QueryError(duckdb_v2_connection_handle conn, std::string_view sql);

int64_t QueryI64(duckdb_v2_connection_handle conn, std::string_view sql) {
	auto wrapped = "SELECT (" + std::string(sql) + ")::BIGINT";
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	if (Query(conn, wrapped, &result) == DUCKDB_V2_ERROR_NONE) {
		while (duckdb_v2_result_step(result, &chunk, &status, nullptr) == DUCKDB_V2_ERROR_NONE && !chunk &&
		       status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			duckdb_v2_result_wait(result, nullptr);
		}
	}
	if (!chunk) {
		duckdb_v2_result_destroy(&result);
		FAIL(QueryError(conn, wrapped));
	}
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto out = static_cast<const int64_t *>(view.data)[SelAt(view.sel, 0)];
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
	return out;
}

std::string QueryError(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = Query(conn, sql, &result, &err);
	while (rc == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		rc = duckdb_v2_result_step(result, &chunk, &status, &err);
		duckdb_v2_data_chunk_destroy(&chunk);
		if (rc != DUCKDB_V2_ERROR_NONE || status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED ||
		    status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED) {
			break;
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			rc = duckdb_v2_result_wait(result, &err);
		}
	}
	duckdb_v2_result_destroy(&result);
	std::string message;
	if (rc != DUCKDB_V2_ERROR_NONE && err) {
		const char *text = nullptr;
		idx_t text_length = 0;
		duckdb_v2_error_info_get_text(err, &text, &text_length);
		message = std::string(text, text_length);
	}
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	return message;
}

// Declares a BIGINT result column from inside a bind callback.
bool AddBigintColumn(duckdb_v2_table_function_bind_info_handle result, duckdb_v2_context_handle context,
                     std::string_view name, duckdb_v2_error_info_handle *err) {
	duckdb_v2_factory_handle factory = nullptr;
	duckdb_v2_context_get_factory(context, &factory, nullptr);
	duckdb_v2_logical_type_handle bigint = nullptr;
	if (duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, nullptr, nullptr, nullptr, 0,
	                                          &bigint, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	auto rc = duckdb_v2_table_function_bind_add_result_column(result, name.data(), name.size(), bigint, err);
	duckdb_v2_logical_type_destroy(&bigint);
	return rc == DUCKDB_V2_ERROR_NONE;
}

// Requires the operator's input to be a single BIGINT column.
bool RequireBigintInput(duckdb_v2_table_function_bind_info_handle result, duckdb_v2_error_info_handle *err) {
	idx_t count = 0;
	if (duckdb_v2_table_function_bind_get_input_column_count(result, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	duckdb_v2_logical_type_handle type = nullptr;
	if (count != 1 ||
	    duckdb_v2_table_function_bind_get_input_column_type(result, 0, &type, err) != DUCKDB_V2_ERROR_NONE) {
		SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "expected a single input column");
		return false;
	}
	DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	duckdb_v2_logical_type_get_id(type, &id, nullptr);
	duckdb_v2_logical_type_destroy(&type);
	if (id != DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT) {
		SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "expected a BIGINT input column");
		return false;
	}
	return true;
}

// The first input column and the first output column of an execute call.
struct ExecuteChunks {
	duckdb_v2_data_chunk_handle input = nullptr;
	duckdb_v2_vector_view in_view {};
	idx_t input_size = 0;
	duckdb_v2_vector_handle out = nullptr;
	int64_t *out_data = nullptr;
};

bool GetChunks(duckdb_v2_operator_execute_info_handle info, ExecuteChunks &chunks, duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle output = nullptr;
	duckdb_v2_vector_handle in_vec = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_operator_execute_get_input_chunk(info, &chunks.input, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_size(chunks.input, &chunks.input_size, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(chunks.input, 0, &in_vec, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_flatten(in_vec, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_view(in_vec, &chunks.in_view, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_operator_execute_get_output_chunk(info, &output, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(output, 0, &chunks.out, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(chunks.out, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	chunks.out_data = static_cast<int64_t *>(raw);
	return true;
}

int64_t InputAt(const ExecuteChunks &chunks, idx_t row) {
	return static_cast<const int64_t *>(chunks.in_view.data)[SelAt(chunks.in_view.sel, row)];
}

// ---------------------------------------------------------------------------
// my_double(TABLE): one BIGINT column "doubled", twice each input value.
// ---------------------------------------------------------------------------

void DoubleBindCb(duckdb_v2_function_bind_info_handle, duckdb_v2_table_function_bind_info_handle result,
                  duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	if (RequireBigintInput(result, err)) {
		AddBigintColumn(result, context, "doubled", err);
	}
}

void DoubleExecCb(duckdb_v2_operator_execute_info_handle info, duckdb_v2_context_handle,
                  duckdb_v2_error_info_handle *err) {
	ExecuteChunks chunks;
	if (!GetChunks(info, chunks, err)) {
		return;
	}
	for (idx_t i = 0; i < chunks.input_size; i++) {
		chunks.out_data[i] = 2 * InputAt(chunks, i);
	}
	duckdb_v2_vector_set_size(chunks.out, chunks.input_size, err);
}

void RegisterDouble(EnvFixture &fx) {
	auto function = MakeTable(fx.conn, "my_double");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DoubleBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_function_docs_handle docs = nullptr;
	REQUIRE(duckdb_v2_table_function_get_docs(function, &docs, nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string_view description = "doubles its input";
	REQUIRE(duckdb_v2_function_docs_set_description(docs, description.data(), description.size(), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, DoubleExecCb);
	REQUIRE(Register(fx.conn, function, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// explode(n): one BIGINT column "i" with 0..n-1 for every input row, n rows
// spread over as many output chunks as it takes.
// ---------------------------------------------------------------------------

struct ExplodeLocal {
	idx_t row = 0;
	int64_t position = 0;
};

void DeleteExplodeLocal(void *ptr) {
	delete static_cast<ExplodeLocal *>(ptr);
}

void ExplodeBindCb(duckdb_v2_function_bind_info_handle, duckdb_v2_table_function_bind_info_handle result,
                   duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	if (RequireBigintInput(result, err)) {
		AddBigintColumn(result, context, "i", err);
	}
}

void ExplodeInitLocalCb(duckdb_v2_operator_init_local_info_handle info, duckdb_v2_context_handle,
                        duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque state = {new ExplodeLocal(), DeleteExplodeLocal, nullptr};
	duckdb_v2_operator_init_local_set_local_state(info, &state, err);
}

void ExplodeExecCb(duckdb_v2_operator_execute_info_handle info, duckdb_v2_context_handle,
                   duckdb_v2_error_info_handle *err) {
	ExecuteChunks chunks;
	void *local_ptr = nullptr;
	if (!GetChunks(info, chunks, err) ||
	    duckdb_v2_operator_execute_get_local_state(info, &local_ptr, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &local = *static_cast<ExplodeLocal *>(local_ptr);
	idx_t produced = 0;
	while (local.row < chunks.input_size && produced < STANDARD_VECTOR_SIZE) {
		auto n = RowValid(chunks.in_view, SelAt(chunks.in_view.sel, local.row)) ? InputAt(chunks, local.row) : 0;
		if (local.position >= n) {
			local.row++;
			local.position = 0;
			continue;
		}
		chunks.out_data[produced++] = local.position++;
	}
	duckdb_v2_vector_set_size(chunks.out, produced, err);
	if (local.row < chunks.input_size) {
		duckdb_v2_operator_execute_set_result(info, DUCKDB_V2_OPERATOR_RESULT_HAVE_MORE_OUTPUT, err);
		return;
	}
	local.row = 0;
}

void RegisterExplode(EnvFixture &fx) {
	auto function = MakeTable(fx.conn, "explode");
	auto bigint = MakeType(fx.factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	AddParam(function, "n", bigint);
	duckdb_v2_logical_type_destroy(&bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, ExplodeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, ExplodeExecCb);
	REQUIRE(duckdb_v2_operator_set_init_local_callback(op, ExplodeInitLocalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Register(fx.conn, function, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_ARGUMENTS) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// my_count(TABLE): buffers nothing but a per-thread row count, and emits it
// from the finalize callback - as the count, then a 0, over two invocations.
// ---------------------------------------------------------------------------

struct CountLocal {
	int64_t count = 0;
	int finalize_calls = 0;
};

void DeleteCountLocal(void *ptr) {
	delete static_cast<CountLocal *>(ptr);
}

void CountBindCb(duckdb_v2_function_bind_info_handle, duckdb_v2_table_function_bind_info_handle result,
                 duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	AddBigintColumn(result, context, "cnt", err);
}

void CountInitGlobalCb(duckdb_v2_operator_init_global_info_handle info, duckdb_v2_context_handle,
                       duckdb_v2_error_info_handle *err) {
	void *bind_ptr = nullptr;
	if (duckdb_v2_operator_init_global_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// the bind data says how many threads may run it
	if (bind_ptr) {
		duckdb_v2_operator_init_global_set_max_threads(info, *static_cast<idx_t *>(bind_ptr), err);
	}
}

void CountInitLocalCb(duckdb_v2_operator_init_local_info_handle info, duckdb_v2_context_handle,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque state = {new CountLocal(), DeleteCountLocal, nullptr};
	duckdb_v2_operator_init_local_set_local_state(info, &state, err);
}

void CountExecCb(duckdb_v2_operator_execute_info_handle info, duckdb_v2_context_handle,
                 duckdb_v2_error_info_handle *err) {
	void *local_ptr = nullptr;
	duckdb_v2_data_chunk_handle input = nullptr;
	idx_t size = 0;
	if (duckdb_v2_operator_execute_get_local_state(info, &local_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_operator_execute_get_input_chunk(info, &input, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_size(input, &size, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	static_cast<CountLocal *>(local_ptr)->count += static_cast<int64_t>(size);
}

void CountFinalizeCb(duckdb_v2_operator_finalize_info_handle info, duckdb_v2_context_handle,
                     duckdb_v2_error_info_handle *err) {
	void *local_ptr = nullptr;
	duckdb_v2_data_chunk_handle output = nullptr;
	duckdb_v2_vector_handle out = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_operator_finalize_get_local_state(info, &local_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_operator_finalize_get_output_chunk(info, &output, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(output, 0, &out, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(out, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &local = *static_cast<CountLocal *>(local_ptr);
	auto first = local.finalize_calls++ == 0;
	static_cast<int64_t *>(raw)[0] = first ? local.count : 0;
	duckdb_v2_vector_set_size(out, 1, err);
	duckdb_v2_operator_finalize_set_have_more_output(info, first, err);
}

void SingleThreadBindCb(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
                        duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	if (!AddBigintColumn(result, context, "cnt", err)) {
		return;
	}
	duckdb_v2_opaque bind_data = {new idx_t(1), [](void *ptr) { delete static_cast<idx_t *>(ptr); }, nullptr};
	duckdb_v2_function_bind_set_bind_data(info, &bind_data, err);
}

void RegisterCount(EnvFixture &fx, std::string_view name, duckdb_v2_table_function_bind_callback_fn bind) {
	auto function = MakeTable(fx.conn, name);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, bind, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, CountExecCb);
	REQUIRE(duckdb_v2_operator_set_init_global_callback(op, CountInitGlobalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_operator_set_init_local_callback(op, CountInitLocalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_operator_set_finalize_callback(op, CountFinalizeCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Register(fx.conn, function, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) == DUCKDB_V2_ERROR_NONE);
}

// ---------------------------------------------------------------------------
// my_take(TABLE, n := BIGINT): passes the first n input rows through, then
// reports that it needs no more input.
// ---------------------------------------------------------------------------

struct TakeBind {
	int64_t limit = 0;
};
struct TakeGlobal {
	std::atomic<int64_t> taken {0};
};

void TakeBindCb(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
                duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	// the table is the first argument, so the named parameter follows it
	idx_t index = 0;
	bool found = false;
	duckdb_v2_value_handle value = nullptr;
	int64_t limit = 0;
	if (duckdb_v2_function_bind_get_arg_index(info, "n", 1, &index, &found, err) != DUCKDB_V2_ERROR_NONE || !found ||
	    duckdb_v2_function_bind_get_arg_value(info, index, &value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto rc = duckdb_v2_value_get_bigint(value, &limit, err);
	duckdb_v2_value_destroy(&value);
	if (rc != DUCKDB_V2_ERROR_NONE || !RequireBigintInput(result, err) || !AddBigintColumn(result, context, "v", err)) {
		return;
	}
	duckdb_v2_opaque bind_data = {new TakeBind {limit}, [](void *ptr) { delete static_cast<TakeBind *>(ptr); },
	                              nullptr};
	duckdb_v2_function_bind_set_bind_data(info, &bind_data, err);
}

void TakeInitGlobalCb(duckdb_v2_operator_init_global_info_handle info, duckdb_v2_context_handle,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque state = {new TakeGlobal(), [](void *ptr) { delete static_cast<TakeGlobal *>(ptr); }, nullptr};
	if (duckdb_v2_operator_init_global_set_global_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_operator_init_global_set_max_threads(info, 1, err);
}

void TakeExecCb(duckdb_v2_operator_execute_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	ExecuteChunks chunks;
	void *bind_ptr = nullptr;
	void *global_ptr = nullptr;
	if (!GetChunks(info, chunks, err) ||
	    duckdb_v2_operator_execute_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_operator_execute_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto limit = static_cast<TakeBind *>(bind_ptr)->limit;
	auto &global = *static_cast<TakeGlobal *>(global_ptr);
	idx_t produced = 0;
	while (produced < chunks.input_size && global.taken < limit) {
		chunks.out_data[produced] = InputAt(chunks, produced);
		produced++;
		global.taken++;
	}
	duckdb_v2_vector_set_size(chunks.out, produced, err);
	if (global.taken >= limit) {
		duckdb_v2_operator_execute_set_result(info, DUCKDB_V2_OPERATOR_RESULT_FINISHED, err);
	}
}

// ---------------------------------------------------------------------------
// echo(TABLE): returns its input as-is, named and typed after it.
// ---------------------------------------------------------------------------

void EchoBindCb(duckdb_v2_function_bind_info_handle, duckdb_v2_table_function_bind_info_handle result,
                duckdb_v2_context_handle, duckdb_v2_error_info_handle *err) {
	idx_t count = 0;
	if (duckdb_v2_table_function_bind_get_input_column_count(result, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		const char *name = nullptr;
		idx_t name_length = 0;
		duckdb_v2_logical_type_handle type = nullptr;
		if (duckdb_v2_table_function_bind_get_input_column_name(result, i, &name, &name_length, err) !=
		        DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_table_function_bind_get_input_column_type(result, i, &type, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		std::string echoed = "echo_" + std::string(name, name_length);
		auto rc = duckdb_v2_table_function_bind_add_result_column(result, echoed.data(), echoed.size(), type, err);
		duckdb_v2_logical_type_destroy(&type);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
}

void EchoExecCb(duckdb_v2_operator_execute_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle input = nullptr;
	duckdb_v2_data_chunk_handle output = nullptr;
	idx_t count = 0;
	if (duckdb_v2_operator_execute_get_input_chunk(info, &input, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_operator_execute_get_output_chunk(info, &output, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector_count(input, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_vector_handle source = nullptr;
		duckdb_v2_vector_handle target = nullptr;
		if (duckdb_v2_data_chunk_get_vector(input, i, &source, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_data_chunk_get_vector(output, i, &target, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_reference(target, source, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
}

// ---------------------------------------------------------------------------
// Callbacks reporting failures, and the user data every callback sees.
// ---------------------------------------------------------------------------

void FailingExecCb(duckdb_v2_operator_execute_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *err) {
	SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "execute refused");
}

void FailingFinalizeCb(duckdb_v2_operator_finalize_info_handle, duckdb_v2_context_handle,
                       duckdb_v2_error_info_handle *err) {
	SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "finalize refused");
}

void OverflowExecCb(duckdb_v2_operator_execute_info_handle info, duckdb_v2_context_handle,
                    duckdb_v2_error_info_handle *err) {
	duckdb_v2_data_chunk_handle output = nullptr;
	duckdb_v2_vector_handle out = nullptr;
	if (duckdb_v2_operator_execute_get_output_chunk(info, &output, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(output, 0, &out, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_set_size(out, STANDARD_VECTOR_SIZE + 1, err);
}

std::atomic<int> user_data_seen {0};
std::atomic<int> user_data_destroyed {0};
int user_data_marker = 42;

bool IsMarker(void *data) {
	return data == &user_data_marker;
}

void UserDataInitGlobalCb(duckdb_v2_operator_init_global_info_handle info, duckdb_v2_context_handle,
                          duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_operator_init_global_get_user_data(info, &data, err);
	user_data_seen += IsMarker(data);
}

void UserDataInitLocalCb(duckdb_v2_operator_init_local_info_handle info, duckdb_v2_context_handle,
                         duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_operator_init_local_get_user_data(info, &data, err);
	user_data_seen += IsMarker(data);
}

void UserDataExecCb(duckdb_v2_operator_execute_info_handle info, duckdb_v2_context_handle context,
                    duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_operator_execute_get_user_data(info, &data, err);
	user_data_seen += IsMarker(data);
	DoubleExecCb(info, context, err);
}

void UserDataFinalizeCb(duckdb_v2_operator_finalize_info_handle info, duckdb_v2_context_handle,
                        duckdb_v2_error_info_handle *err) {
	void *data = nullptr;
	duckdb_v2_operator_finalize_get_user_data(info, &data, err);
	user_data_seen += IsMarker(data);
}

// ===========================================================================
// Table input: the rows of a subquery, chunk by chunk.
// ===========================================================================

TEST_CASE("V2 operator: table input transforms the rows of a subquery", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	RegisterDouble(fx);

	REQUIRE(QueryI64(fx.conn, "SELECT sum(doubled) FROM my_double((SELECT range FROM range(10)))") == 90);
	// spanning many input chunks, possibly on several threads
	REQUIRE(QueryI64(fx.conn, "SELECT sum(doubled) FROM my_double((SELECT range FROM range(100000)))") ==
	        99999LL * 100000LL);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_double((SELECT range FROM range(100000)))") == 100000);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_double((SELECT range FROM range(0)))") == 0);
	// the input is cast to the declared input column type
	REQUIRE(QueryI64(fx.conn, "SELECT sum(doubled) FROM my_double((SELECT 21::BIGINT))") == 42);
	// the documentation describes the overload, table parameter included
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM duckdb_functions() WHERE function_name = 'my_double' AND "
	                          "description = 'doubles its input' AND parameter_types = ['TABLE']") == 1);

	// the table is the only positional argument, and it must be a subquery
	REQUIRE(QueryError(fx.conn, "SELECT * FROM my_double(1)").find("subquery") != std::string::npos);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM my_double((SELECT 'a' AS s))").find("BIGINT input") !=
	        std::string::npos);
}

TEST_CASE("V2 operator: bind sees the names and types of the input columns", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	auto function = MakeTable(fx.conn, "echo");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, EchoBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, EchoExecCb);
	REQUIRE(Register(fx.conn, function, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT * FROM echo((SELECT 1::INTEGER AS a, 'x' AS b, [1, 2] AS c))", &result) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ColumnCount(result) == 3);
	RequireColumn(result, 0, "echo_a", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	RequireColumn(result, 1, "echo_b", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	RequireColumn(result, 2, "echo_c", DUCKDB_V2_LOGICAL_TYPE_ID_LIST);
	REQUIRE(DrainRowCount(result) == 1);
	duckdb_v2_result_destroy(&result);

	REQUIRE(QueryI64(fx.conn, "SELECT sum(echo_range) FROM echo((SELECT range FROM range(5000)))") == 4999 * 5000 / 2);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM echo((SELECT 's' || range AS s FROM range(3000)))") == 3000);
}

TEST_CASE("V2 operator: finalize flushes what the operator held back", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	RegisterCount(fx, "my_count", CountBindCb);
	RegisterCount(fx, "my_count_single", SingleThreadBindCb);

	// every thread emits its count, then a 0
	REQUIRE(QueryI64(fx.conn, "SELECT sum(cnt) FROM my_count((SELECT * FROM range(100000)))") == 100000);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) % 2 FROM my_count((SELECT * FROM range(100000)))") == 0);
	REQUIRE(QueryI64(fx.conn, "SELECT sum(cnt) FROM my_count((SELECT * FROM range(0)))") == 0);
	// a single thread runs it when the global init callback says so
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_count_single((SELECT * FROM range(100000)))") == 2);
	REQUIRE(QueryI64(fx.conn, "SELECT max(cnt) FROM my_count_single((SELECT * FROM range(100000)))") == 100000);
}

TEST_CASE("V2 operator: an operator that finishes early stops its input", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	auto function = MakeTable(fx.conn, "my_take");
	auto bigint = MakeType(fx.factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	AddParam(function, "n", bigint, DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_ONLY);
	duckdb_v2_logical_type_destroy(&bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, TakeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, TakeExecCb);
	REQUIRE(duckdb_v2_operator_set_init_global_callback(op, TakeInitGlobalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Register(fx.conn, function, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_take((SELECT range FROM range(100000)), n := 5)") == 5);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_take((SELECT range FROM range(100000)), n := 5000)") == 5000);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_take((SELECT range FROM range(3)), n := 5)") == 3);
	REQUIRE(QueryI64(fx.conn, "SELECT sum(v) FROM my_take((SELECT range FROM range(100)), n := 10)") == 45);
}

TEST_CASE("V2 operator: a table input correlated with an outer query", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	RegisterDouble(fx);
	RegisterCount(fx, "my_count", CountBindCb);

	// the operator runs once per row of the outer query
	REQUIRE(QueryI64(fx.conn, "SELECT sum(doubled) FROM range(4) t(n), my_double((SELECT t.n))") == 12);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM range(4) t(n), my_double((SELECT t.n)) WHERE doubled = 2 * n") ==
	        4);
	// which does not support a finalize callback
	REQUIRE(QueryError(fx.conn, "SELECT * FROM range(4) t(n), my_count((SELECT t.n))").find("finalize") !=
	        std::string::npos);
	REQUIRE(QueryI64(fx.conn, "SELECT 1") == 1);
}

// ===========================================================================
// Argument input: constant arguments, and LATERAL arguments.
// ===========================================================================

TEST_CASE("V2 operator: argument input runs once for constant arguments", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	RegisterExplode(fx);

	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM explode(3)") == 3);
	REQUIRE(QueryI64(fx.conn, "SELECT sum(i) FROM explode(3)") == 3);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM explode(0)") == 0);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM explode(NULL)") == 0);
	// many output chunks for a single input row
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM explode(10000)") == 10000);
	REQUIRE(QueryI64(fx.conn, "SELECT sum(i) FROM explode(10000)") == 9999LL * 10000LL / 2);
	// an uncorrelated subquery argument is streamed in
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM explode((SELECT 5000))") == 5000);
}

// explode_step(n, step := ...): checks at bind time that it reads the named parameter, also when the positional
// argument is a subquery whose rows are the operator's input and so has no value at bind time
void StepBindCb(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
                duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	idx_t index = 0;
	bool found = false;
	duckdb_v2_value_handle value = nullptr;
	int64_t step = 0;
	if (duckdb_v2_function_bind_get_arg_index(info, "step", 4, &index, &found, err) != DUCKDB_V2_ERROR_NONE || !found ||
	    duckdb_v2_function_bind_get_arg_value(info, index, &value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto rc = duckdb_v2_value_get_bigint(value, &step, err);
	duckdb_v2_value_destroy(&value);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (step != 2) {
		SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "unexpected step");
		return;
	}
	if (RequireBigintInput(result, err)) {
		AddBigintColumn(result, context, "i", err);
	}
}

TEST_CASE("V2 operator: argument input binds the named parameters after the input arguments",
          "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	auto function = MakeTable(fx.conn, "explode_step");
	auto bigint = MakeType(fx.factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	AddParam(function, "n", bigint);
	AddParam(function, "step", bigint, DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_ONLY);
	duckdb_v2_logical_type_destroy(&bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, StepBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, ExplodeExecCb);
	REQUIRE(duckdb_v2_operator_set_init_local_callback(op, ExplodeInitLocalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Register(fx.conn, function, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_ARGUMENTS) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM explode_step(3, step := 2)") == 3);
	REQUIRE(QueryError(fx.conn, "SELECT count(*) FROM explode_step(3, step := 5)").find("unexpected step") !=
	        std::string::npos);
	// a subquery argument is streamed in, so only the named parameter has a value at bind time
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM explode_step((SELECT 3), step := 2)") == 3);
	REQUIRE(QueryError(fx.conn, "SELECT count(*) FROM explode_step((SELECT 3), step := 5)").find("unexpected step") !=
	        std::string::npos);
}

TEST_CASE("V2 operator: argument input runs per row for LATERAL arguments", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	RegisterExplode(fx);

	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM range(1, 5) t(n), explode(t.n)") == 1 + 2 + 3 + 4);
	// every row the operator produces joins the row it was produced for
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM range(1, 5) t(n), explode(t.n) e WHERE e.i >= t.n") == 0);
	REQUIRE(QueryI64(fx.conn, "SELECT sum(n * 100 + i) FROM range(1, 4) t(n), explode(t.n)") ==
	        (100 + 0) + (200 + 0 + 200 + 1) + (300 + 0 + 300 + 1 + 300 + 2));
	// rows spanning many output chunks, and rows producing nothing
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM (VALUES (3000), (0), (NULL), (5000)) t(n), explode(t.n)") == 8000);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM range(3000) t(n), explode(t.n % 3)") == 3000);
}

// ===========================================================================
// State and user data.
// ===========================================================================

TEST_CASE("V2 operator: user data reaches every callback and is destroyed", "[capi_v2][capi_v2_operator]") {
	user_data_seen = 0;
	user_data_destroyed = 0;
	{
		EnvFixture fx;
		auto function = MakeTable(fx.conn, "user_data_op");
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DoubleBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		auto op = MakeOperator(fx.conn, UserDataExecCb);
		duckdb_v2_opaque data = {&user_data_marker, [](void *) { user_data_destroyed++; }, nullptr};
		REQUIRE(duckdb_v2_operator_set_user_data(op, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_operator_set_init_global_callback(op, UserDataInitGlobalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_operator_set_init_local_callback(op, UserDataInitLocalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_operator_set_finalize_callback(op, UserDataFinalizeCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		// one operator may be handed to several functions
		auto second = MakeTable(fx.conn, "user_data_op2");
		REQUIRE(duckdb_v2_table_function_set_bind_callback(second, DoubleBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_operator(second, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, second, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_table_function_destroy(&second);
		REQUIRE(Register(fx.conn, function, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(user_data_destroyed == 0);

		REQUIRE(QueryI64(fx.conn, "SELECT sum(doubled) FROM user_data_op((SELECT 1::BIGINT))") == 2);
		// init global, init local, execute and finalize at least once each
		REQUIRE(user_data_seen >= 4);
		REQUIRE(QueryI64(fx.conn, "SELECT sum(doubled) FROM user_data_op2((SELECT 2::BIGINT))") == 4);
	}
	REQUIRE(user_data_destroyed == 1);
}

// ===========================================================================
// Errors.
// ===========================================================================

TEST_CASE("V2 operator: callback errors fail the query", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	auto failing = MakeTable(fx.conn, "failing_exec");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(failing, DoubleBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, FailingExecCb);
	REQUIRE(Register(fx.conn, failing, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) == DUCKDB_V2_ERROR_NONE);

	auto failing_args = MakeTable(fx.conn, "failing_args");
	auto bigint = MakeType(fx.factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	AddParam(failing_args, "n", bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(failing_args, ExplodeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	op = MakeOperator(fx.conn, FailingExecCb);
	REQUIRE(Register(fx.conn, failing_args, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_ARGUMENTS) == DUCKDB_V2_ERROR_NONE);

	auto failing_final = MakeTable(fx.conn, "failing_finalize");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(failing_final, CountBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	op = MakeOperator(fx.conn, CountExecCb);
	REQUIRE(duckdb_v2_operator_set_init_local_callback(op, CountInitLocalCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_operator_set_finalize_callback(op, FailingFinalizeCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Register(fx.conn, failing_final, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE) == DUCKDB_V2_ERROR_NONE);

	auto overflow = MakeTable(fx.conn, "overflow");
	AddParam(overflow, "n", bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(overflow, ExplodeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	op = MakeOperator(fx.conn, OverflowExecCb);
	REQUIRE(Register(fx.conn, overflow, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_ARGUMENTS) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&bigint);

	REQUIRE(QueryError(fx.conn, "SELECT * FROM failing_exec((SELECT 1::BIGINT))").find("execute refused") !=
	        std::string::npos);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM failing_args(1)").find("execute refused") != std::string::npos);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM range(3) t(n), failing_args(t.n)").find("execute refused") !=
	        std::string::npos);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM failing_finalize((SELECT 1))").find("finalize refused") !=
	        std::string::npos);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM overflow(1)").find("more than") != std::string::npos);
	// the connection is still usable afterwards
	REQUIRE(QueryI64(fx.conn, "SELECT 1") == 1);
}

TEST_CASE("V2 operator: registration refusals", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	auto bigint = MakeType(fx.factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	auto refuse = [&](const char *expected, DUCKDB_V2_TABLE_OPERATOR_INPUT input, auto configure) {
		auto function = MakeTable(fx.conn, "refused");
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DoubleBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_operator_handle op = nullptr;
		REQUIRE(duckdb_v2_operator_create(fx.factory, &op, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_operator_set_execute_callback(op, DoubleExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		configure(function, op);
		std::string error;
		REQUIRE(Register(fx.conn, function, op, input, &error) == DUCKDB_V2_ERROR_INPUT_INVALID);
		INFO(error);
		REQUIRE(error.find(expected) != std::string::npos);
	};
	auto table = DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE;
	auto arguments = DUCKDB_V2_TABLE_OPERATOR_INPUT_ARGUMENTS;

	refuse("Execute callback must be set", table, [](duckdb_v2_table_function_handle, duckdb_v2_operator_handle op) {
		duckdb_v2_operator_set_execute_callback(op, nullptr, nullptr);
	});
	refuse("an exec callback", table, [&](duckdb_v2_table_function_handle function, duckdb_v2_operator_handle) {
		duckdb_v2_data_source_handle source = nullptr;
		REQUIRE(duckdb_v2_data_source_create(fx.factory, &source, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_data_source_set_exec_callback(
		    source,
		    [](duckdb_v2_data_source_exec_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {},
		    nullptr);
		duckdb_v2_table_function_set_data_source(function, source, nullptr);
		duckdb_v2_data_source_destroy(&source);
	});
	refuse("an init global callback", table, [&](duckdb_v2_table_function_handle function, duckdb_v2_operator_handle) {
		duckdb_v2_data_source_handle source = nullptr;
		REQUIRE(duckdb_v2_data_source_create(fx.factory, &source, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_data_source_set_init_global_callback(
		    source,
		    [](duckdb_v2_data_source_init_global_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
		    },
		    nullptr);
		duckdb_v2_table_function_set_data_source(function, source, nullptr);
		duckdb_v2_data_source_destroy(&source);
	});
	refuse("projection pushdown", table, [](duckdb_v2_table_function_handle function, duckdb_v2_operator_handle) {
		duckdb_v2_table_function_set_projection_pushdown(function, true, nullptr);
	});
	refuse(
	    "cannot declare positional parameters", table,
	    [&](duckdb_v2_table_function_handle function, duckdb_v2_operator_handle) { AddParam(function, "x", bigint); });
	refuse("cannot declare positional parameters", table,
	       [&](duckdb_v2_table_function_handle function, duckdb_v2_operator_handle) {
		       AddParam(function, "xs", bigint, DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_VARIADIC);
	       });
	refuse("cannot have a finalize callback", arguments,
	       [](duckdb_v2_table_function_handle, duckdb_v2_operator_handle op) {
		       duckdb_v2_operator_set_finalize_callback(op, CountFinalizeCb, nullptr);
	       });

	// an invalid input kind is refused where it is set
	auto function = MakeTable(fx.conn, "refused");
	auto op = MakeOperator(fx.conn, DoubleExecCb);
	REQUIRE(duckdb_v2_table_function_set_operator(function, op, static_cast<DUCKDB_V2_TABLE_OPERATOR_INPUT>(7),
	                                              nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_operator_destroy(&op);
	duckdb_v2_table_function_destroy(&function);

	// an operator created for another database is refused
	EnvFixture other;
	function = MakeTable(fx.conn, "refused");
	op = MakeOperator(other.conn, DoubleExecCb);
	REQUIRE(duckdb_v2_table_function_set_operator(function, op, table, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_operator_destroy(&op);
	duckdb_v2_table_function_destroy(&function);

	duckdb_v2_logical_type_destroy(&bigint);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM refused((SELECT 1))").find("refused") != std::string::npos);
}

TEST_CASE("V2 operator: a function with a table input cannot be overloaded", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	RegisterDouble(fx);
	auto overload = MakeTable(fx.conn, "my_double");
	auto bigint = MakeType(fx.factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	AddParam(overload, "x", bigint);
	duckdb_v2_logical_type_destroy(&bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(overload, DoubleBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto op = MakeOperator(fx.conn, DoubleExecCb);
	REQUIRE(Register(fx.conn, overload, op, DUCKDB_V2_TABLE_OPERATOR_INPUT_ARGUMENTS) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM my_double((SELECT 1::BIGINT))").find("overloads") != std::string::npos);
	REQUIRE(QueryI64(fx.conn, "SELECT 1") == 1);
}

TEST_CASE("V2 operator: invalid values and null arguments", "[capi_v2][capi_v2_operator]") {
	EnvFixture fx;
	REQUIRE(duckdb_v2_operator_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_operator_handle op = nullptr;
	REQUIRE(duckdb_v2_operator_destroy(&op) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_operator_create(nullptr, &op, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_operator_create(fx.factory, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_operator_set_execute_callback(nullptr, DoubleExecCb, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_operator_set_user_data(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_operator_execute_set_result(nullptr, DUCKDB_V2_OPERATOR_RESULT_FINISHED, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_operator_init_global_set_max_threads(nullptr, 1, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_set_operator(nullptr, nullptr, DUCKDB_V2_TABLE_OPERATOR_INPUT_TABLE, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_bind_get_input_column_count(nullptr, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

} // namespace operator_test

} // namespace test_capi_v2
