#include "test_capi_v2.hpp"

#include <cstring>

// ---------------------------------------------------------------------------
// V2 table function tests: build a function on a connection, configure its
// signature, register it, and scan it through SQL.
//
// Callbacks avoid Catch assertions: a REQUIRE would throw through the C
// callback boundary into the engine. On failure they populate the provided
// error slot and return; the failure then surfaces as a query error that the
// test asserts on. Cross-callback observations are latched into file-scope
// statics and asserted after the query.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

duckdb_v2_identifier_t TableIdent(const char *s) {
	return duckdb_v2_identifier_t {s, std::strlen(s)};
}

// Create a table function on the connection with the given name.
duckdb_v2_table_function_handle MakeTable(duckdb_v2_connection_handle conn, const char *name) {
	duckdb_v2_table_function_handle function = nullptr;
	REQUIRE(duckdb_v2_table_function_create_with_connection(conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto str = Convert(name);
	REQUIRE(duckdb_v2_table_function_set_name(function, &str, nullptr) == DUCKDB_V2_ERROR_NONE);
	return function;
}

// The function's borrowed signature.
duckdb_v2_function_signature_handle SigOf(duckdb_v2_table_function_handle function) {
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_table_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(sig != nullptr);
	return sig;
}

void TableSigParam(duckdb_v2_function_signature_handle sig, const char *name, duckdb_v2_logical_type_handle type,
                   duckdb_v2_value_handle default_value = nullptr) {
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, TableIdent(name), type, default_value, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
}

// Run a query producing a single BIGINT cell.
int64_t QueryI64(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql, &result) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto out = static_cast<const int64_t *>(view.data)[SelAt(view.sel, 0)];
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
	return out;
}

// Builds a type from inside a callback, where a context rather than a connection is in hand.
// Returns null after populating the error slot, so the caller can bail out.
duckdb_v2_logical_type_handle MakeTypeInCallback(duckdb_v2_context_handle context, DUCKDB_V2_LOGICAL_TYPE_ID id,
                                                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_logical_type_handle type = nullptr;
	if (duckdb_v2_context_create_type_from_id(context, id, nullptr, nullptr, 0, &type, err) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	return type;
}

// The message of a failing query, or the empty string when it succeeded.
std::string QueryError(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	duckdb_v2_error_info_handle err = nullptr;
	auto rc = Query(conn, sql, &result, &err);
	// Streaming execution is lazy: the failure only surfaces once stepping reaches it, which may
	// take several rounds. Drain until something fails or the stream ends.
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
		duckdb_v2_str text = {nullptr, 0};
		duckdb_v2_error_info_get_text(err, &text);
		message = Convert(text);
	}
	duckdb_v2_error_info_destroy(&err);
	REQUIRE(rc != DUCKDB_V2_ERROR_NONE);
	return message;
}

// ---------------------------------------------------------------------------
// my_range(n): one BIGINT column "i" carrying 0..n-1.
// ---------------------------------------------------------------------------

struct RangeBind {
	int64_t count = 0;
};
struct RangeGlobal {
	int64_t position = 0;
};

void DeleteRangeBind(void *ptr) {
	delete static_cast<RangeBind *>(ptr);
}
void DeleteRangeGlobal(void *ptr) {
	delete static_cast<RangeGlobal *>(ptr);
}

void RangeBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle value = nullptr;
	if (duckdb_v2_table_function_bind_get_arg_value(info, 0, &value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int64_t count = 0;
	auto rc = duckdb_v2_value_get_bigint(value, &count, err);
	duckdb_v2_value_destroy(&value);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	duckdb_v2_logical_type_handle bigint = nullptr;
	if (duckdb_v2_context_create_type_from_id(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, nullptr, nullptr, 0, &bigint,
	                                          err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	rc = duckdb_v2_table_function_bind_add_result_column(info, TableIdent("i"), bigint, err);
	duckdb_v2_logical_type_destroy(&bigint);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	// An exact row count is known here, so hand it to the optimizer directly.
	if (duckdb_v2_table_function_bind_set_cardinality(info, static_cast<idx_t>(count), true, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}

	duckdb_v2_opaque bind_data = {new RangeBind {count}, DeleteRangeBind, nullptr};
	duckdb_v2_table_function_bind_set_bind_data(info, &bind_data, err);
}

void RangeInitGlobalCb(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle,
                       duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque state = {new RangeGlobal {0}, DeleteRangeGlobal, nullptr};
	duckdb_v2_table_function_init_global_set_global_state(info, &state, err);
}

void RangeExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                 duckdb_v2_error_info_handle *err) {
	void *bind_ptr = nullptr;
	void *global_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	if (duckdb_v2_table_function_exec_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &bind = *static_cast<RangeBind *>(bind_ptr);
	auto &global = *static_cast<RangeGlobal *>(global_ptr);

	duckdb_v2_vector_handle vec = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(vec, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	auto remaining = bind.count - global.position;
	auto produced =
	    remaining < static_cast<int64_t>(STANDARD_VECTOR_SIZE) ? remaining : static_cast<int64_t>(STANDARD_VECTOR_SIZE);
	if (produced < 0) {
		produced = 0;
	}
	auto *out = static_cast<int64_t *>(raw);
	for (int64_t i = 0; i < produced; i++) {
		out[i] = global.position + i;
	}
	global.position += produced;

	// The first vector's size is the batch's row count; 0 ends the scan.
	duckdb_v2_vector_set_size(vec, static_cast<idx_t>(produced), err);
}

// Registers my_range on the connection.
void RegisterRange(duckdb_v2_connection_handle conn, const char *name = "my_range") {
	auto bigint = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	auto function = MakeTable(conn, name);
	TableSigParam(SigOf(function), "n", bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, RangeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_global_callback(function, RangeInitGlobalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&bigint);
}

// ---------------------------------------------------------------------------
// my_pairs(n): two columns, "a" INTEGER and "b" VARCHAR, produced in one batch.
// Pins that the row count taken from the first vector reaches the others.
// ---------------------------------------------------------------------------

#if (STANDARD_VECTOR_SIZE > 2)
struct PairsBind {
	int32_t count = 0;
};

void DeletePairsBind(void *ptr) {
	delete static_cast<PairsBind *>(ptr);
}

void PairsBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle value = nullptr;
	if (duckdb_v2_table_function_bind_get_arg_value(info, 0, &value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int32_t count = 0;
	auto rc = duckdb_v2_value_get_int(value, &count, err);
	duckdb_v2_value_destroy(&value);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	auto integer = MakeTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, err);
	if (!integer) {
		return;
	}
	rc = duckdb_v2_table_function_bind_add_result_column(info, TableIdent("a"), integer, err);
	duckdb_v2_logical_type_destroy(&integer);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto varchar = MakeTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, err);
	if (!varchar) {
		return;
	}
	rc = duckdb_v2_table_function_bind_add_result_column(info, TableIdent("b"), varchar, err);
	duckdb_v2_logical_type_destroy(&varchar);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	duckdb_v2_opaque bind_data = {new PairsBind {count}, DeletePairsBind, nullptr};
	duckdb_v2_table_function_bind_set_bind_data(info, &bind_data, err);
}

void PairsExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                 duckdb_v2_error_info_handle *err) {
	void *bind_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	if (duckdb_v2_table_function_exec_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &bind = *static_cast<PairsBind *>(bind_ptr);

	// Everything is produced in the first batch; the next call ends the scan.
	auto produced = bind.count;
	bind.count = 0;

	duckdb_v2_vector_handle a = nullptr;
	duckdb_v2_vector_handle b = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_data_chunk_get_vector(chunk, 0, &a, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(chunk, 1, &b, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(a, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto *values = static_cast<int32_t *>(raw);
	for (int32_t i = 0; i < produced; i++) {
		values[i] = i;
		char text[16] = {};
		auto len = std::snprintf(text, sizeof(text), "row%d", i);
		if (V2VectorAssignString(b, static_cast<idx_t>(i), text, static_cast<idx_t>(len), err) !=
		    DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	// Only the first vector is sized; the engine propagates the count to "b".
	duckdb_v2_vector_set_size(a, static_cast<idx_t>(produced), err);
}
#endif

// ---------------------------------------------------------------------------
// my_args(a, b := 7, ...): latches the argument slots the bind callback sees.
// ---------------------------------------------------------------------------

struct TableArgProbe {
	idx_t arg_count = 0;
	int64_t values[8] = {};
	DUCKDB_V2_LOGICAL_TYPE_ID types[8] = {};
	DUCKDB_V2_ERROR oob_type_rc = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR oob_value_rc = DUCKDB_V2_ERROR_NONE;
	bool oob_type_cleared = false;
	bool oob_value_cleared = false;
};
TableArgProbe table_arg_probe;

void ArgsBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                duckdb_v2_error_info_handle *err) {
	table_arg_probe = TableArgProbe {};
	if (duckdb_v2_table_function_bind_get_arg_count(info, &table_arg_probe.arg_count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < table_arg_probe.arg_count && i < 8; i++) {
		duckdb_v2_logical_type_handle type = nullptr;
		if (duckdb_v2_table_function_bind_get_arg_type(info, i, &type, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		auto rc = duckdb_v2_logical_type_get_id(type, &table_arg_probe.types[i], err);
		duckdb_v2_logical_type_destroy(&type);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		duckdb_v2_value_handle value = nullptr;
		if (duckdb_v2_table_function_bind_get_arg_value(info, i, &value, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		rc = duckdb_v2_value_get_bigint(value, &table_arg_probe.values[i], err);
		duckdb_v2_value_destroy(&value);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}

	// An index past the last argument is an input error, and leaves the out-parameter cleared.
	duckdb_v2_logical_type_handle oob_type = nullptr;
	duckdb_v2_value_handle oob_value = nullptr;
	table_arg_probe.oob_type_rc = duckdb_v2_table_function_bind_get_arg_type(info, 99, &oob_type, nullptr);
	table_arg_probe.oob_value_rc = duckdb_v2_table_function_bind_get_arg_value(info, 99, &oob_value, nullptr);
	table_arg_probe.oob_type_cleared = oob_type == nullptr;
	table_arg_probe.oob_value_cleared = oob_value == nullptr;

	auto bigint = MakeTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, err);
	if (!bigint) {
		return;
	}
	auto rc = duckdb_v2_table_function_bind_add_result_column(info, TableIdent("n"), bigint, err);
	duckdb_v2_logical_type_destroy(&bigint);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_opaque bind_data = {new RangeBind {static_cast<int64_t>(table_arg_probe.arg_count)}, DeleteRangeBind,
	                              nullptr};
	duckdb_v2_table_function_bind_set_bind_data(info, &bind_data, err);
}

// Emits a single row carrying the argument count, then ends the scan.
void ArgsExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	void *bind_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	if (duckdb_v2_table_function_exec_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &bind = *static_cast<RangeBind *>(bind_ptr);

	duckdb_v2_vector_handle vec = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(vec, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	idx_t produced = 0;
	if (bind.count >= 0) {
		static_cast<int64_t *>(raw)[0] = bind.count;
		produced = 1;
		bind.count = -1;
	}
	duckdb_v2_vector_set_size(vec, produced, err);
}

// ---------------------------------------------------------------------------
// State plumbing: a global counter and a per-thread local state, both read back
// in exec.
// ---------------------------------------------------------------------------

struct StateProbe {
	idx_t init_global_calls = 0;
	idx_t init_local_calls = 0;
	bool local_saw_global = false;
	bool exec_saw_global = false;
	bool exec_saw_local = false;
	bool exec_saw_user_data = false;
};
StateProbe state_probe;

struct GlobalState {
	int64_t remaining = 0;
};
struct LocalState {
	int64_t tag = 0;
};

void DeleteGlobalState(void *ptr) {
	delete static_cast<GlobalState *>(ptr);
}
void DeleteLocalState(void *ptr) {
	delete static_cast<LocalState *>(ptr);
}

void StateBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                 duckdb_v2_error_info_handle *err) {
	state_probe = StateProbe {};
	auto bigint = MakeTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, err);
	if (!bigint) {
		return;
	}
	auto rc = duckdb_v2_table_function_bind_add_result_column(info, TableIdent("v"), bigint, err);
	duckdb_v2_logical_type_destroy(&bigint);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_opaque bind_data = {new RangeBind {3}, DeleteRangeBind, nullptr};
	duckdb_v2_table_function_bind_set_bind_data(info, &bind_data, err);
}

void StateInitGlobalCb(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle,
                       duckdb_v2_error_info_handle *err) {
	state_probe.init_global_calls++;
	void *bind_ptr = nullptr;
	if (duckdb_v2_table_function_init_global_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &bind = *static_cast<RangeBind *>(bind_ptr);
	duckdb_v2_opaque state = {new GlobalState {bind.count}, DeleteGlobalState, nullptr};
	if (duckdb_v2_table_function_init_global_set_global_state(info, &state, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_table_function_init_global_set_max_threads(info, 1, err);
}

void StateInitLocalCb(duckdb_v2_table_function_init_local_info_handle info, duckdb_v2_context_handle,
                      duckdb_v2_error_info_handle *err) {
	state_probe.init_local_calls++;
	void *global_ptr = nullptr;
	if (duckdb_v2_table_function_init_local_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	state_probe.local_saw_global = global_ptr != nullptr;
	duckdb_v2_opaque state = {new LocalState {42}, DeleteLocalState, nullptr};
	duckdb_v2_table_function_init_local_set_local_state(info, &state, err);
}

void StateExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                 duckdb_v2_error_info_handle *err) {
	void *global_ptr = nullptr;
	void *local_ptr = nullptr;
	void *user_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	if (duckdb_v2_table_function_exec_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_local_state(info, &local_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_user_data(info, &user_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	state_probe.exec_saw_global = global_ptr != nullptr;
	state_probe.exec_saw_local = local_ptr != nullptr;
	state_probe.exec_saw_user_data = user_ptr != nullptr;

	auto &global = *static_cast<GlobalState *>(global_ptr);
	auto &local = *static_cast<LocalState *>(local_ptr);

	duckdb_v2_vector_handle vec = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(vec, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	idx_t produced = 0;
	if (global.remaining > 0) {
		global.remaining--;
		static_cast<int64_t *>(raw)[0] = local.tag;
		produced = 1;
	}
	duckdb_v2_vector_set_size(vec, produced, err);
}

// ---------------------------------------------------------------------------
// Failure callbacks
// ---------------------------------------------------------------------------

void FailingBindCb(duckdb_v2_table_function_bind_info_handle, duckdb_v2_context_handle,
                   duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_set_text(*err, Convert("bind refused"));
}

void FailingExecCb(duckdb_v2_table_function_exec_info_handle, duckdb_v2_context_handle,
                   duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE);
	duckdb_v2_error_info_set_text(*err, Convert("exec refused"));
}

// A bind callback that declares nothing: registration cannot catch this, the scan must.
void NoColumnsBindCb(duckdb_v2_table_function_bind_info_handle, duckdb_v2_context_handle,
                     duckdb_v2_error_info_handle *) {
}

// A bind callback that tries to declare an ANY column.
void AnyColumnBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                     duckdb_v2_error_info_handle *err) {
	auto any = MakeTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_ANY, err);
	if (!any) {
		return;
	}
	auto rc = duckdb_v2_table_function_bind_add_result_column(info, TableIdent("x"), any, err);
	duckdb_v2_logical_type_destroy(&any);
	(void)rc;
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

struct HookProbe {
	idx_t progress_calls = 0;
	bool progress_saw_global_state = false;
};
HookProbe hook_probe;

// Only the progress test uses this, and that test needs a full-size vector to stay quick.
#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
void ProgressCb(duckdb_v2_table_function_progress_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	hook_probe.progress_calls++;
	void *global_ptr = nullptr;
	if (duckdb_v2_table_function_progress_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	hook_probe.progress_saw_global_state = global_ptr != nullptr;
	duckdb_v2_table_function_progress_set_progress(info, 0.5, err);
}
#endif

} // namespace

// ===========================================================================
// Register on a connection and scan through SQL.
// ===========================================================================

TEST_CASE("V2 table: register on connection and scan", "[capi_v2][table_function]") {
	EnvFixture fx;
	RegisterRange(fx.conn);

	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_range(10)") == 10);
	REQUIRE(QueryI64(fx.conn, "SELECT sum(i) FROM my_range(10)") == 45);
	// A scan spanning several batches, and one producing nothing at all.
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_range(5000)") == 5000);
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM my_range(0)") == 0);

	// The bind callback's column declaration is the function's schema.
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT * FROM my_range(3)", &result) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ColumnCount(result) == 1);
	RequireColumn(result, 0, "i", DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	REQUIRE(DrainRowCount(result) == 3);
	duckdb_v2_result_destroy(&result);
}

// my_pairs writes its whole result in one batch, so it needs a vector that holds more than two rows.
#if (STANDARD_VECTOR_SIZE > 2)
TEST_CASE("V2 table: multiple result columns share the batch row count", "[capi_v2][table_function]") {
	EnvFixture fx;
	auto integer = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	auto function = MakeTable(fx.conn, "my_pairs");
	TableSigParam(SigOf(function), "n", integer);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, PairsBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, PairsExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&integer);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT * FROM my_pairs(4)", &result) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ColumnCount(result) == 2);
	RequireColumn(result, 0, "a", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	RequireColumn(result, 1, "b", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	REQUIRE(DrainRowCount(result) == 4);
	duckdb_v2_result_destroy(&result);

	// Both columns carry all four rows: the count set on "a" reached "b".
	REQUIRE(QueryI64(fx.conn, "SELECT count(b) FROM my_pairs(4)") == 4);
	REQUIRE(QueryI64(fx.conn, "SELECT sum(a) FROM my_pairs(4)") == 6);

	duckdb_v2_result_handle text = nullptr;
	REQUIRE(Query(fx.conn, "SELECT string_agg(b, ',' ORDER BY a) FROM my_pairs(3)", &text) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(text);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto bytes = static_cast<const duckdb_v2_bytes *>(view.data)[SelAt(view.sel, 0)];
	REQUIRE(Convert(Convert(bytes)) == "row0,row1,row2");
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&text);
}
#endif

// ===========================================================================
// Argument routing: required parameters are positional, defaulted ones named.
// ===========================================================================

TEST_CASE("V2 table: parameter defaults, named arguments and varargs", "[capi_v2][table_function]") {
	EnvFixture fx;
	auto bigint = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	auto seven = MakeInt64Value(fx.conn, 7);

	auto function = MakeTable(fx.conn, "my_args");
	auto sig = SigOf(function);
	TableSigParam(sig, "a", bigint);
	TableSigParam(sig, "b", bigint, seven);
	REQUIRE(duckdb_v2_function_signature_set_varargs(sig, bigint, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, ArgsBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, ArgsExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	duckdb_v2_value_destroy(&seven);
	duckdb_v2_logical_type_destroy(&bigint);

	// Only the required parameter: the defaulted one is still present, carrying its default.
	REQUIRE(QueryI64(fx.conn, "SELECT * FROM my_args(1)") == 2);
	REQUIRE(table_arg_probe.arg_count == 2);
	REQUIRE(table_arg_probe.values[0] == 1);
	REQUIRE(table_arg_probe.values[1] == 7);
	REQUIRE(table_arg_probe.types[0] == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	// An out-of-range index fails and leaves the out-parameter cleared.
	REQUIRE(table_arg_probe.oob_type_rc == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(table_arg_probe.oob_value_rc == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(table_arg_probe.oob_type_cleared);
	REQUIRE(table_arg_probe.oob_value_cleared);

	// A defaulted parameter is passed by name.
	REQUIRE(QueryI64(fx.conn, "SELECT * FROM my_args(1, b => 5)") == 2);
	REQUIRE(table_arg_probe.values[0] == 1);
	REQUIRE(table_arg_probe.values[1] == 5);

	// The variadic tail follows the fixed slots, in call order.
	REQUIRE(QueryI64(fx.conn, "SELECT * FROM my_args(1, 30, 40)") == 4);
	REQUIRE(table_arg_probe.arg_count == 4);
	REQUIRE(table_arg_probe.values[0] == 1);
	REQUIRE(table_arg_probe.values[1] == 7);
	REQUIRE(table_arg_probe.values[2] == 30);
	REQUIRE(table_arg_probe.values[3] == 40);
}

// ===========================================================================
// Global and local state.
// ===========================================================================

TEST_CASE("V2 table: user data, global state and local state reach exec", "[capi_v2][table_function]") {
	EnvFixture fx;

	auto function = MakeTable(fx.conn, "my_state");
	int payload = 99;
	duckdb_v2_opaque user_data = {&payload, nullptr, nullptr};
	REQUIRE(duckdb_v2_table_function_set_user_data(function, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, StateBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_global_callback(function, StateInitGlobalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_local_callback(function, StateInitLocalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, StateExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);

	// The bind data seeded the global counter with 3 rows, each carrying the local state's tag.
	REQUIRE(QueryI64(fx.conn, "SELECT sum(v) FROM my_state()") == 126);
	REQUIRE(state_probe.init_global_calls == 1);
	REQUIRE(state_probe.init_local_calls >= 1);
	REQUIRE(state_probe.local_saw_global);
	REQUIRE(state_probe.exec_saw_global);
	REQUIRE(state_probe.exec_saw_local);
	REQUIRE(state_probe.exec_saw_user_data);
}

// ===========================================================================
// Errors reported from the callbacks.
// ===========================================================================

TEST_CASE("V2 table: bind and exec errors propagate to the result", "[capi_v2][table_function]") {
	EnvFixture fx;

	auto failing_bind = MakeTable(fx.conn, "bad_bind");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(failing_bind, FailingBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(failing_bind, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(failing_bind, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&failing_bind);

	auto failing_exec = MakeTable(fx.conn, "bad_exec");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(failing_exec, StateBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(failing_exec, FailingExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(failing_exec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&failing_exec);

	auto no_columns = MakeTable(fx.conn, "no_columns");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(no_columns, NoColumnsBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(no_columns, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(no_columns, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&no_columns);

	auto any_column = MakeTable(fx.conn, "any_column");
	REQUIRE(duckdb_v2_table_function_set_bind_callback(any_column, AnyColumnBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(any_column, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(any_column, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&any_column);

	REQUIRE(QueryError(fx.conn, "SELECT * FROM bad_bind()").find("bind refused") != std::string::npos);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM bad_exec()").find("exec refused") != std::string::npos);
	REQUIRE(QueryError(fx.conn, "SELECT * FROM no_columns()").find("result columns") != std::string::npos);
	// The ANY column is rejected where it is declared, and the bind callback ignores the failure,
	// so the scan fails for having declared nothing.
	REQUIRE_FALSE(QueryError(fx.conn, "SELECT * FROM any_column()").empty());
}

// ===========================================================================
// Registration refusals.
// ===========================================================================

TEST_CASE("V2 table: registration refusals", "[capi_v2][table_function]") {
	EnvFixture fx;
	auto bigint = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

	// No name.
	{
		duckdb_v2_table_function_handle function = nullptr;
		REQUIRE(duckdb_v2_table_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, StateBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_exec_callback(function, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
	}

	// No bind callback: a table function has no other way to declare its columns.
	{
		auto function = MakeTable(fx.conn, "no_bind");
		REQUIRE(duckdb_v2_table_function_set_exec_callback(function, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
	}

	// No exec callback.
	{
		auto function = MakeTable(fx.conn, "no_exec");
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, StateBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
	}

	// A return type on the signature: the columns come from bind instead.
	{
		auto function = MakeTable(fx.conn, "has_return_type");
		REQUIRE(duckdb_v2_function_signature_set_return_type(SigOf(function), bigint, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, StateBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_exec_callback(function, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
	}

	// A parameter without a default following one with a default.
	{
		auto function = MakeTable(fx.conn, "bad_defaults");
		auto sig = SigOf(function);
		auto seven = MakeInt64Value(fx.conn, 7);
		TableSigParam(sig, "a", bigint, seven);
		TableSigParam(sig, "b", bigint);
		duckdb_v2_value_destroy(&seven);
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, StateBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_exec_callback(function, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
	}

	duckdb_v2_logical_type_destroy(&bigint);
}

// ===========================================================================
// Progress.
// ===========================================================================

#if (STANDARD_VECTOR_SIZE == DEFAULT_STANDARD_VECTOR_SIZE)
TEST_CASE("V2 table: progress callback reports scan progress", "[capi_v2][table_function]") {
	EnvFixture fx;
	hook_probe = HookProbe {};

	// Single-threaded so all execution happens in our steps, and the progress
	// bar enabled so the engine polls the scan for progress at all.
	for (const char *setup_sql :
	     {"SET threads=1", "SET enable_progress_bar=true", "SET enable_progress_bar_print=false"}) {
		ExecSQL(fx.conn, setup_sql);
	}

	auto bigint = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	auto function = MakeTable(fx.conn, "my_progress");
	TableSigParam(SigOf(function), "n", bigint);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, RangeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_global_callback(function, RangeInitGlobalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_progress_callback(function, ProgressCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&bigint);

	duckdb_v2_result_handle r = nullptr;
	REQUIRE(Query(fx.conn, "SELECT * FROM my_progress(1000000)", &r, nullptr) == DUCKDB_V2_ERROR_NONE);

	// The round count is timing-dependent, so the loop latches instead of asserting.
	auto step_rc = DUCKDB_V2_ERROR_NONE;
	idx_t rows = 0;
	while (true) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
		step_rc = duckdb_v2_result_step(r, &chunk, &status, nullptr);
		if (step_rc != DUCKDB_V2_ERROR_NONE) {
			break;
		}
		if (chunk) {
			idx_t size = 0;
			duckdb_v2_data_chunk_get_size(chunk, &size, nullptr);
			rows += size;
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED) {
			break;
		}
	}
	REQUIRE(step_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(rows == 1000000);
	REQUIRE(hook_probe.progress_calls >= 1);
	REQUIRE(hook_probe.progress_saw_global_state);

	duckdb_v2_result_destroy(&r);
}
#endif

// ===========================================================================
// Null argument handling and destroy null-safety.
// ===========================================================================

TEST_CASE("V2 table: null arguments and destroy null-safety", "[capi_v2][table_function]") {
	EnvFixture fx;

	duckdb_v2_table_function_handle function = nullptr;
	REQUIRE(duckdb_v2_table_function_create_with_connection(nullptr, &function, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_create_with_connection(fx.conn, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_create_with_extension(nullptr, &function, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	auto str = Convert("x");
	REQUIRE(duckdb_v2_table_function_set_name(nullptr, &str, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(nullptr, StateBindCb, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(nullptr, RangeExecCb, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_get_signature(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_register(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	void *data = nullptr;
	REQUIRE(duckdb_v2_table_function_bind_get_user_data(nullptr, &data, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_exec_get_output_chunk(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_progress_set_progress(nullptr, 0.5, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// A thread count of zero would starve the scan.
	REQUIRE(duckdb_v2_table_function_init_global_set_max_threads(nullptr, 0, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Destroy is null-safe and clears the slot.
	REQUIRE(duckdb_v2_table_function_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_handle already_null = nullptr;
	REQUIRE(duckdb_v2_table_function_destroy(&already_null) == DUCKDB_V2_ERROR_NONE);
	auto live = MakeTable(fx.conn, "unused");
	REQUIRE(duckdb_v2_table_function_destroy(&live) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(live == nullptr);
}

// ---------------------------------------------------------------------------
// proj_probe(): three BIGINT columns x, y, z, three rows, cell = declared column * 100 + row. The exec callback
// fills whatever the scan asks for through the column mapping, so it serves with and without projection pushdown.
// ---------------------------------------------------------------------------

namespace {

struct ProjGlobal {
	idx_t position = 0;
};

constexpr idx_t PROJ_ROWS = 3;

void DeleteProjGlobal(void *ptr) {
	delete static_cast<ProjGlobal *>(ptr);
}

// The column mapping each init callback observed for the last scan.
std::vector<idx_t> proj_global_columns;
std::vector<idx_t> proj_local_columns;

void ProjBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                duckdb_v2_error_info_handle *err) {
	auto bigint = MakeTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, err);
	if (!bigint) {
		return;
	}
	for (auto name : {"x", "y", "z"}) {
		if (duckdb_v2_table_function_bind_add_result_column(info, TableIdent(name), bigint, err) !=
		    DUCKDB_V2_ERROR_NONE) {
			break;
		}
	}
	duckdb_v2_logical_type_destroy(&bigint);
}

void ProjInitGlobalCb(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle,
                      duckdb_v2_error_info_handle *err) {
	proj_global_columns.clear();
	idx_t count = 0;
	if (duckdb_v2_table_function_init_global_get_column_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		idx_t column = 0;
		if (duckdb_v2_table_function_init_global_get_column_index(info, i, &column, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		proj_global_columns.push_back(column);
	}
	duckdb_v2_opaque state = {new ProjGlobal {}, DeleteProjGlobal, nullptr};
	duckdb_v2_table_function_init_global_set_global_state(info, &state, err);
}

void ProjInitLocalCb(duckdb_v2_table_function_init_local_info_handle info, duckdb_v2_context_handle,
                     duckdb_v2_error_info_handle *err) {
	proj_local_columns.clear();
	idx_t count = 0;
	if (duckdb_v2_table_function_init_local_get_column_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		idx_t column = 0;
		if (duckdb_v2_table_function_init_local_get_column_index(info, i, &column, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		proj_local_columns.push_back(column);
	}
}

void ProjExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	void *global_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	idx_t count = 0;
	if (duckdb_v2_table_function_exec_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_column_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &global = *static_cast<ProjGlobal *>(global_ptr);
	// Batches are capped by the vector size so the probe also serves builds with a tiny STANDARD_VECTOR_SIZE.
	idx_t rows = PROJ_ROWS - global.position;
	if (rows > STANDARD_VECTOR_SIZE) {
		rows = STANDARD_VECTOR_SIZE;
	}

	for (idx_t i = 0; i < count; i++) {
		idx_t column = 0;
		duckdb_v2_vector_handle vec = nullptr;
		void *raw = nullptr;
		if (duckdb_v2_table_function_exec_get_column_index(info, i, &column, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_data_chunk_get_vector(chunk, i, &vec, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_get_data_mutable(vec, &raw, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		for (idx_t row = 0; row < rows; row++) {
			static_cast<int64_t *>(raw)[row] = static_cast<int64_t>(column * 100 + global.position + row);
		}
		if (i == 0) {
			duckdb_v2_vector_set_size(vec, rows, err);
		}
	}
	global.position += rows;
}

void RegisterProj(duckdb_v2_connection_handle conn, const char *name, bool projection_pushdown) {
	auto function = MakeTable(conn, name);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, ProjBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_global_callback(function, ProjInitGlobalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_local_callback(function, ProjInitLocalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, ProjExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_projection_pushdown(function, projection_pushdown, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
}

// Runs a query and collects its BIGINT columns row-major.
std::vector<int64_t> QueryCells(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql, &result) == DUCKDB_V2_ERROR_NONE);
	std::vector<int64_t> cells;
	while (auto chunk = StepChunk(result)) {
		idx_t columns = 0;
		idx_t rows = 0;
		duckdb_v2_data_chunk_get_vector_count(chunk, &columns, nullptr);
		duckdb_v2_data_chunk_get_size(chunk, &rows, nullptr);
		for (idx_t row = 0; row < rows; row++) {
			for (idx_t col = 0; col < columns; col++) {
				duckdb_v2_vector_handle vec = nullptr;
				duckdb_v2_data_chunk_get_vector(chunk, col, &vec, nullptr);
				duckdb_v2_vector_view view {};
				duckdb_v2_vector_get_view(vec, &view, nullptr);
				cells.push_back(static_cast<const int64_t *>(view.data)[SelAt(view.sel, row)]);
			}
		}
		duckdb_v2_data_chunk_destroy(&chunk);
	}
	duckdb_v2_result_destroy(&result);
	return cells;
}

// ---------------------------------------------------------------------------
// claim_probe(n): column i BIGINT with 0..n-1. Its pushdown callback claims every "i < constant" predicate by
// recording the bound in the bind data; the scan then deliberately produces two rows past the bound, which only
// reach the result if the engine really stopped applying the claimed predicate.
// ---------------------------------------------------------------------------

struct ClaimBind {
	int64_t count = 0;
	int64_t bound = -1;
};
struct ClaimGlobal {
	int64_t position = 0;
};

void DeleteClaimBind(void *ptr) {
	delete static_cast<ClaimBind *>(ptr);
}
void DeleteClaimGlobal(void *ptr) {
	delete static_cast<ClaimGlobal *>(ptr);
}

// Whether the pushdown callback saw the user data it was registered with.
bool claim_saw_user_data = false;
int claim_user_data_marker = 0;

void ClaimBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle value = nullptr;
	if (duckdb_v2_table_function_bind_get_arg_value(info, 0, &value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int64_t count = 0;
	auto rc = duckdb_v2_value_get_bigint(value, &count, err);
	duckdb_v2_value_destroy(&value);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto bigint = MakeTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, err);
	if (!bigint) {
		return;
	}
	rc = duckdb_v2_table_function_bind_add_result_column(info, TableIdent("i"), bigint, err);
	duckdb_v2_logical_type_destroy(&bigint);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_opaque bind_data = {new ClaimBind {count, -1}, DeleteClaimBind, nullptr};
	duckdb_v2_table_function_bind_set_bind_data(info, &bind_data, err);
}

void ClaimPushdownCb(duckdb_v2_table_function_filter_pushdown_info_handle info, duckdb_v2_context_handle,
                     duckdb_v2_error_info_handle *err) {
	void *user_ptr = nullptr;
	void *bind_ptr = nullptr;
	idx_t count = 0;
	if (duckdb_v2_table_function_filter_pushdown_get_user_data(info, &user_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_filter_pushdown_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_filter_pushdown_get_filter_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	claim_saw_user_data = user_ptr == &claim_user_data_marker;
	auto &bind = *static_cast<ClaimBind *>(bind_ptr);

	for (idx_t i = 0; i < count; i++) {
		duckdb_v2_expression_handle filter = nullptr;
		DUCKDB_V2_EXPRESSION_TYPE type = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
		if (duckdb_v2_table_function_filter_pushdown_get_filter(info, i, &filter, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_expression_get_type(filter, &type, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		if (type != DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHAN) {
			continue;
		}
		duckdb_v2_expression_handle left = nullptr;
		duckdb_v2_expression_handle right = nullptr;
		DUCKDB_V2_EXPRESSION_TYPE left_type = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
		DUCKDB_V2_EXPRESSION_TYPE right_type = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
		if (duckdb_v2_expression_get_child(filter, 0, &left, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_expression_get_child(filter, 1, &right, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_expression_get_type(left, &left_type, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_expression_get_type(right, &right_type, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		if (left_type != DUCKDB_V2_EXPRESSION_TYPE_BOUND_COLUMN_REF ||
		    right_type != DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT) {
			continue;
		}
		idx_t column = 0;
		idx_t declared = 0;
		duckdb_v2_value_handle value = nullptr;
		if (duckdb_v2_expression_column_ref_get_index(left, &column, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_table_function_filter_pushdown_get_column_index(info, column, &declared, err) !=
		        DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_expression_constant_get_value(right, &value, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		int64_t bound = 0;
		auto rc = duckdb_v2_value_get_bigint(value, &bound, err);
		duckdb_v2_value_destroy(&value);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		if (declared != 0) {
			continue;
		}
		bind.bound = bound;
		if (duckdb_v2_table_function_filter_pushdown_accept(info, i, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
}

void ClaimInitGlobalCb(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle,
                       duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque state = {new ClaimGlobal {0}, DeleteClaimGlobal, nullptr};
	duckdb_v2_table_function_init_global_set_global_state(info, &state, err);
}

void ClaimExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
                 duckdb_v2_error_info_handle *err) {
	void *bind_ptr = nullptr;
	void *global_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	if (duckdb_v2_table_function_exec_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &bind = *static_cast<ClaimBind *>(bind_ptr);
	auto &global = *static_cast<ClaimGlobal *>(global_ptr);

	duckdb_v2_vector_handle vec = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(vec, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	// Two rows past a claimed bound: they surface only if the engine dropped the predicate.
	auto limit = bind.bound < 0 ? bind.count : bind.bound + 2;
	if (limit > bind.count) {
		limit = bind.count;
	}
	auto remaining = limit - global.position;
	auto produced =
	    remaining < static_cast<int64_t>(STANDARD_VECTOR_SIZE) ? remaining : static_cast<int64_t>(STANDARD_VECTOR_SIZE);
	if (produced < 0) {
		produced = 0;
	}
	auto *out = static_cast<int64_t *>(raw);
	for (int64_t i = 0; i < produced; i++) {
		out[i] = global.position + i;
	}
	global.position += produced;
	duckdb_v2_vector_set_size(vec, static_cast<idx_t>(produced), err);
}

void FailingPushdownCb(duckdb_v2_table_function_filter_pushdown_info_handle, duckdb_v2_context_handle,
                       duckdb_v2_error_info_handle *err) {
	SetErrorInfo(err, DUCKDB_V2_ERROR_INPUT_INVALID, "pushdown refused");
}

void RegisterClaim(duckdb_v2_connection_handle conn, const char *name,
                   duckdb_v2_table_function_filter_pushdown_callback_fn pushdown) {
	auto bigint = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	auto function = MakeTable(conn, name);
	TableSigParam(SigOf(function), "n", bigint);
	duckdb_v2_opaque user_data = {&claim_user_data_marker, nullptr, nullptr};
	REQUIRE(duckdb_v2_table_function_set_user_data(function, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, ClaimBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_global_callback(function, ClaimInitGlobalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, ClaimExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_filter_pushdown_callback(function, pushdown, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&bigint);
}

} // namespace

TEST_CASE("V2 table: projection pushdown narrows the output chunk", "[capi_v2][table_function]") {
	EnvFixture fx;
	RegisterProj(fx.conn, "proj_probe", true);
	RegisterProj(fx.conn, "proj_plain", false);

	using Cells = std::vector<int64_t>;
	using Columns = std::vector<idx_t>;

	// Two of three columns, in query order: the chunk holds exactly those, and the values prove the mapping.
	REQUIRE(QueryCells(fx.conn, "SELECT z, x FROM proj_probe()") == Cells {200, 0, 201, 1, 202, 2});
	REQUIRE(proj_global_columns == Columns {2, 0});
	REQUIRE(proj_local_columns == Columns {2, 0});

	// A query that needs no column at all still scans one.
	REQUIRE(QueryCells(fx.conn, "SELECT count(*) FROM proj_probe()") == Cells {3});
	REQUIRE(proj_global_columns.size() == 1);

	// Without projection pushdown the chunk always holds every declared column, in declaration order.
	REQUIRE(QueryCells(fx.conn, "SELECT z, x FROM proj_plain()") == Cells {200, 0, 201, 1, 202, 2});
	REQUIRE(proj_global_columns == Columns {0, 1, 2});
	REQUIRE(proj_local_columns == Columns {0, 1, 2});
}

TEST_CASE("V2 table: claimed filters are no longer applied by the engine", "[capi_v2][table_function]") {
	EnvFixture fx;
	RegisterClaim(fx.conn, "claim_probe", ClaimPushdownCb);

	// Nothing to claim: every row comes through.
	claim_saw_user_data = false;
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM claim_probe(100) WHERE i % 2 = 0") == 50);
	REQUIRE(claim_saw_user_data);

	// The claimed bound plus the two rows the scan sneaks past it: the engine no longer filters them out.
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM claim_probe(100) WHERE i < 10") == 12);
	// The unclaimed half of the conjunction is still applied above the scan.
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM claim_probe(100) WHERE i < 10 AND i % 2 = 0") == 6);
	// A comparison the callback does not recognize stays with the engine.
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM claim_probe(100) WHERE i > 90") == 9);
}

TEST_CASE("V2 table: filter pushdown errors fail the query", "[capi_v2][table_function]") {
	EnvFixture fx;
	RegisterClaim(fx.conn, "claim_fail", FailingPushdownCb);

	// Without a predicate there is nothing to offer, so the callback never runs.
	REQUIRE(QueryI64(fx.conn, "SELECT count(*) FROM claim_fail(5)") == 5);
	auto message = QueryError(fx.conn, "SELECT count(*) FROM claim_fail(5) WHERE i < 3");
	REQUIRE(message.find("pushdown refused") != std::string::npos);
}

TEST_CASE("V2 table: pushdown null arguments", "[capi_v2][table_function]") {
	idx_t count = 0;
	void *data = nullptr;
	REQUIRE(duckdb_v2_table_function_set_projection_pushdown(nullptr, true, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_set_filter_pushdown_callback(nullptr, ClaimPushdownCb, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_init_global_get_column_count(nullptr, &count, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_init_global_get_column_index(nullptr, 0, &count, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_init_local_get_column_count(nullptr, &count, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_init_local_get_column_index(nullptr, 0, &count, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_exec_get_column_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_exec_get_column_index(nullptr, 0, &count, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_filter_pushdown_get_user_data(nullptr, &data, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_filter_pushdown_get_bind_data(nullptr, &data, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

} // namespace test_capi_v2
