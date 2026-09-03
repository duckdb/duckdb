#include "test_capi_v2.hpp"

#include <atomic>
#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// V2 copy function tests: build a function on a connection, register it, and
// drive it through COPY ... TO.
//
// Callbacks avoid Catch assertions: a REQUIRE would throw through the C
// callback boundary into the engine. On failure they populate the provided
// error slot and return; the failure then surfaces as a query error that the
// test asserts on. Cross-callback observations are latched into file-scope
// statics and asserted after the query. The batch callback may run on several
// threads at once, so the latches it writes are atomic.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

// Create a copy function on the connection with the given name.
duckdb_v2_copy_function_handle MakeCopy(duckdb_v2_connection_handle conn, const char *name) {
	duckdb_v2_copy_function_handle function = nullptr;
	REQUIRE(duckdb_v2_copy_function_create_with_connection(conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto str = Convert(name);
	REQUIRE(duckdb_v2_copy_function_set_name(function, &str, nullptr) == DUCKDB_V2_ERROR_NONE);
	return function;
}

// COPY the rows of `source` to `path` with the given format. The target never exists beforehand, so the engine
// writes it in place; USE_TMP_FILE is pinned anyway so a leftover file from an earlier run cannot change that.
std::string CopyStatement(const std::string &source, const std::string &path, const char *format) {
	return "COPY (" + source + ") TO '" + path + "' (FORMAT " + format + ", USE_TMP_FILE FALSE)";
}

// Runs a COPY statement and returns the row count it reports.
int64_t RunCopy(duckdb_v2_connection_handle conn, const std::string &sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql.c_str(), &result) == DUCKDB_V2_ERROR_NONE);
	auto rows = DrainChangedRows(result);
	duckdb_v2_result_destroy(&result);
	return rows;
}

// Runs a statement expected to fail, returning the code it fails with: either straight from the execute call, or
// while stepping the result when the failure only surfaces during execution.
DUCKDB_V2_ERROR RunFailingCopy(duckdb_v2_connection_handle conn, const std::string &sql) {
	duckdb_v2_result_handle result = nullptr;
	auto rc = Query(conn, sql.c_str(), &result);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_result_destroy(&result);
		return rc;
	}
	auto status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	for (int i = 0; i < 100000 && rc == DUCKDB_V2_ERROR_NONE && status != DUCKDB_V2_RESULT_STEP_STATUS_FINISHED; i++) {
		duckdb_v2_data_chunk_handle chunk = nullptr;
		rc = duckdb_v2_result_step(result, &chunk, &status, nullptr);
		if (chunk) {
			duckdb_v2_data_chunk_destroy(&chunk);
		}
		if (rc == DUCKDB_V2_ERROR_NONE && status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			rc = duckdb_v2_result_wait(result, nullptr);
		}
	}
	duckdb_v2_result_destroy(&result);
	return rc;
}

// ---------------------------------------------------------------------------
// probe_copy: every callback latches what it saw. The user, bind, init and
// batch data are file-scope markers with counting destructors.
// ---------------------------------------------------------------------------

int copy_user_marker = 0;
int copy_bind_marker = 0;
int copy_init_marker = 0;
int copy_batch_marker = 0;

enum Phase { BIND = 0, BATCH_SIZE, INIT, BATCH, FLUSH, FINALIZE, PHASE_COUNT };

struct CopyProbe {
	// What each phase's data accessors returned.
	std::atomic<void *> user_data[PHASE_COUNT];
	std::atomic<void *> bind_data[PHASE_COUNT];
	std::atomic<void *> init_data[PHASE_COUNT];
	std::atomic<void *> batch_data_in_flush;
	std::atomic<idx_t> calls[PHASE_COUNT];
	// Taking the batch a second time is refused.
	std::atomic<DUCKDB_V2_ERROR> second_take_rc;
	// Rows seen by the batch callback, summed over every batch.
	std::atomic<idx_t> rows;
	std::atomic<int> user_data_destroys;
	std::atomic<int> bind_data_destroys;
	std::atomic<int> init_data_destroys;
	std::atomic<int> batch_data_destroys;

	// Written by the bind and init callbacks only, which run once per statement.
	idx_t column_count = 0;
	std::string column_names[2];
	DUCKDB_V2_LOGICAL_TYPE_ID column_types[2] = {DUCKDB_V2_LOGICAL_TYPE_ID_INVALID, DUCKDB_V2_LOGICAL_TYPE_ID_INVALID};
	// Out-of-range probes, latched with their own (null) error slot.
	DUCKDB_V2_ERROR oob_type_rc = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR oob_name_rc = DUCKDB_V2_ERROR_NONE;
	std::string file_path;

	void Reset() {
		for (int phase = 0; phase < PHASE_COUNT; phase++) {
			user_data[phase] = nullptr;
			bind_data[phase] = nullptr;
			init_data[phase] = nullptr;
			calls[phase] = 0;
		}
		batch_data_in_flush = nullptr;
		second_take_rc = DUCKDB_V2_ERROR_NONE;
		rows = 0;
		user_data_destroys = 0;
		bind_data_destroys = 0;
		init_data_destroys = 0;
		batch_data_destroys = 0;
		column_count = 0;
		column_names[0].clear();
		column_names[1].clear();
		column_types[0] = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		column_types[1] = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
		oob_type_rc = DUCKDB_V2_ERROR_NONE;
		oob_name_rc = DUCKDB_V2_ERROR_NONE;
		file_path.clear();
	}
} copy_probe;

void DestroyUserData(void *) {
	copy_probe.user_data_destroys++;
}
void DestroyBindData(void *) {
	copy_probe.bind_data_destroys++;
}
void DestroyInitData(void *) {
	copy_probe.init_data_destroys++;
}
void DestroyBatchData(void *) {
	copy_probe.batch_data_destroys++;
}

void ProbeBind(duckdb_v2_copy_function_bind_info_handle info, duckdb_v2_context_handle,
               duckdb_v2_error_info_handle *err) {
	copy_probe.calls[BIND]++;
	void *user_data = nullptr;
	if (duckdb_v2_copy_function_bind_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	copy_probe.user_data[BIND] = user_data;

	if (duckdb_v2_copy_function_bind_get_column_count(info, &copy_probe.column_count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	for (idx_t i = 0; i < copy_probe.column_count && i < 2; i++) {
		duckdb_v2_identifier_t name = {nullptr, 0};
		if (duckdb_v2_copy_function_bind_get_column_name(info, i, &name, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		copy_probe.column_names[i] = Convert(name);
		duckdb_v2_logical_type_handle type = nullptr;
		if (duckdb_v2_copy_function_bind_get_column_type(info, i, &type, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		auto rc = duckdb_v2_logical_type_get_id(type, &copy_probe.column_types[i], err);
		duckdb_v2_logical_type_destroy(&type);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	// An index past the last column is an input error.
	duckdb_v2_logical_type_handle oob_type = nullptr;
	duckdb_v2_identifier_t oob_name = {nullptr, 0};
	copy_probe.oob_type_rc =
	    duckdb_v2_copy_function_bind_get_column_type(info, copy_probe.column_count, &oob_type, nullptr);
	copy_probe.oob_name_rc =
	    duckdb_v2_copy_function_bind_get_column_name(info, copy_probe.column_count, &oob_name, nullptr);

	duckdb_v2_opaque bind_data = {&copy_bind_marker, DestroyBindData, nullptr};
	duckdb_v2_copy_function_bind_set_bind_data(info, &bind_data, err);
}

// Asks for batches larger than any input in these tests, so a whole copy arrives as one batch per thread.
void ProbeBatchSize(duckdb_v2_copy_function_batch_size_info_handle info, duckdb_v2_context_handle,
                    duckdb_v2_error_info_handle *err) {
	copy_probe.calls[BATCH_SIZE]++;
	void *user_data = nullptr;
	void *bind_data = nullptr;
	if (duckdb_v2_copy_function_batch_size_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_batch_size_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	copy_probe.user_data[BATCH_SIZE] = user_data;
	copy_probe.bind_data[BATCH_SIZE] = bind_data;
	duckdb_v2_copy_function_batch_size_set_target(info, 100000, err);
}

void ProbeInit(duckdb_v2_copy_function_init_info_handle info, duckdb_v2_context_handle,
               duckdb_v2_error_info_handle *err) {
	copy_probe.calls[INIT]++;
	void *user_data = nullptr;
	void *bind_data = nullptr;
	duckdb_v2_str path = {nullptr, 0};
	if (duckdb_v2_copy_function_init_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_init_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_init_get_file_path(info, &path, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	copy_probe.user_data[INIT] = user_data;
	copy_probe.bind_data[INIT] = bind_data;
	copy_probe.file_path = Convert(path);

	duckdb_v2_opaque init_data = {&copy_init_marker, DestroyInitData, nullptr};
	duckdb_v2_copy_function_init_set_init_data(info, &init_data, err);
}

void ProbeBatch(duckdb_v2_copy_function_batch_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	copy_probe.calls[BATCH]++;
	void *user_data = nullptr;
	void *bind_data = nullptr;
	void *init_data = nullptr;
	duckdb_v2_column_data_collection_handle input = nullptr;
	if (duckdb_v2_copy_function_batch_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_batch_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_batch_get_init_data(info, &init_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_batch_take_input(info, &input, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	copy_probe.user_data[BATCH] = user_data;
	copy_probe.bind_data[BATCH] = bind_data;
	copy_probe.init_data[BATCH] = init_data;

	// The batch is ours now: count its rows and release it. A second take has nothing left to hand out.
	idx_t count = 0;
	auto rc = duckdb_v2_column_data_collection_row_count(input, &count, err);
	duckdb_v2_column_data_collection_destroy(&input);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	copy_probe.rows += count;
	duckdb_v2_column_data_collection_handle again = nullptr;
	copy_probe.second_take_rc = duckdb_v2_copy_function_batch_take_input(info, &again, nullptr);

	duckdb_v2_opaque batch_data = {&copy_batch_marker, DestroyBatchData, nullptr};
	duckdb_v2_copy_function_batch_set_batch_data(info, &batch_data, err);
}

void ProbeFlush(duckdb_v2_copy_function_flush_info_handle info, duckdb_v2_context_handle,
                duckdb_v2_error_info_handle *err) {
	copy_probe.calls[FLUSH]++;
	void *user_data = nullptr;
	void *bind_data = nullptr;
	void *init_data = nullptr;
	void *batch_data = nullptr;
	if (duckdb_v2_copy_function_flush_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_flush_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_flush_get_init_data(info, &init_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_flush_get_batch_data(info, &batch_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	copy_probe.user_data[FLUSH] = user_data;
	copy_probe.bind_data[FLUSH] = bind_data;
	copy_probe.init_data[FLUSH] = init_data;
	copy_probe.batch_data_in_flush = batch_data;
}

void ProbeFinalize(duckdb_v2_copy_function_finalize_info_handle info, duckdb_v2_context_handle,
                   duckdb_v2_error_info_handle *err) {
	copy_probe.calls[FINALIZE]++;
	void *user_data = nullptr;
	void *bind_data = nullptr;
	void *init_data = nullptr;
	if (duckdb_v2_copy_function_finalize_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_finalize_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_copy_function_finalize_get_init_data(info, &init_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	copy_probe.user_data[FINALIZE] = user_data;
	copy_probe.bind_data[FINALIZE] = bind_data;
	copy_probe.init_data[FINALIZE] = init_data;
}

// Registers the probe function under the given name, with every callback and user data set, and destroys the
// handle. Without the batch size callback, the batch size is left to the statement.
void RegisterProbeCopy(duckdb_v2_connection_handle conn, const char *name = "probe_copy", bool with_batch_size = true) {
	auto function = MakeCopy(conn, name);
	if (with_batch_size) {
		REQUIRE(duckdb_v2_copy_function_set_batch_size_callback(function, ProbeBatchSize, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
	}
	duckdb_v2_opaque user_data = {&copy_user_marker, DestroyUserData, nullptr};
	REQUIRE(duckdb_v2_copy_function_set_user_data(function, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_bind_callback(function, ProbeBind, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_init_callback(function, ProbeInit, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(function, ProbeBatch, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_flush_callback(function, ProbeFlush, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_finalize_callback(function, ProbeFinalize, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_copy_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_destroy(&function) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(function == nullptr);
}

// Noop callbacks for registration-refusal tests.
void CopyNoopBatch(duckdb_v2_copy_function_batch_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}
void CopyNoopFlush(duckdb_v2_copy_function_flush_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}

// Fail the query through a callback's error slot.
void FailingBind(duckdb_v2_copy_function_bind_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_IO_GENERAL);
	duckdb_v2_error_info_set_text(*err, Convert("copy bind failed on purpose"));
}
void FailingBatch(duckdb_v2_copy_function_batch_info_handle, duckdb_v2_context_handle,
                  duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_IO_GENERAL);
	duckdb_v2_error_info_set_text(*err, Convert("copy batch failed on purpose"));
}
void FailingBatchSize(duckdb_v2_copy_function_batch_size_info_handle, duckdb_v2_context_handle,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_IO_GENERAL);
	duckdb_v2_error_info_set_text(*err, Convert("copy batch size failed on purpose"));
}
// Leaves the target unset, which the engine refuses.
void EmptyBatchSize(duckdb_v2_copy_function_batch_size_info_handle, duckdb_v2_context_handle,
                    duckdb_v2_error_info_handle *) {
}

} // namespace

// ===========================================================================
// Register on a connection and drive through COPY ... TO.
// ===========================================================================

TEST_CASE("V2 copy: register on connection and execute", "[capi_v2][copy_function]") {
	EnvFixture fx;
	RegisterProbeCopy(fx.conn);

	// More rows than one chunk, so the batch callback sees real batches.
	const auto path = duckdb::TestCreatePath("v2_copy_probe.out");
	copy_probe.Reset();
	REQUIRE(RunCopy(fx.conn,
	                CopyStatement("SELECT r AS a, r::VARCHAR AS b FROM range(5000) t(r)", path, "probe_copy")) == 5000);

	// Bind saw the columns of the SELECT; an index past them is refused.
	REQUIRE(copy_probe.calls[BIND].load() == 1);
	REQUIRE(copy_probe.column_count == 2);
	REQUIRE(copy_probe.column_names[0] == "a");
	REQUIRE(copy_probe.column_names[1] == "b");
	REQUIRE(copy_probe.column_types[0] == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	REQUIRE(copy_probe.column_types[1] == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	REQUIRE(copy_probe.oob_type_rc == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(copy_probe.oob_name_rc == DUCKDB_V2_ERROR_INPUT_INVALID);

	// The batch size callback ran once, after bind.
	REQUIRE(copy_probe.calls[BATCH_SIZE].load() == 1);

	// One file: init and finalize ran once, with the COPY target as the path.
	REQUIRE(copy_probe.calls[INIT].load() == 1);
	REQUIRE(copy_probe.file_path == path);
	REQUIRE(copy_probe.calls[FINALIZE].load() == 1);

	// Every row went through a batch, and every batch was flushed.
	REQUIRE(copy_probe.calls[BATCH].load() >= 1);
	REQUIRE(copy_probe.calls[FLUSH].load() == copy_probe.calls[BATCH].load());
	REQUIRE(copy_probe.rows.load() == 5000);
	REQUIRE(copy_probe.second_take_rc.load() == DUCKDB_V2_ERROR_INPUT_INVALID);

	// The data slots reached every phase they are defined for.
	for (int phase = 0; phase < PHASE_COUNT; phase++) {
		REQUIRE(copy_probe.user_data[phase].load() == &copy_user_marker);
	}
	for (int phase = BATCH_SIZE; phase < PHASE_COUNT; phase++) {
		REQUIRE(copy_probe.bind_data[phase].load() == &copy_bind_marker);
	}
	for (int phase = BATCH; phase < PHASE_COUNT; phase++) {
		REQUIRE(copy_probe.init_data[phase].load() == &copy_init_marker);
	}
	REQUIRE(copy_probe.batch_data_in_flush.load() == &copy_batch_marker);

	// Each batch's data was released after its flush, and the file's init data and the statement's bind data with
	// the statement.
	REQUIRE(copy_probe.batch_data_destroys.load() == copy_probe.calls[BATCH].load());
	REQUIRE(copy_probe.init_data_destroys.load() == 1);
	REQUIRE(copy_probe.bind_data_destroys.load() == 1);
	// The user data lives with the registered function.
	REQUIRE(copy_probe.user_data_destroys.load() == 0);
}

// ===========================================================================
// The batch size callback decides where batches are cut when the statement sets no BATCH_SIZE.
// ===========================================================================

TEST_CASE("V2 copy: batch size callback", "[capi_v2][copy_function]") {
	EnvFixture fx;
	// One sink thread, so the batch count follows from the chunk sizes alone.
	ExecSQL(fx.conn, "SET threads = 1");
	RegisterProbeCopy(fx.conn, "chunked_copy", false);
	RegisterProbeCopy(fx.conn, "whole_copy", true);

	// Without a batch size every sunk chunk is a batch: 2048, 2048 and 904 rows.
	const auto path = duckdb::TestCreatePath("v2_copy_batch_size.out");
	copy_probe.Reset();
	REQUIRE(RunCopy(fx.conn, CopyStatement("SELECT r FROM range(5000) t(r)", path, "chunked_copy")) == 5000);
	REQUIRE(copy_probe.calls[BATCH_SIZE].load() == 0);
	REQUIRE(copy_probe.calls[BATCH].load() == 3);
	REQUIRE(copy_probe.rows.load() == 5000);

	// The callback asks for batches larger than the input: everything arrives as one batch.
	copy_probe.Reset();
	REQUIRE(RunCopy(fx.conn, CopyStatement("SELECT r FROM range(5000) t(r)", path, "whole_copy")) == 5000);
	REQUIRE(copy_probe.calls[BATCH_SIZE].load() == 1);
	REQUIRE(copy_probe.calls[BATCH].load() == 1);
	REQUIRE(copy_probe.rows.load() == 5000);

	// The statement's own BATCH_SIZE wins, and the callback is not consulted.
	copy_probe.Reset();
	REQUIRE(RunCopy(fx.conn, "COPY (SELECT r FROM range(5000) t(r)) TO '" + path +
	                             "' (FORMAT whole_copy, USE_TMP_FILE FALSE, BATCH_SIZE 4096)") == 5000);
	REQUIRE(copy_probe.calls[BATCH_SIZE].load() == 0);
	REQUIRE(copy_probe.calls[BATCH].load() == 2);
	REQUIRE(copy_probe.rows.load() == 5000);
}

// ===========================================================================
// Only the batch and flush callbacks are required; the others leave their data slots empty.
// ===========================================================================

TEST_CASE("V2 copy: optional callbacks and empty data slots", "[capi_v2][copy_function]") {
	EnvFixture fx;

	auto function = MakeCopy(fx.conn, "bare_copy");
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(function, ProbeBatch, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_flush_callback(function, ProbeFlush, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_destroy(&function);

	const auto path = duckdb::TestCreatePath("v2_copy_bare.out");
	copy_probe.Reset();
	REQUIRE(RunCopy(fx.conn, CopyStatement("SELECT r FROM range(10) t(r)", path, "bare_copy")) == 10);

	REQUIRE(copy_probe.calls[BIND].load() == 0);
	REQUIRE(copy_probe.calls[INIT].load() == 0);
	REQUIRE(copy_probe.calls[FINALIZE].load() == 0);
	REQUIRE(copy_probe.calls[BATCH].load() >= 1);
	REQUIRE(copy_probe.calls[FLUSH].load() == copy_probe.calls[BATCH].load());
	REQUIRE(copy_probe.rows.load() == 10);

	// Nothing was set, so every slot reads as null.
	REQUIRE(copy_probe.user_data[BATCH].load() == nullptr);
	REQUIRE(copy_probe.bind_data[BATCH].load() == nullptr);
	REQUIRE(copy_probe.init_data[BATCH].load() == nullptr);
	REQUIRE(copy_probe.user_data[FLUSH].load() == nullptr);
	REQUIRE(copy_probe.bind_data[FLUSH].load() == nullptr);
	REQUIRE(copy_probe.init_data[FLUSH].load() == nullptr);
	// The batch data was set by the batch callback, so it did reach the flush.
	REQUIRE(copy_probe.batch_data_in_flush.load() == &copy_batch_marker);
	REQUIRE(copy_probe.batch_data_destroys.load() == copy_probe.calls[BATCH].load());

	// A batch callback that never takes its input leaves the engine to release it.
	auto untaken = MakeCopy(fx.conn, "untaken_copy");
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(untaken, CopyNoopBatch, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_flush_callback(untaken, CopyNoopFlush, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_register(untaken, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_destroy(&untaken);
	REQUIRE(RunCopy(fx.conn, CopyStatement("SELECT r FROM range(10) t(r)", path, "untaken_copy")) == 10);
}

// ===========================================================================
// Errors
// ===========================================================================

// An error set in a callback's slot fails the statement with its code.
TEST_CASE("V2 copy: callback errors propagate to the result", "[capi_v2][copy_function]") {
	EnvFixture fx;

	auto batch_fails = MakeCopy(fx.conn, "copy_batch_fails");
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(batch_fails, FailingBatch, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_flush_callback(batch_fails, CopyNoopFlush, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_register(batch_fails, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_destroy(&batch_fails);

	auto bind_fails = MakeCopy(fx.conn, "copy_bind_fails");
	REQUIRE(duckdb_v2_copy_function_set_bind_callback(bind_fails, FailingBind, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(bind_fails, CopyNoopBatch, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_flush_callback(bind_fails, CopyNoopFlush, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_register(bind_fails, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_destroy(&bind_fails);

	auto batch_size_fails = MakeCopy(fx.conn, "copy_batch_size_fails");
	REQUIRE(duckdb_v2_copy_function_set_batch_size_callback(batch_size_fails, FailingBatchSize, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(batch_size_fails, CopyNoopBatch, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_flush_callback(batch_size_fails, CopyNoopFlush, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_register(batch_size_fails, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_destroy(&batch_size_fails);

	auto batch_size_empty = MakeCopy(fx.conn, "copy_batch_size_empty");
	REQUIRE(duckdb_v2_copy_function_set_batch_size_callback(batch_size_empty, EmptyBatchSize, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(batch_size_empty, CopyNoopBatch, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_flush_callback(batch_size_empty, CopyNoopFlush, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_register(batch_size_empty, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_destroy(&batch_size_empty);

	// The callback's code round-trips through the engine's exception machinery.
	const auto path = duckdb::TestCreatePath("v2_copy_fails.out");
	REQUIRE(RunFailingCopy(fx.conn, CopyStatement("SELECT r FROM range(10) t(r)", path, "copy_batch_fails")) ==
	        DUCKDB_V2_ERROR_IO_GENERAL);
	REQUIRE(RunFailingCopy(fx.conn, CopyStatement("SELECT r FROM range(10) t(r)", path, "copy_bind_fails")) ==
	        DUCKDB_V2_ERROR_IO_GENERAL);
	REQUIRE(RunFailingCopy(fx.conn, CopyStatement("SELECT r FROM range(10) t(r)", path, "copy_batch_size_fails")) ==
	        DUCKDB_V2_ERROR_IO_GENERAL);
	// A batch size callback that sets no target fails the statement.
	REQUIRE(RunFailingCopy(fx.conn, CopyStatement("SELECT r FROM range(10) t(r)", path, "copy_batch_size_empty")) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 copy: registration refusals", "[capi_v2][copy_function]") {
	EnvFixture fx;

	// No name.
	{
		duckdb_v2_copy_function_handle function = nullptr;
		REQUIRE(duckdb_v2_copy_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_copy_function_set_batch_callback(function, CopyNoopBatch, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_copy_function_set_flush_callback(function, CopyNoopFlush, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_copy_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_copy_function_destroy(&function);
	}

	// No batch callback.
	{
		auto function = MakeCopy(fx.conn, "copy_no_batch");
		REQUIRE(duckdb_v2_copy_function_set_flush_callback(function, CopyNoopFlush, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_copy_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_copy_function_destroy(&function);
	}

	// No flush callback.
	{
		auto function = MakeCopy(fx.conn, "copy_no_flush");
		REQUIRE(duckdb_v2_copy_function_set_batch_callback(function, CopyNoopBatch, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_copy_function_register(function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_copy_function_destroy(&function);
	}
}

TEST_CASE("V2 copy: null arguments and destroy null-safety", "[capi_v2][copy_function]") {
	EnvFixture fx;

	duckdb_v2_copy_function_handle function = nullptr;
	REQUIRE(duckdb_v2_copy_function_create_with_connection(nullptr, &function, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(function == nullptr);
	REQUIRE(duckdb_v2_copy_function_create_with_connection(fx.conn, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	REQUIRE(duckdb_v2_copy_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_copy_function_set_name(function, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_set_user_data(function, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_set_batch_size_callback(nullptr, ProbeBatchSize, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_set_batch_callback(nullptr, CopyNoopBatch, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_register(nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	idx_t count = 0;
	duckdb_v2_logical_type_handle type = nullptr;
	duckdb_v2_identifier_t name = {nullptr, 0};
	duckdb_v2_str path = {nullptr, 0};
	duckdb_v2_column_data_collection_handle input = nullptr;
	void *data = nullptr;
	REQUIRE(duckdb_v2_copy_function_bind_get_column_count(nullptr, &count, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_bind_get_column_type(nullptr, 0, &type, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_bind_get_column_name(nullptr, 0, &name, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_batch_size_set_target(nullptr, 1, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_init_get_file_path(nullptr, &path, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_batch_take_input(nullptr, &input, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_flush_get_batch_data(nullptr, &data, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_copy_function_finalize_get_init_data(nullptr, &data, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_copy_function_destroy(&function);

	REQUIRE(duckdb_v2_copy_function_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_handle null_function = nullptr;
	REQUIRE(duckdb_v2_copy_function_destroy(&null_function) == DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
