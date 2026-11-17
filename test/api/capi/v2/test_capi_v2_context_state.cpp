#include "test_capi_v2.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 extension state: connection state with query / transaction / profiling
// callbacks and a destructor.
//
// Callbacks avoid Catch assertions: they run inside DuckDB. They record what
// they see in their user data, and the test asserts on it afterwards.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

constexpr const char *STATE_KEY = "v2_test.state";

// Reports a failure through the live error slot a callback receives.
void Fail(duckdb_v2_error_info_handle *err, const char *message) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_set_text(*err, message, std::strlen(message));
}

// What the state saw, carried in its user data.
struct Recorder {
	std::vector<std::string> events;
	std::string last_query_error;
	bool fail_query_begin = false;
	bool fail_init = false;
	idx_t inits = 0;
	idx_t profiles = 0;
	bool *destroyed = nullptr;
	// filled in from inside a callback, to test the context-based lookups
	bool context_found_self = false;
	void *context_data = nullptr;
};

void DestroyRecorder(void *ptr) {
	auto recorder = static_cast<Recorder *>(ptr);
	if (recorder->destroyed) {
		*recorder->destroyed = true;
	}
	delete recorder;
}

void OnQueryBegin(duckdb_v2_context_handle context, void *user_data, duckdb_v2_error_info_handle *err) {
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.events.push_back("query_begin");
	const char *key = STATE_KEY;
	duckdb_v2_context_get_state(context, key, std::strlen(key), &recorder.context_found_self, &recorder.context_data,
	                            nullptr);
	if (recorder.fail_query_begin) {
		Fail(err, "query refused by the state");
	}
}

void OnQueryEnd(duckdb_v2_context_handle, void *user_data, const char *error, idx_t error_length) {
	auto &recorder = *static_cast<Recorder *>(user_data);
	recorder.events.push_back("query_end");
	recorder.last_query_error = error ? std::string(error, error_length) : std::string();
}

void OnTransactionBegin(duckdb_v2_context_handle, void *user_data, duckdb_v2_error_info_handle *) {
	static_cast<Recorder *>(user_data)->events.push_back("tx_begin");
}

void OnTransactionCommit(duckdb_v2_context_handle, void *user_data) {
	static_cast<Recorder *>(user_data)->events.push_back("tx_commit");
}

void OnTransactionRollback(duckdb_v2_context_handle, void *user_data) {
	static_cast<Recorder *>(user_data)->events.push_back("tx_rollback");
}

void OnProfile(duckdb_v2_context_state_profile_handle profile, void *user_data, duckdb_v2_error_info_handle *err) {
	static_cast<Recorder *>(user_data)->profiles++;
	const char *text = "v2 state profile\n";
	duckdb_v2_context_state_profile_write(profile, text, std::strlen(text), err);
}

// Configures a state to record into the Recorder handed in as the init's user data.
void InitRecording(duckdb_v2_context_handle, void *init_user_data, duckdb_v2_context_state_handle state,
                   duckdb_v2_error_info_handle *err) {
	auto recorder = static_cast<Recorder *>(init_user_data);
	recorder->inits++;
	duckdb_v2_opaque data = {recorder, DestroyRecorder, nullptr};
	duckdb_v2_context_state_set_user_data(state, &data, err);
	if (recorder->fail_init) {
		Fail(err, "init refused");
		return;
	}
	duckdb_v2_context_state_set_query_begin_callback(state, OnQueryBegin, err);
	duckdb_v2_context_state_set_query_end_callback(state, OnQueryEnd, err);
	duckdb_v2_context_state_set_transaction_begin_callback(state, OnTransactionBegin, err);
	duckdb_v2_context_state_set_transaction_commit_callback(state, OnTransactionCommit, err);
	duckdb_v2_context_state_set_transaction_rollback_callback(state, OnTransactionRollback, err);
	duckdb_v2_context_state_set_profiling_callback(state, OnProfile, err);
}

// Registers a recording state on the connection, which then owns the recorder.
DUCKDB_V2_ERROR RegisterRecorder(duckdb_v2_connection_handle conn, Recorder *recorder) {
	const char *key = STATE_KEY;
	return duckdb_v2_connection_get_or_create_state(conn, key, std::strlen(key), InitRecording, recorder, nullptr,
	                                                nullptr);
}

idx_t Count(const Recorder &recorder, const std::string &event) {
	idx_t count = 0;
	for (auto &entry : recorder.events) {
		count += entry == event ? 1 : 0;
	}
	return count;
}

} // namespace

TEST_CASE("V2 connection state: query and transaction callbacks", "[capi_v2][state]") {
	EnvFixture fx;
	auto recorder = new Recorder();
	REQUIRE(RegisterRecorder(fx.conn, recorder) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(RunSql(fx.conn, "SELECT 42") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Count(*recorder, "query_begin") == 1);
	REQUIRE(Count(*recorder, "query_end") == 1);
	REQUIRE(recorder->last_query_error.empty());
	// from inside the callback, the context reaches the state
	REQUIRE(recorder->context_found_self);
	REQUIRE(recorder->context_data == recorder);

	recorder->events.clear();
	REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "SELECT 1") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Count(*recorder, "tx_begin") == 1);
	REQUIRE(Count(*recorder, "tx_commit") == 1);
	REQUIRE(Count(*recorder, "tx_rollback") == 0);

	recorder->events.clear();
	REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "ROLLBACK") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(Count(*recorder, "tx_begin") == 1);
	REQUIRE(Count(*recorder, "tx_commit") == 0);
	REQUIRE(Count(*recorder, "tx_rollback") == 1);

	// a failing query still ends, and tells how
	recorder->events.clear();
	REQUIRE(RunSql(fx.conn, "SELECT * FROM v2_state_missing_table") != DUCKDB_V2_ERROR_NONE);
	REQUIRE(Count(*recorder, "query_end") == 1);
	REQUIRE(recorder->last_query_error.find("v2_state_missing_table") != std::string::npos);
}

TEST_CASE("V2 connection state: created once per key, destroyed with the connection", "[capi_v2][state]") {
	EnvFixture fx;
	bool destroyed = false;
	auto recorder = new Recorder();
	recorder->destroyed = &destroyed;
	const char *key = STATE_KEY;

	void *data = nullptr;
	REQUIRE(duckdb_v2_connection_get_or_create_state(fx.conn, key, std::strlen(key), InitRecording, recorder, &data,
	                                                 nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(data == recorder);
	REQUIRE(recorder->inits == 1);

	// the second get finds the state: no init, same data
	auto other = new Recorder();
	data = nullptr;
	REQUIRE(duckdb_v2_connection_get_or_create_state(fx.conn, key, std::strlen(key), InitRecording, other, &data,
	                                                 nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(data == recorder);
	REQUIRE(other->inits == 0);
	delete other;

	bool found = false;
	data = nullptr;
	REQUIRE(duckdb_v2_connection_get_state(fx.conn, key, std::strlen(key), &found, &data, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(found);
	REQUIRE(data == recorder);

	const char *missing = "v2_test.missing";
	REQUIRE(duckdb_v2_connection_get_state(fx.conn, missing, std::strlen(missing), &found, &data, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(found);

	// another connection has its own state
	duckdb_v2_connection_handle conn2 = nullptr;
	REQUIRE(duckdb_v2_connection_create(fx.instance, &conn2, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_get_state(conn2, key, std::strlen(key), &found, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(found);
	duckdb_v2_connection_destroy(&conn2);
	REQUIRE_FALSE(destroyed);

	duckdb_v2_connection_destroy(&fx.conn);
	REQUIRE(destroyed);
}

TEST_CASE("V2 connection state: remove", "[capi_v2][state]") {
	EnvFixture fx;
	bool destroyed = false;
	auto recorder = new Recorder();
	recorder->destroyed = &destroyed;
	REQUIRE(RegisterRecorder(fx.conn, recorder) == DUCKDB_V2_ERROR_NONE);

	const char *key = STATE_KEY;
	REQUIRE(duckdb_v2_connection_remove_state(fx.conn, key, std::strlen(key), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroyed);
	bool found = true;
	REQUIRE(duckdb_v2_connection_get_state(fx.conn, key, std::strlen(key), &found, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(found);
	// removing again is fine
	REQUIRE(duckdb_v2_connection_remove_state(fx.conn, key, std::strlen(key), nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 connection state: failures", "[capi_v2][state]") {
	EnvFixture fx;
	const char *key = STATE_KEY;

	SECTION("a failing init registers nothing and releases the user data") {
		bool destroyed = false;
		auto recorder = new Recorder();
		recorder->destroyed = &destroyed;
		recorder->fail_init = true;
		REQUIRE(RegisterRecorder(fx.conn, recorder) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(destroyed);
		bool found = true;
		REQUIRE(duckdb_v2_connection_get_state(fx.conn, key, std::strlen(key), &found, nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE_FALSE(found);
	}

	SECTION("an error from the query begin callback fails the query") {
		auto recorder = new Recorder();
		recorder->fail_query_begin = true;
		REQUIRE(RegisterRecorder(fx.conn, recorder) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "SELECT 1") != DUCKDB_V2_ERROR_NONE);
		recorder->fail_query_begin = false;
		REQUIRE(RunSql(fx.conn, "SELECT 1") == DUCKDB_V2_ERROR_NONE);
	}

	SECTION("null arguments") {
		REQUIRE(duckdb_v2_connection_get_or_create_state(fx.conn, nullptr, 5, InitRecording, nullptr, nullptr,
		                                                 nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_connection_get_or_create_state(fx.conn, key, std::strlen(key), nullptr, nullptr, nullptr,
		                                                 nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_connection_get_state(fx.conn, key, std::strlen(key), nullptr, nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
	}
}

TEST_CASE("V2 connection state: profiling", "[capi_v2][state]") {
	EnvFixture fx;
	auto recorder = new Recorder();
	REQUIRE(RegisterRecorder(fx.conn, recorder) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "EXPLAIN ANALYZE SELECT 42") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(recorder->profiles >= 1);
}

} // namespace test_capi_v2
