#include "test_capi_v2.hpp"

// ---------------------------------------------------------------------------
// V2 user data tests: attach caller-owned data to a connection under a key and
// read it back from the connection or from a context inside a callback.
//
// Callbacks avoid Catch assertions: a REQUIRE would throw through the C
// callback boundary into the engine. Cross-callback observations are latched
// into file-scope statics and asserted after the query.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

// Counts destroy-callback invocations so the test can pin down when they run.
struct Payload {
	int value = 0;
	int *destroyed = nullptr;
};

void DestroyPayload(void *ptr) {
	auto *payload = static_cast<Payload *>(ptr);
	if (payload->destroyed) {
		(*payload->destroyed)++;
	}
	delete payload;
}

duckdb_v2_opaque Opaque(Payload *payload) {
	return duckdb_v2_opaque {payload, DestroyPayload, nullptr};
}

void *GetConn(duckdb_v2_connection_handle conn, const char *key) {
	void *out = reinterpret_cast<void *>(0x1);
	REQUIRE(duckdb_v2_connection_get_user_data(conn, Convert(key), &out, nullptr) == DUCKDB_V2_ERROR_NONE);
	return out;
}

// What the exec callback observed through the context.
struct UserDataProbe {
	void *seen_from_context = nullptr;
	Payload *set_from_context = nullptr;
} user_data_probe;

void UserDataProbeExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle context,
               duckdb_v2_error_info_handle *err) {
	if (duckdb_v2_context_get_user_data(context, Convert("host"), &user_data_probe.seen_from_context, err) !=
	    DUCKDB_V2_ERROR_NONE) {
		return;
	}
	user_data_probe.set_from_context = new Payload {7, nullptr};
	auto opaque = Opaque(user_data_probe.set_from_context);
	if (duckdb_v2_context_set_user_data(context, Convert("callback"), &opaque, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_handle out = nullptr;
	void *raw = nullptr;
	if (duckdb_v2_scalar_function_exec_get_result(info, &out, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(out, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	static_cast<int32_t *>(raw)[0] = 1;
}

void RegisterUserDataProbe(duckdb_v2_connection_handle conn) {
	auto integer = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_scalar_function_handle function = nullptr;
	REQUIRE(duckdb_v2_scalar_function_create_with_connection(conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Convert("probe_user_data");
	REQUIRE(duckdb_v2_scalar_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_scalar_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, integer, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_set_exec_callback(function, UserDataProbeExec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_destroy(&function) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&integer);
}

} // namespace

TEST_CASE("V2 user data: set, get, replace and remove on a connection", "[capi_v2][user_data]") {
	EnvFixture fx;
	int destroyed = 0;

	REQUIRE(GetConn(fx.conn, "missing") == nullptr);

	auto *first = new Payload {1, &destroyed};
	auto opaque = Opaque(first);
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("k"), &opaque, nullptr) == DUCKDB_V2_ERROR_NONE);
	// The struct is copied out; clobbering it afterwards changes nothing.
	opaque = duckdb_v2_opaque {};
	REQUIRE(GetConn(fx.conn, "k") == first);
	REQUIRE(destroyed == 0);

	// Replacing destroys the old value and exposes the new one.
	auto *second = new Payload {2, &destroyed};
	opaque = Opaque(second);
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("k"), &opaque, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroyed == 1);
	REQUIRE(GetConn(fx.conn, "k") == second);

	// Keys are independent.
	auto *other = new Payload {3, &destroyed};
	opaque = Opaque(other);
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("other"), &opaque, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(GetConn(fx.conn, "k") == second);
	REQUIRE(GetConn(fx.conn, "other") == other);

	// A null opaque removes the entry, destroying its value; removing again is a no-op.
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("k"), nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroyed == 2);
	REQUIRE(GetConn(fx.conn, "k") == nullptr);
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("k"), nullptr, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroyed == 2);

	// An opaque without a destructor is stored as-is and never destroyed.
	int plain = 0;
	opaque = duckdb_v2_opaque {&plain, nullptr, nullptr};
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("plain"), &opaque, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(GetConn(fx.conn, "plain") == &plain);

	// Whatever is left is destroyed with the connection.
	REQUIRE(duckdb_v2_connection_destroy(&fx.conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroyed == 3);
}

TEST_CASE("V2 user data: connections do not share entries", "[capi_v2][user_data]") {
	EnvFixture fx;
	duckdb_v2_connection_handle other = nullptr;
	REQUIRE(duckdb_v2_connection_create(fx.db, &other, nullptr) == DUCKDB_V2_ERROR_NONE);

	int a = 0;
	auto opaque = duckdb_v2_opaque {&a, nullptr, nullptr};
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("k"), &opaque, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(GetConn(fx.conn, "k") == &a);
	REQUIRE(GetConn(other, "k") == nullptr);

	REQUIRE(duckdb_v2_connection_destroy(&other) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 user data: visible from a context and back", "[capi_v2][user_data]") {
	EnvFixture fx;
	int destroyed = 0;
	user_data_probe = UserDataProbe {};
	RegisterUserDataProbe(fx.conn);

	auto *host = new Payload {5, &destroyed};
	auto opaque = Opaque(host);
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert("host"), &opaque, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT probe_user_data()", &result) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);

	// The callback saw what the host set, and what it set is visible to the host.
	REQUIRE(user_data_probe.seen_from_context == host);
	REQUIRE(user_data_probe.set_from_context != nullptr);
	REQUIRE(GetConn(fx.conn, "callback") == user_data_probe.set_from_context);
	REQUIRE(destroyed == 0);

	REQUIRE(duckdb_v2_connection_destroy(&fx.conn) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroyed == 1);
}

TEST_CASE("V2 user data: null arguments and empty key", "[capi_v2][user_data]") {
	EnvFixture fx;
	void *out = nullptr;
	int a = 0;
	auto opaque = duckdb_v2_opaque {&a, nullptr, nullptr};
	auto bad_key = duckdb_v2_str {nullptr, 3};

	REQUIRE(duckdb_v2_connection_set_user_data(nullptr, Convert("k"), &opaque, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, bad_key, &opaque, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_set_user_data(fx.conn, Convert(""), &opaque, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_get_user_data(nullptr, Convert("k"), &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_get_user_data(fx.conn, bad_key, &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_get_user_data(fx.conn, Convert(""), &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_get_user_data(fx.conn, Convert("k"), nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_context_set_user_data(nullptr, Convert("k"), &opaque, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_context_get_user_data(nullptr, Convert("k"), &out, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Rejected calls leave nothing behind.
	REQUIRE(GetConn(fx.conn, "k") == nullptr);
}

} // namespace test_capi_v2
