#include "test_capi_v2.hpp"

#include <map>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 secret tests: define secret types through secret functions, create secrets from SQL, and read, look up and create
// secrets from inside create callbacks (the only place a context is available outside a table function).
//
// Callbacks avoid Catch assertions: a REQUIRE would throw through the C callback boundary into the engine. They record
// what they saw in the user data and the test asserts on it afterwards.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

std::string Str(const char *ptr, idx_t len) {
	return std::string(ptr, len);
}

// What the `probe` provider saw.
struct ProbeLog {
	idx_t calls = 0;
	// The lookup by path
	bool found = false;
	int64_t score = 0;
	std::string type, provider, name, storage, persist;
	std::vector<std::string> scopes;
	std::map<std::string, std::string> values;
	bool token_redacted = false;
	bool region_redacted = true;
	bool missing_key_is_null = false;
	// The lookup by name
	bool by_name_found = false;
	std::string by_name_region;
	// What the callback was not allowed to do
	DUCKDB_V2_ERROR set_on_input_rc = DUCKDB_V2_ERROR_NONE;
	DUCKDB_V2_ERROR set_property_on_output_rc = DUCKDB_V2_ERROR_NONE;
	// What the statement looked like
	std::string input_type, input_provider, input_name;
	// The result of creating a secret from the callback
	DUCKDB_V2_ERROR create_rc = DUCKDB_V2_ERROR_NONE;
	std::string created_name, created_persist;
};

void CopyString(duckdb_v2_value_handle value, std::string &out) {
	if (!value) {
		out.clear();
		return;
	}
	const char *ptr = nullptr;
	idx_t len = 0;
	if (duckdb_v2_value_get_varchar(value, &ptr, &len, nullptr) == DUCKDB_V2_ERROR_NONE) {
		out = Str(ptr, len);
	}
}

std::string GetOption(duckdb_v2_secret_handle input, const char *key) {
	duckdb_v2_value_handle value = nullptr;
	std::string result;
	if (duckdb_v2_secret_get_value(input, key, strlen(key), &value, nullptr) == DUCKDB_V2_ERROR_NONE) {
		CopyString(value, result);
	}
	duckdb_v2_value_destroy(&value);
	return result;
}

void RecordSecret(ProbeLog &log, duckdb_v2_secret_handle secret) {
	auto read = [&](DUCKDB_V2_SECRET_PROPERTY property) {
		const char *ptr = nullptr;
		idx_t len = 0;
		duckdb_v2_secret_get_property(secret, property, &ptr, &len, nullptr);
		return Str(ptr, len);
	};
	log.type = read(DUCKDB_V2_SECRET_PROPERTY_TYPE);
	log.provider = read(DUCKDB_V2_SECRET_PROPERTY_PROVIDER);
	log.name = read(DUCKDB_V2_SECRET_PROPERTY_NAME);
	log.storage = read(DUCKDB_V2_SECRET_PROPERTY_STORAGE);
	log.persist = read(DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE);
	idx_t scopes = 0;
	duckdb_v2_secret_get_scope_count(secret, &scopes, nullptr);
	log.scopes.clear();
	for (idx_t i = 0; i < scopes; i++) {
		const char *ptr = nullptr;
		idx_t len = 0;
		duckdb_v2_secret_get_scope(secret, i, &ptr, &len, nullptr);
		log.scopes.push_back(Str(ptr, len));
	}
	idx_t keys = 0;
	duckdb_v2_secret_get_key_count(secret, &keys, nullptr);
	log.values.clear();
	for (idx_t i = 0; i < keys; i++) {
		const char *key = nullptr;
		idx_t key_len = 0;
		duckdb_v2_secret_get_key(secret, i, &key, &key_len, nullptr);
		duckdb_v2_value_handle value = nullptr;
		duckdb_v2_secret_get_value(secret, key, key_len, &value, nullptr);
		log.values[Str(key, key_len)] = value ? Render(value) : "<missing>";
		duckdb_v2_value_destroy(&value);
	}
	duckdb_v2_secret_is_key_redacted(secret, "token", 5, &log.token_redacted, nullptr);
	duckdb_v2_secret_is_key_redacted(secret, "region", 6, &log.region_redacted, nullptr);
	duckdb_v2_value_handle missing = reinterpret_cast<duckdb_v2_value_handle>(1);
	duckdb_v2_secret_get_value(secret, "nope", 4, &missing, nullptr);
	log.missing_key_is_null = missing == nullptr;
}

// The `demo` provider: copies its options into the secret, redacting `token`, and scopes it to `demo://` unless the
// statement scoped it. It reports a `fail` option as an error.
void CreateDemo(duckdb_v2_secret_handle input, duckdb_v2_secret_handle output, void *user_data,
                duckdb_v2_context_handle, duckdb_v2_error_info_handle *err) {
	auto &calls = *static_cast<idx_t *>(user_data);
	calls++;
	if (!GetOption(input, "fail").empty()) {
		duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
		std::string_view text = "demo secret refused";
		duckdb_v2_error_info_set_text(*err, text.data(), text.size());
		return;
	}
	idx_t keys = 0;
	duckdb_v2_secret_get_key_count(input, &keys, err);
	for (idx_t i = 0; i < keys; i++) {
		const char *key = nullptr;
		idx_t key_len = 0;
		duckdb_v2_value_handle value = nullptr;
		if (duckdb_v2_secret_get_key(input, i, &key, &key_len, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_secret_get_value(input, key, key_len, &value, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		auto rc = duckdb_v2_secret_set_value(output, key, key_len, value, err);
		duckdb_v2_value_destroy(&value);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	duckdb_v2_secret_add_redacted_key(output, "token", 5, err);
	idx_t scopes = 0;
	duckdb_v2_secret_get_scope_count(output, &scopes, err);
	if (scopes == 0) {
		duckdb_v2_secret_add_scope(output, "demo://", 7, err);
	}
}

// The `probe` provider: reads secrets and reports what it saw. Options: `path` looks up the best `demo` secret for a
// path, `by_name` fetches a secret by name, `vend` creates a `demo` secret from inside the callback.
void CreateProbe(duckdb_v2_secret_handle input, duckdb_v2_secret_handle output, void *user_data,
                 duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	auto &log = *static_cast<ProbeLog *>(user_data);
	log.calls++;
	log.input_type = [&] {
		const char *ptr = nullptr;
		idx_t len = 0;
		duckdb_v2_secret_get_property(input, DUCKDB_V2_SECRET_PROPERTY_TYPE, &ptr, &len, nullptr);
		return Str(ptr, len);
	}();
	log.found = false;
	log.by_name_found = false;

	// The input is read-only, the properties of the output are fixed.
	duckdb_v2_value_handle value = nullptr;
	duckdb_v2_factory_handle factory = nullptr;
	duckdb_v2_context_get_factory(context, &factory, nullptr);
	duckdb_v2_value_create_bigint(factory, 1, &value, nullptr);
	log.set_on_input_rc = duckdb_v2_secret_set_value(input, "x", 1, value, nullptr);
	log.set_property_on_output_rc =
	    duckdb_v2_secret_set_property(output, DUCKDB_V2_SECRET_PROPERTY_NAME, "other", 5, nullptr);
	duckdb_v2_value_destroy(&value);

	auto path = GetOption(input, "path");
	if (!path.empty()) {
		duckdb_v2_secret_handle secret = nullptr;
		if (duckdb_v2_context_lookup_secret(context, path.data(), path.size(), "demo", 4, &secret, &log.score, err) !=
		    DUCKDB_V2_ERROR_NONE) {
			return;
		}
		if (secret) {
			log.found = true;
			RecordSecret(log, secret);
		}
		duckdb_v2_secret_destroy(&secret);
	}
	auto by_name = GetOption(input, "by_name");
	if (!by_name.empty()) {
		duckdb_v2_secret_handle secret = nullptr;
		if (duckdb_v2_context_get_secret_by_name(context, by_name.data(), by_name.size(), &secret, err) !=
		    DUCKDB_V2_ERROR_NONE) {
			return;
		}
		if (secret) {
			log.by_name_found = true;
			duckdb_v2_value_handle region = nullptr;
			duckdb_v2_secret_get_value(secret, "region", 6, &region, nullptr);
			CopyString(region, log.by_name_region);
			duckdb_v2_value_destroy(&region);
		}
		duckdb_v2_secret_destroy(&secret);
	}
	auto create = GetOption(input, "vend");
	if (!create.empty()) {
		// A transaction secret of type demo, created through the demo provider, scoped to txn://
		duckdb_v2_secret_handle request = nullptr;
		duckdb_v2_secret_create(factory, &request, nullptr);
		duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_TYPE, "demo", 4, nullptr);
		duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_NAME, create.data(), create.size(), nullptr);
		duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE, "transaction", 11, nullptr);
		duckdb_v2_secret_add_scope(request, "txn://", 6, nullptr);
		duckdb_v2_value_handle region = nullptr;
		std::string_view text = "vended";
		duckdb_v2_value_create_varchar(factory, text.data(), text.size(), &region, nullptr);
		duckdb_v2_secret_set_value(request, "region", 6, region, nullptr);
		duckdb_v2_value_destroy(&region);
		duckdb_v2_secret_handle created = nullptr;
		log.create_rc =
		    duckdb_v2_context_create_secret(context, request, DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE, &created, err);
		if (created) {
			const char *ptr = nullptr;
			idx_t len = 0;
			duckdb_v2_secret_get_property(created, DUCKDB_V2_SECRET_PROPERTY_NAME, &ptr, &len, nullptr);
			log.created_name = Str(ptr, len);
			duckdb_v2_secret_get_property(created, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE, &ptr, &len, nullptr);
			log.created_persist = Str(ptr, len);
		}
		duckdb_v2_secret_destroy(&created);
		duckdb_v2_secret_destroy(&request);
	}
}

duckdb_v2_secret_function_handle MakeFunction(duckdb_v2_factory_handle factory, std::string_view type,
                                              std::string_view provider, duckdb_v2_secret_create_callback_fn callback,
                                              void *user_data, bool is_default) {
	duckdb_v2_secret_function_handle function = nullptr;
	REQUIRE(duckdb_v2_secret_function_create(factory, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_function_set_type(function, type.data(), type.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_function_set_provider(function, provider.data(), provider.size(), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_function_set_default(function, is_default, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_function_set_create_callback(function, callback, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_opaque data;
	data.ptr = user_data;
	data.destroy = nullptr;
	data.equals = nullptr;
	REQUIRE(duckdb_v2_secret_function_set_user_data(function, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
	return function;
}

void AddParameter(duckdb_v2_factory_handle factory, duckdb_v2_secret_function_handle function, std::string_view name,
                  DUCKDB_V2_LOGICAL_TYPE_ID id) {
	auto type = MakeType(factory, id);
	REQUIRE(duckdb_v2_secret_function_add_named_parameter(function, name.data(), name.size(), type, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&type);
}

// Registers the demo provider (the default of type demo) and the probe provider on the connection's instance.
void RegisterProviders(EnvFixture &fx, idx_t &demo_calls, ProbeLog &log) {
	auto demo = MakeFunction(fx.factory, "demo", "config", CreateDemo, &demo_calls, true);
	AddParameter(fx.factory, demo, "region", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	AddParameter(fx.factory, demo, "token", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	AddParameter(fx.factory, demo, "port", DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	AddParameter(fx.factory, demo, "fail", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	AddParameter(fx.factory, demo, "anything", DUCKDB_V2_LOGICAL_TYPE_ID_ANY);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, demo, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_function_destroy(&demo);

	auto probe = MakeFunction(fx.factory, "probe", "config", CreateProbe, &log, true);
	AddParameter(fx.factory, probe, "path", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	AddParameter(fx.factory, probe, "by_name", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	AddParameter(fx.factory, probe, "vend", DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, probe, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_function_destroy(&probe);
}

} // namespace

TEST_CASE("V2 secret: a registered provider creates secrets from SQL", "[capi_v2][secret]") {
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);

	// The type's default provider is used when the statement names none, named parameters are cast.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET s1 (TYPE demo, region 'eu', token 'shh', port 5)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(demo_calls == 1);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET s2 (TYPE demo, PROVIDER config, region 'us', SCOPE 'demo://special')") ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(demo_calls == 2);

	// What the statement and the stored secret look like, read back from another provider.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET p1 (TYPE probe, path 'demo://bucket/key', by_name 's2')") ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(log.input_type == "probe");
	REQUIRE(log.found);
	REQUIRE(log.type == "demo");
	REQUIRE(log.provider == "config");
	REQUIRE(log.name == "s1");
	REQUIRE(log.persist == "temporary");
	REQUIRE(log.storage == "memory");
	REQUIRE(log.scopes == std::vector<std::string> {"demo://"});
	REQUIRE(log.values["region"] == "eu");
	REQUIRE(log.values["token"] == "shh");
	REQUIRE(log.values["port"] == "5");
	REQUIRE(log.token_redacted);
	REQUIRE_FALSE(log.region_redacted);
	REQUIRE(log.missing_key_is_null);
	REQUIRE(log.by_name_found);
	REQUIRE(log.by_name_region == "us");

	// The best match is the one with the longest matching scope, and the score ranks matches.
	auto default_score = log.score;
	REQUIRE(RunSql(fx.conn, "CREATE SECRET p2 (TYPE probe, path 'demo://special/key')") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(log.name == "s2");
	REQUIRE(log.scopes == std::vector<std::string> {"demo://special"});
	REQUIRE(log.score > default_score);

	// No match and no such name: a null handle, not an error.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET p3 (TYPE probe, path 'other://x', by_name 'missing')") ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(log.found);
	REQUIRE_FALSE(log.by_name_found);

	// Read-only input, fixed output properties.
	REQUIRE(log.set_on_input_rc == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(log.set_property_on_output_rc == DUCKDB_V2_ERROR_INPUT_INVALID);
}

TEST_CASE("V2 secret: errors are reported as the statement's error", "[capi_v2][secret]") {
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);

	// A callback error fails the statement and creates no secret.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET bad (TYPE demo, fail 'yes')") == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET p (TYPE probe, by_name 'bad')") == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(log.by_name_found);
	// Unknown and mistyped parameters, unknown provider
	REQUIRE(RunSql(fx.conn, "CREATE SECRET x (TYPE demo, bogus 1)") == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET x (TYPE demo, port 'not a number')") == DUCKDB_V2_ERROR_QUERY_BINDER);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET x (TYPE demo, PROVIDER nope)") == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Registration is validated: a function exists once per type and provider, and only the function defining a type
	// can be its default.
	auto duplicate = MakeFunction(fx.factory, "demo", "config", CreateDemo, &demo_calls, false);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, duplicate, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_secret_function_destroy(&duplicate);
	auto second_default = MakeFunction(fx.factory, "demo", "other", CreateDemo, &demo_calls, true);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, second_default, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_secret_function_destroy(&second_default);
	auto second_provider = MakeFunction(fx.factory, "demo", "other", CreateDemo, &demo_calls, false);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, second_provider, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_function_destroy(&second_provider);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET y (TYPE demo, PROVIDER other)") == DUCKDB_V2_ERROR_NONE);

	// A function needs a type, a provider and a callback.
	duckdb_v2_secret_function_handle incomplete = nullptr;
	REQUIRE(duckdb_v2_secret_function_create(fx.factory, &incomplete, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, incomplete, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_secret_function_destroy(&incomplete);
}

TEST_CASE("V2 secret: a callback creates transaction secrets, which end with the transaction", "[capi_v2][secret]") {
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);

	REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET c1 (TYPE probe, vend 'vended_1')") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(log.create_rc == DUCKDB_V2_ERROR_NONE);
	REQUIRE(log.created_name == "vended_1");
	REQUIRE(log.created_persist == "transaction");
	// Visible inside the transaction, by path and by name, with the values the provider stored.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET c2 (TYPE probe, path 'txn://a', by_name 'vended_1')") ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(log.found);
	REQUIRE(log.name == "vended_1");
	REQUIRE(log.persist == "transaction");
	REQUIRE(log.values["region"] == "vended");
	REQUIRE(log.by_name_found);
	REQUIRE(RunSql(fx.conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);

	// Gone with the transaction.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET c3 (TYPE probe, path 'txn://a', by_name 'vended_1')") ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(log.found);
	REQUIRE_FALSE(log.by_name_found);

	// The same through SQL is refused, which is why the API creates them.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET t IN transaction (TYPE demo)") == DUCKDB_V2_ERROR_QUERY_BINDER);
}

TEST_CASE("V2 secret: a connection looks up and creates secrets without a callback", "[capi_v2][secret]") {
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET s1 (TYPE demo, region 'eu', token 'shh')") == DUCKDB_V2_ERROR_NONE);

	// By path, with a score, and by name.
	duckdb_v2_secret_handle secret = nullptr;
	int64_t score = 0;
	REQUIRE(duckdb_v2_connection_lookup_secret(fx.conn, "demo://x", 8, "demo", 4, &secret, &score, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(secret != nullptr);
	RecordSecret(log, secret);
	REQUIRE(log.name == "s1");
	REQUIRE(log.values["region"] == "eu");
	REQUIRE(log.token_redacted);
	duckdb_v2_secret_destroy(&secret);
	REQUIRE(duckdb_v2_connection_lookup_secret(fx.conn, "other://x", 9, "demo", 4, &secret, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(secret == nullptr);
	REQUIRE(duckdb_v2_connection_get_secret_by_name(fx.conn, "s1", 2, &secret, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(secret != nullptr);
	duckdb_v2_secret_destroy(&secret);
	REQUIRE(duckdb_v2_connection_get_secret_by_name(fx.conn, "nope", 4, &secret, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(secret == nullptr);

	// Create one from a built secret, through the provider: the keys are the provider's named parameters.
	duckdb_v2_secret_handle request = nullptr;
	REQUIRE(duckdb_v2_secret_create(fx.factory, &request, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_TYPE, "demo", 4, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_NAME, "s2", 2, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_add_scope(request, "made://", 7, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto port = MakeInt32Value(fx.factory, 7);
	REQUIRE(duckdb_v2_secret_set_value(request, "port", 4, port, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&port);
	duckdb_v2_secret_handle created = nullptr;
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, &created,
	                                           nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(demo_calls == 2);
	RecordSecret(log, created);
	REQUIRE(log.name == "s2");
	REQUIRE(log.provider == "config");
	REQUIRE(log.values["port"] == "7");
	duckdb_v2_secret_destroy(&created);
	// The same name again: the conflict behavior decides.
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr,
	                                           nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_IGNORE, &created,
	                                           nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(created != nullptr);
	duckdb_v2_secret_destroy(&created);
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE, nullptr,
	                                           nullptr) == DUCKDB_V2_ERROR_NONE);
	// A request needs a type.
	REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_TYPE, "", 0, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE, nullptr,
	                                           nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_secret_destroy(&request);
	REQUIRE(duckdb_v2_connection_lookup_secret(fx.conn, "made://x", 8, "demo", 4, &secret, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(secret != nullptr);
	duckdb_v2_secret_destroy(&secret);

	// A transaction secret lives in the connection's explicit transaction, and not past it.
	auto make_transaction_secret = [&]() {
		duckdb_v2_secret_handle txn = nullptr;
		REQUIRE(duckdb_v2_secret_create(fx.factory, &txn, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_secret_set_property(txn, DUCKDB_V2_SECRET_PROPERTY_TYPE, "demo", 4, nullptr);
		duckdb_v2_secret_set_property(txn, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE, "transaction", 11, nullptr);
		duckdb_v2_secret_add_scope(txn, "txn://", 6, nullptr);
		REQUIRE(duckdb_v2_connection_create_secret(fx.conn, txn, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr,
		                                           nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_secret_destroy(&txn);
	};
	auto txn_visible = [&]() {
		duckdb_v2_secret_handle found = nullptr;
		REQUIRE(duckdb_v2_connection_lookup_secret(fx.conn, "txn://x", 7, "demo", 4, &found, nullptr, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		bool visible = found != nullptr;
		duckdb_v2_secret_destroy(&found);
		return visible;
	};
	REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	make_transaction_secret();
	REQUIRE(txn_visible());
	REQUIRE(RunSql(fx.conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(txn_visible());
	// Outside an explicit transaction it ends with the implicit one.
	make_transaction_secret();
	REQUIRE_FALSE(txn_visible());
}

TEST_CASE("V2 secret: a connection creates secrets as CREATE SECRET would", "[capi_v2][secret]") {
	EnvFixture fx;
	auto make_request = [&](const char *name, std::initializer_list<std::pair<const char *, std::string>> values) {
		duckdb_v2_secret_handle request = nullptr;
		REQUIRE(duckdb_v2_secret_create(fx.factory, &request, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_TYPE, "http", 4, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		if (name) {
			REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_NAME, name, strlen(name),
			                                      nullptr) == DUCKDB_V2_ERROR_NONE);
		}
		for (auto &entry : values) {
			auto value = MakeVarcharValue(fx.factory, entry.second);
			REQUIRE(duckdb_v2_secret_set_value(request, entry.first, strlen(entry.first), value, nullptr) ==
			        DUCKDB_V2_ERROR_NONE);
			duckdb_v2_value_destroy(&value);
		}
		return request;
	};
	auto create = [&](duckdb_v2_secret_handle request, DUCKDB_V2_SECRET_ON_CONFLICT on_conflict,
	                  duckdb_v2_secret_handle *created) {
		return duckdb_v2_connection_create_secret(fx.conn, request, on_conflict, created, nullptr);
	};

	// The parameters are checked and cast against the provider, also for a provider not registered via this API
	REQUIRE(RunSql(fx.conn, "CREATE SECRET bad_sql (TYPE http, verify_ssl 'not-a-bool')") ==
	        DUCKDB_V2_ERROR_QUERY_BINDER);
	auto bad_value = make_request("bad_api", {{"verify_ssl", "not-a-bool"}});
	REQUIRE(create(bad_value, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);
	duckdb_v2_secret_destroy(&bad_value);
	auto unknown = make_request("unknown_api", {{"bogus", "1"}});
	REQUIRE(create(unknown, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr) == DUCKDB_V2_ERROR_QUERY_BINDER);
	duckdb_v2_secret_destroy(&unknown);
	auto cast = make_request("cast_api", {{"verify_ssl", "false"}});
	REQUIRE(create(cast, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_destroy(&cast);

	// An unnamed secret gets the default name of its type
	REQUIRE(RunSql(fx.conn, "CREATE SECRET (TYPE http)") == DUCKDB_V2_ERROR_NONE);
	auto unnamed = make_request(nullptr, {});
	REQUIRE(create(unnamed, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_secret_handle created = nullptr;
	REQUIRE(create(unnamed, DUCKDB_V2_SECRET_ON_CONFLICT_IGNORE, &created) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(created != nullptr);
	ProbeLog log;
	RecordSecret(log, created);
	REQUIRE(log.name == "__default_http");
	duckdb_v2_secret_destroy(&created);
	duckdb_v2_secret_destroy(&unnamed);
	duckdb_v2_secret_handle empty_name = nullptr;
	REQUIRE(duckdb_v2_connection_get_secret_by_name(fx.conn, "", 0, &empty_name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(empty_name == nullptr);

	// An ignored conflict returns the secret from the storage the request resolved to
	REQUIRE(RunSql(fx.conn, "CREATE TEMPORARY SECRET duplicate (TYPE http, http_proxy 'memory_one')") ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	auto in_transaction = make_request("duplicate", {{"http_proxy", "txn_one"}});
	REQUIRE(duckdb_v2_secret_set_property(in_transaction, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE, "transaction", 11,
	                                      nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(create(in_transaction, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_destroy(&in_transaction);
	auto in_memory = make_request("duplicate", {{"http_proxy", "ignored"}});
	REQUIRE(duckdb_v2_secret_set_property(in_memory, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE, "temporary", 9, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_set_property(in_memory, DUCKDB_V2_SECRET_PROPERTY_STORAGE, "memory", 6, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(create(in_memory, DUCKDB_V2_SECRET_ON_CONFLICT_IGNORE, &created) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(created != nullptr);
	RecordSecret(log, created);
	REQUIRE(log.storage == "memory");
	REQUIRE(log.values["http_proxy"] == "memory_one");
	duckdb_v2_secret_destroy(&created);
	duckdb_v2_secret_destroy(&in_memory);
	REQUIRE(RunSql(fx.conn, "ROLLBACK") == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 secret: a parameter of type ANY takes the value as provided", "[capi_v2][secret]") {
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);

	// From SQL
	REQUIRE(RunSql(fx.conn, "CREATE SECRET from_sql (TYPE demo, anything 42)") == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_handle secret = nullptr;
	REQUIRE(duckdb_v2_connection_get_secret_by_name(fx.conn, "from_sql", 8, &secret, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(secret != nullptr);
	RecordSecret(log, secret);
	REQUIRE(log.values["anything"] == "42");
	duckdb_v2_secret_destroy(&secret);

	// From the API
	duckdb_v2_secret_handle request = nullptr;
	REQUIRE(duckdb_v2_secret_create(fx.factory, &request, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_TYPE, "demo", 4, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_NAME, "from_api", 8, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	auto value = MakeVarcharValue(fx.factory, "free text");
	REQUIRE(duckdb_v2_secret_set_value(request, "anything", 8, value, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&value);
	duckdb_v2_secret_handle created = nullptr;
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, &created,
	                                           nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(created != nullptr);
	RecordSecret(log, created);
	REQUIRE(log.values["anything"] == "free text");
	duckdb_v2_secret_destroy(&created);
	duckdb_v2_secret_destroy(&request);
}

TEST_CASE("V2 secret: a type an extension defines is loaded, not defined by a provider", "[capi_v2][secret]") {
	EnvFixture fx;
	REQUIRE(RunSql(fx.conn, "SET autoload_known_extensions = false") == DUCKDB_V2_ERROR_NONE);
	idx_t calls = 0;
	// mysql belongs to mysql_scanner, which is not loaded
	auto function = MakeFunction(fx.factory, "mysql", "vault", CreateDemo, &calls, false);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_secret_function_destroy(&function);
	REQUIRE(RunSql(fx.conn, "SELECT CASE WHEN count(*) > 0 THEN error('mysql was defined') END "
	                        "FROM duckdb_secret_types() WHERE type = 'mysql'") == DUCKDB_V2_ERROR_NONE);
	// a type no extension defines is defined by its first provider
	auto own = MakeFunction(fx.factory, "my_vault", "vault", CreateDemo, &calls, true);
	REQUIRE(duckdb_v2_connection_register_secret_function(fx.conn, own, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_function_destroy(&own);
	REQUIRE(RunSql(fx.conn, "CREATE SECRET v (TYPE my_vault)") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(calls == 1);
}

TEST_CASE("V2 secret: a looked-up transaction secret can be created again", "[capi_v2][secret]") {
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);

	auto set = [&](duckdb_v2_secret_handle secret, DUCKDB_V2_SECRET_PROPERTY property, std::string_view value) {
		REQUIRE(duckdb_v2_secret_set_property(secret, property, value.data(), value.size(), nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
	};
	REQUIRE(RunSql(fx.conn, "BEGIN") == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_handle vended = nullptr;
	REQUIRE(duckdb_v2_secret_create(fx.factory, &vended, nullptr) == DUCKDB_V2_ERROR_NONE);
	set(vended, DUCKDB_V2_SECRET_PROPERTY_TYPE, "demo");
	set(vended, DUCKDB_V2_SECRET_PROPERTY_NAME, "vended");
	set(vended, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE, "transaction");
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, vended, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_destroy(&vended);

	// The refresh recipe: copy the properties of the old secret, set the refresh options as keys.
	duckdb_v2_secret_handle old = nullptr;
	REQUIRE(duckdb_v2_connection_get_secret_by_name(fx.conn, "vended", 6, &old, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(old != nullptr);
	RecordSecret(log, old);
	REQUIRE(log.storage == "transaction");
	REQUIRE(log.persist == "transaction");
	auto copy = [&](bool with_persist_type) {
		duckdb_v2_secret_handle request = nullptr;
		REQUIRE(duckdb_v2_secret_create(fx.factory, &request, nullptr) == DUCKDB_V2_ERROR_NONE);
		for (auto property :
		     {DUCKDB_V2_SECRET_PROPERTY_TYPE, DUCKDB_V2_SECRET_PROPERTY_PROVIDER, DUCKDB_V2_SECRET_PROPERTY_NAME,
		      DUCKDB_V2_SECRET_PROPERTY_STORAGE, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE}) {
			if (property == DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE && !with_persist_type) {
				continue;
			}
			const char *ptr = nullptr;
			idx_t len = 0;
			REQUIRE(duckdb_v2_secret_get_property(old, property, &ptr, &len, nullptr) == DUCKDB_V2_ERROR_NONE);
			if (len > 0) {
				set(request, property, std::string_view(ptr, len));
			}
		}
		auto region = MakeVarcharValue(fx.factory, "refreshed");
		REQUIRE(duckdb_v2_secret_set_value(request, "region", 6, region, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_value_destroy(&region);
		return request;
	};
	for (bool with_persist_type : {true, false}) {
		auto request = copy(with_persist_type);
		duckdb_v2_secret_handle created = nullptr;
		REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE, &created,
		                                           nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(created != nullptr);
		RecordSecret(log, created);
		REQUIRE(log.persist == "transaction");
		REQUIRE(log.values["region"] == "refreshed");
		duckdb_v2_secret_destroy(&created);
		duckdb_v2_secret_destroy(&request);
	}
	// Storage `transaction` with another persist type is refused before the provider runs.
	auto request = copy(false);
	set(request, DUCKDB_V2_SECRET_PROPERTY_PERSIST_TYPE, "temporary");
	auto calls_before = demo_calls;
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_REPLACE, nullptr,
	                                           nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(demo_calls == calls_before);
	duckdb_v2_secret_destroy(&request);
	duckdb_v2_secret_destroy(&old);
	REQUIRE(RunSql(fx.conn, "COMMIT") == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 secret: creating secrets without external access is refused first", "[capi_v2][secret]") {
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);
	REQUIRE(RunSql(fx.conn, "SET enable_external_access = false") == DUCKDB_V2_ERROR_NONE);

	// The permission error comes before the parameters are checked, from SQL and from the API alike.
	REQUIRE(RunSql(fx.conn, "CREATE SECRET x (TYPE demo, bogus 1)") == DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION);
	duckdb_v2_secret_handle request = nullptr;
	REQUIRE(duckdb_v2_secret_create(fx.factory, &request, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_secret_set_property(request, DUCKDB_V2_SECRET_PROPERTY_TYPE, "demo", 4, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	auto value = MakeInt32Value(fx.factory, 1);
	REQUIRE(duckdb_v2_secret_set_value(request, "bogus", 5, value, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_value_destroy(&value);
	REQUIRE(duckdb_v2_connection_create_secret(fx.conn, request, DUCKDB_V2_SECRET_ON_CONFLICT_ERROR, nullptr,
	                                           nullptr) == DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION);
	duckdb_v2_secret_destroy(&request);
	REQUIRE(demo_calls == 0);
}

TEST_CASE("V2 secret: a persistent secret of an API-defined type survives a restart", "[capi_v2][secret]") {
	auto secret_dir = duckdb::TestCreatePath("v2_secret_persistent");
	auto set_dir = "SET secret_directory = '" + secret_dir + "'";
	{
		EnvFixture fx;
		idx_t demo_calls = 0;
		ProbeLog log;
		RegisterProviders(fx, demo_calls, log);
		REQUIRE(RunSql(fx.conn, set_dir) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(RunSql(fx.conn, "CREATE PERSISTENT SECRET kept (TYPE demo, region 'eu', token 'shh', port 5, "
		                        "SCOPE 'demo://kept')") == DUCKDB_V2_ERROR_NONE);
	}
	EnvFixture fx;
	idx_t demo_calls = 0;
	ProbeLog log;
	RegisterProviders(fx, demo_calls, log);
	REQUIRE(RunSql(fx.conn, set_dir) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_secret_handle secret = nullptr;
	REQUIRE(duckdb_v2_connection_get_secret_by_name(fx.conn, "kept", 4, &secret, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(secret != nullptr);
	RecordSecret(log, secret);
	REQUIRE(log.type == "demo");
	REQUIRE(log.provider == "config");
	REQUIRE(log.persist == "persistent");
	REQUIRE(log.storage == "local_file");
	REQUIRE(log.scopes == std::vector<std::string> {"demo://kept"});
	REQUIRE(log.values["region"] == "eu");
	REQUIRE(log.values["token"] == "shh");
	REQUIRE(log.values["port"] == "5");
	REQUIRE(log.token_redacted);
	REQUIRE_FALSE(log.region_redacted);
	duckdb_v2_secret_destroy(&secret);
	REQUIRE(demo_calls == 0);
	REQUIRE(RunSql(fx.conn, "DROP PERSISTENT SECRET kept") == DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
