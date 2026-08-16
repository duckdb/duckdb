#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;

namespace {

constexpr const char *SECRET_TYPE = "derivation_test";

//! Number of times the provider has produced a token
atomic<idx_t> derivation_count {0};
//! When set, the provider throws instead of producing a token
atomic<bool> derivation_fails {false};

//! A provider whose token is different every time, so that a re-derived value can be told apart
//! from a cached one. It is slow on purpose, to widen the window concurrent lookups race over.
unique_ptr<BaseSecret> CreateDerivationTestSecret(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope.empty() ? vector<string> {""} : input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	for (const auto &option : input.options) {
		auto key = Identifier(option.first);
		if (key == "refresh_info") {
			// the recipe is stored as a STRUCT of the named parameters, the user passes it as a MAP
			child_list_t<Value> struct_fields;
			for (const auto &kv_child : MapValue::GetChildren(option.second)) {
				auto kv_pair = StructValue::GetChildren(kv_child);
				struct_fields.emplace_back(kv_pair[0].ToString(), kv_pair[1]);
			}
			secret->secret_map[key] = Value::STRUCT(struct_fields);
		} else {
			secret->secret_map[key] = option.second;
		}
	}

	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	if (derivation_fails) {
		throw InvalidConfigurationException("derivation_test: derivation failed on purpose");
	}

	secret->secret_map["token"] = Value(StringUtil::Format("token_%d", ++derivation_count));
	secret->transient_keys = {"token"};
	return std::move(secret);
}

void RegisterDerivationTestSecret(DatabaseInstance &instance) {
	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {instance, extension_info, Identifier("derivation_test_secret"), Identifier()};
	ExtensionLoader loader {load_info};

	SecretType secret_type;
	secret_type.name = Identifier(SECRET_TYPE);
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction secret_fun = {SECRET_TYPE, "config", CreateDerivationTestSecret};
	secret_fun.named_parameters["refresh_info"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	loader.RegisterFunction(secret_fun);
}

//! Look the secret up the way a consumer does, and return the token it was handed
string LookupToken(DuckDB &db, SecretManager &secret_manager) {
	Connection con(db);
	con.BeginTransaction();
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*con.context);
	SecretMatch match;
	try {
		match = secret_manager.LookupSecret(transaction, "dt://bucket/file", SECRET_TYPE);
	} catch (...) {
		con.Rollback();
		throw;
	}
	con.Commit();
	if (!match.HasMatch()) {
		return "";
	}
	Value token;
	match.GetSecret().Cast<KeyValueSecret>().TryGetValue("token", token);
	return token.IsNull() ? "" : token.ToString();
}

void MarkRejected(DuckDB &db, SecretManager &secret_manager) {
	Connection con(db);
	con.BeginTransaction();
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*con.context);
	auto match = secret_manager.LookupSecret(transaction, "dt://bucket/file", SECRET_TYPE);
	REQUIRE(match.HasMatch());
	match.GetSecret().Cast<KeyValueSecret>().MarkMustRefresh();
	con.Commit();
}

} // namespace

TEST_CASE("Concurrent lookups of a rejected secret derive exactly once", "[secret][.]") {
	derivation_count = 0;
	derivation_fails = false;

	DuckDB db(nullptr);
	RegisterDerivationTestSecret(*db.instance);
	auto &secret_manager = SecretManager::Get(*db.instance);

	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s1 (TYPE derivation_test, SCOPE 'dt://', "
	                          "REFRESH_INFO MAP {'account': 'acct'})"));
	REQUIRE(derivation_count == 1);

	// A consumer found the values it was handed to be rejected
	MarkRejected(db, secret_manager);

	constexpr idx_t THREAD_COUNT = 8;
	vector<string> tokens(THREAD_COUNT);
	vector<string> errors(THREAD_COUNT);
	vector<std::thread> threads;
	for (idx_t i = 0; i < THREAD_COUNT; i++) {
		threads.emplace_back([&, i]() {
			try {
				tokens[i] = LookupToken(db, secret_manager);
			} catch (std::exception &ex) {
				errors[i] = ex.what();
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (idx_t i = 0; i < THREAD_COUNT; i++) {
		INFO("thread " << i << " failed with: " << errors[i]);
		REQUIRE(errors[i].empty());
	}

	// Exactly one of the racing lookups ran the recipe, the rest waited for it
	REQUIRE(derivation_count == 2);

	// ... and every one of them was handed the same, newly derived value
	for (idx_t i = 0; i < THREAD_COUNT; i++) {
		REQUIRE(tokens[i] == "token_2");
	}

	// The secret is valid again, so a further lookup derives nothing
	REQUIRE(LookupToken(db, secret_manager) == "token_2");
	REQUIRE(derivation_count == 2);
}

TEST_CASE("A failed derivation errors and leaves the secret marked", "[secret][.]") {
	derivation_count = 0;
	derivation_fails = false;

	DuckDB db(nullptr);
	RegisterDerivationTestSecret(*db.instance);
	auto &secret_manager = SecretManager::Get(*db.instance);

	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s1 (TYPE derivation_test, SCOPE 'dt://', "
	                          "REFRESH_INFO MAP {'account': 'acct'})"));
	REQUIRE(LookupToken(db, secret_manager) == "token_1");

	MarkRejected(db, secret_manager);
	derivation_fails = true;

	// Handing out values the backend has already rejected would be worse than failing
	REQUIRE_THROWS(LookupToken(db, secret_manager));

	// The failure left the secret marked, so the next lookup tries again rather than serving the
	// rejected values
	derivation_fails = false;
	REQUIRE(LookupToken(db, secret_manager) == "token_2");
	REQUIRE(derivation_count == 2);
}
