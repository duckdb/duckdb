#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret_storage.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/extension_util.hpp"

using namespace duckdb;
using namespace std;

struct TestSecretLog {
	duckdb::mutex lock;
	duckdb::vector<string> remove_secret_requests;
	duckdb::vector<string> write_secret_requests;
};

// Demo secret type
struct DemoSecretType {
	static duckdb::unique_ptr<BaseSecret> CreateDemoSecret(ClientContext &context, CreateSecretInput &input) {
		auto scope = input.scope;
		if (scope.empty()) {
			scope = {""};
		}
		auto return_value = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
		return std::move(return_value);
	}

	static void RegisterDemoSecret(DatabaseInstance &instance, const string &type_name) {
		SecretType secret_type;
		secret_type.name = type_name;
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";
		ExtensionUtil::RegisterSecretType(instance, secret_type);

		CreateSecretFunction secret_fun = {type_name, "config", CreateDemoSecret};
		ExtensionUtil::RegisterFunction(instance, secret_fun);
	}
};

// Demo pluggable secret storage
class TestSecretStorage : public CatalogSetSecretStorage {
public:
	TestSecretStorage(const string &name_p, DatabaseInstance &db, TestSecretLog &logger_p, int64_t tie_break_offset_p)
	    : CatalogSetSecretStorage(db, name_p), tie_break_offset(tie_break_offset_p), logger(logger_p) {
		secrets = make_uniq<CatalogSet>(Catalog::GetSystemCatalog(db));
		persistent = true;
		include_in_lookups = true;
	}
	bool IncludeInLookups() override {
		return include_in_lookups;
	}

	int64_t GetTieBreakOffset() override {
		return tie_break_offset;
	}

	int64_t tie_break_offset;
	bool include_in_lookups;

protected:
	void WriteSecret(const BaseSecret &secret, OnCreateConflict on_conflict) override {
		duckdb::lock_guard<duckdb::mutex> lock(logger.lock);
		logger.write_secret_requests.push_back(secret.GetName());
	};
	virtual void RemoveSecret(const string &secret, OnEntryNotFound on_entry_not_found) override {
		duckdb::lock_guard<duckdb::mutex> lock(logger.lock);
		logger.remove_secret_requests.push_back(secret);
	};

	TestSecretLog &logger;
};

TEST_CASE("Test secret lookups by secret type", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=false;"));

	// Register a demo secret type
	DemoSecretType::RegisterDemoSecret(*db.instance, "secret_type_1");
	DemoSecretType::RegisterDemoSecret(*db.instance, "secret_type_2");

	// Create some secrets
	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s1 (TYPE secret_type_1, SCOPE '')"));
	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s2 (TYPE secret_type_2, SCOPE '')"));

	// Note that the secrets collide completely, except for their types
	auto res1 = con.Query("SELECT which_secret('blablabla', 'secret_type_1')");
	auto res2 = con.Query("SELECT which_secret('blablabla', 'secret_type_2')");

	// Correct secret is selected
	REQUIRE(res1->GetValue(0, 0).ToString() == "s1");
	REQUIRE(res2->GetValue(0, 0).ToString() == "s2");
}

TEST_CASE("Test adding a custom secret storage", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	TestSecretLog log;

	// Register custom secret storage
	auto &secret_manager = duckdb::SecretManager::Get(*db.instance);

	auto storage_ptr = duckdb::make_uniq<TestSecretStorage>("test_storage", *db.instance, log, 30);
	auto &storage_ref = *storage_ptr;
	secret_manager.LoadSecretStorage(std::move(storage_ptr));

	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=true;"));

	// Set custom secret path to prevent interference with other tests
	auto secret_dir = TestCreatePath("custom_secret_storage_cpp_1");
	REQUIRE_NO_FAIL(con.Query("set secret_directory='" + secret_dir + "'"));

	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s1 IN TEST_STORAGE (TYPE S3, SCOPE 's3://foo')"));
	REQUIRE_NO_FAIL(con.Query("CREATE PERSISTENT SECRET s2 IN test_storage (TYPE S3, SCOPE 's3://')"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY SECRET s2 (TYPE S3, SCOPE 's3://')"));

	// We add this secret of the wrong type, but with better matching scope: these should be ignored on lookup
	DemoSecretType::RegisterDemoSecret(*db.instance, "test");
	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s1_test_type IN TEST_STORAGE (TYPE test, SCOPE 's3://foo/bar.csv')"));

	// Inspect current duckdb_secrets output
	auto result = con.Query("SELECT name, storage from duckdb_secrets() ORDER BY type, name, storage");
	REQUIRE(result->RowCount() == 4);
	REQUIRE(result->GetValue(0, 0).ToString() == "s1");
	REQUIRE(result->GetValue(1, 0).ToString() == "test_storage");
	REQUIRE(result->GetValue(0, 1).ToString() == "s2");
	REQUIRE(result->GetValue(1, 1).ToString() == "memory");
	REQUIRE(result->GetValue(0, 2).ToString() == "s2");
	REQUIRE(result->GetValue(1, 2).ToString() == "test_storage");
	REQUIRE(result->GetValue(0, 3).ToString() == "s1_test_type");
	REQUIRE(result->GetValue(1, 3).ToString() == "test_storage");

	auto transaction = CatalogTransaction::GetSystemTransaction(*db.instance);

	// Ambiguous call -> throws
	REQUIRE_THROWS(secret_manager.GetSecretByName(transaction, "s2"));

	// With specific storage -> works
	auto secret_ptr = secret_manager.GetSecretByName(transaction, "s2", "test_storage");
	REQUIRE(secret_ptr);
	REQUIRE(secret_ptr->storage_mode == "test_storage");
	REQUIRE(secret_ptr->secret->GetName() == "s2");

	// Now try resolve secret by path -> this will return s1 because its scope matches best
	auto which_secret_result = con.Query("SELECT which_secret('s3://foo/bar.csv', 's3');");
	REQUIRE(which_secret_result->GetValue(0, 0).ToString() == "s1");

	// Exclude the storage from lookups
	storage_ref.include_in_lookups = false;

	// Now the lookup will choose the other storage
	which_secret_result = con.Query("SELECT which_secret('s3://foo/bar.csv', 's3');");
	REQUIRE(which_secret_result->GetValue(0, 0).ToString() == "s2");

	// Lets drop stuff now
	REQUIRE_NO_FAIL(con.Query("DROP TEMPORARY SECRET s2"));
	REQUIRE_NO_FAIL(con.Query("DROP SECRET s2 FROM test_storage"));
	REQUIRE_NO_FAIL(con.Query("DROP SECRET s1"));

	// Inspect the log from our logger
	REQUIRE(log.remove_secret_requests.size() == 2);
	REQUIRE(log.write_secret_requests.size() == 3);
	REQUIRE(log.write_secret_requests[0] == "s1");
	REQUIRE(log.write_secret_requests[1] == "s2");
	REQUIRE(log.write_secret_requests[2] == "s1_test_type");
	REQUIRE(log.remove_secret_requests[0] == "s2");
	REQUIRE(log.remove_secret_requests[1] == "s1");
}

TEST_CASE("Test tie-break behaviour for custom secret storage", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	TestSecretLog log1;
	TestSecretLog log2;

	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=true;"));

	// Set custom secret path to prevent interference with other tests
	auto secret_dir = TestCreatePath("custom_secret_storage_cpp_2");
	REQUIRE_NO_FAIL(con.Query("set secret_directory='" + secret_dir + "'"));

	// Register custom secret storage
	auto &secret_manager = duckdb::SecretManager::Get(*db.instance);

	// Correct tie-break offset: 30 places it after temporary and persistent
	auto storage_ptr = duckdb::make_uniq<TestSecretStorage>("test_storage_after", *db.instance, log1, 30);
	secret_manager.LoadSecretStorage(std::move(storage_ptr));

	// Correct tie-break offset: 0 places it before temporary and persistent
	auto storage_ptr2 = duckdb::make_uniq<TestSecretStorage>("test_storage_before", *db.instance, log2, 0);
	secret_manager.LoadSecretStorage(std::move(storage_ptr2));

	// Now create 3 secrets with identical scope: the default s3 scope
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY SECRET s1 (TYPE S3)"));
	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s2 IN test_storage_after (TYPE S3)"));
	REQUIRE_NO_FAIL(con.Query("CREATE SECRET s3 IN test_storage_before (TYPE S3)"));

	// Inspect current duckdb_secrets output
	auto result = con.Query("SELECT name, storage from duckdb_secrets() ORDER BY name, storage");
	REQUIRE(result->RowCount() == 3);
	REQUIRE(result->GetValue(0, 0).ToString() == "s1");
	REQUIRE(result->GetValue(1, 0).ToString() == "memory");
	REQUIRE(result->GetValue(0, 1).ToString() == "s2");
	REQUIRE(result->GetValue(1, 1).ToString() == "test_storage_after");
	REQUIRE(result->GetValue(0, 2).ToString() == "s3");
	REQUIRE(result->GetValue(1, 2).ToString() == "test_storage_before");

	result = con.Query("SELECT which_secret('s3://', 's3');");
	REQUIRE(result->GetValue(0, 0).ToString() == "s3");

	REQUIRE_NO_FAIL(con.Query("DROP SECRET s3"));

	result = con.Query("SELECT which_secret('s3://', 's3');");
	REQUIRE(result->GetValue(0, 0).ToString() == "s1");

	REQUIRE_NO_FAIL(con.Query("DROP SECRET s1"));

	result = con.Query("SELECT which_secret('s3://', 's3');");
	REQUIRE(result->GetValue(0, 0).ToString() == "s2");

	REQUIRE_NO_FAIL(con.Query("DROP SECRET s2"));

	// Inspect the log from our logger
	REQUIRE(log1.remove_secret_requests.size() == 1);
	REQUIRE(log1.write_secret_requests.size() == 1);
	REQUIRE(log1.write_secret_requests[0] == "s2");
	REQUIRE(log1.remove_secret_requests[0] == "s2");

	REQUIRE(log2.remove_secret_requests.size() == 1);
	REQUIRE(log2.write_secret_requests.size() == 1);
	REQUIRE(log2.write_secret_requests[0] == "s3");
	REQUIRE(log2.remove_secret_requests[0] == "s3");
}

TEST_CASE("Secret storage tie-break penalty collision: manager loaded after", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=false;"));

	// Register custom secret storage
	auto &secret_manager = duckdb::SecretManager::Get(*db.instance);

	// This collides with the temporary secret storage: it will throw, but only on first use of the secret manager
	TestSecretLog log;
	auto storage_ptr = duckdb::make_uniq<TestSecretStorage>("failing_storage", *db.instance, log, 10);

	// This passes but is actually wrong already
	secret_manager.LoadSecretStorage(std::move(storage_ptr));

	// This will trigger InitializeSecrets and cause tie-break penalty collision
	REQUIRE_FAIL(con.Query("FROM duckdb_secrets();"));
}

TEST_CASE("Secret storage tie-break penalty collision: manager loaded before", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=false;"));

	// Register custom secret storage
	auto &secret_manager = duckdb::SecretManager::Get(*db.instance);

	// This collides with the temporary secret storage: it will throw, but only on first use of the secret manager
	TestSecretLog log;
	auto storage_ptr = duckdb::make_uniq<TestSecretStorage>("failing_storage", *db.instance, log, 10);

	// Ensure secret manager is fully initialized
	REQUIRE_NO_FAIL(con.Query("FROM duckdb_secrets();"));

	// This fails
	REQUIRE_THROWS(secret_manager.LoadSecretStorage(std::move(storage_ptr)));
}

TEST_CASE("Secret storage name collision: manager loaded before", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=false;"));

	// Register custom secret storage
	auto &secret_manager = duckdb::SecretManager::Get(*db.instance);

	// This collides with the memory manager by name
	TestSecretLog log;
	auto storage_ptr = duckdb::make_uniq<TestSecretStorage>("memory", *db.instance, log, 50);

	// Ensure secret manager is fully initialized
	REQUIRE_NO_FAIL(con.Query("FROM duckdb_secrets();"));

	// This fails
	REQUIRE_THROWS(secret_manager.LoadSecretStorage(std::move(storage_ptr)));
}

TEST_CASE("Secret storage name collision: manager loaded after", "[secret][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("set allow_persistent_secrets=false;"));

	// Register custom secret storage
	auto &secret_manager = duckdb::SecretManager::Get(*db.instance);

	// This collides with the memory manager by name
	TestSecretLog log;
	auto storage_ptr = duckdb::make_uniq<TestSecretStorage>("memory", *db.instance, log, 50);

	// This passes but is actually wrong alsready
	secret_manager.LoadSecretStorage(std::move(storage_ptr));

	// This now fails with a name collision warning
	REQUIRE_FAIL(con.Query("FROM duckdb_secrets();"));
}
