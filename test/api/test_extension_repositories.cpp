#include "catch.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension_repository_manager.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static constexpr const char *TEST_PUBLIC_KEY =
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt9+KeDkWJa7SiBb7AMdXAPV2qpRunztoC2c7DtRmN9+Lq2YQm4rMQmmAQcwyxTxcK8vwPK"
    "OiE4mLMwmQMvTty9tL+buREfvg5KJ+FY3Bv05cZYsf7VuYgUDcw41zSEE5W1thh5PiafcrMn7CPF0jN0oFfVvAHB/fAM0yaZGPKs3WYqnHAHa6v/cV"
    "MeiWZ/lQORCBqqL2hii1fEiuIqw6AOEoZVsN0/lqm7GfnD899zrzLOPRuP4YFg46drL1UyNdg+4TquJ2OV9kR4CAfdGRexqHFWLmoxkTfnUxJ4yL+g"
    "kV57K1H+smEdKrDhDX68pEFM/34E2eVKZoRU/0paxDZQIDAQAB";

static constexpr const char *OTHER_PUBLIC_KEY =
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvTX0K9ihHb3oHcfV+hKS+lcXhWGjRtxGHUkSvMnuTOExsH78a7FcafIF8YcPwRw7cxIK8o"
    "aHIZ0c/DLTR6s9O2qvUzvIU4N6F/rc/ruFJIMSIlN3+CWKxKl/x/unWnnJoza/30zI69n4fuvVHcblGtxLB41tiF3un2Nfs0BrHMVBocEb+vBeozVy"
    "NnZxyKIa9SLy9ZM2/CwsEt/l0uQKwF6dJ+jT11gsitJQuQ4wP18pAx8qamV3Pg6MS3lktd7BlC/Fdm/4Sp68ik971CLYWhkBe3sIzCPbkpwzjeJu52"
    "hE7EOyuB5KaWYpknapwxv5B/MG0y75kRtOnK+L+4PsvQIDAQAB";

static bool KeyIsTrusted(DatabaseInstance &db, ExtensionRepositoryType type, const string &repository_name,
                         const string &compact_key) {
	auto pem_key = ExtensionRepositoryManager::ToPEMPublicKey(compact_key);
	for (auto &key : ExtensionHelper::GetTrustedPublicKeys(db, type, repository_name)) {
		if (key == pem_key) {
			return true;
		}
	}
	return false;
}

TEST_CASE("Test that the public key of a trusted extension repository is trusted", "[api]") {
	auto repository_directory = TestCreatePath("extension_repositories");

	DBConfig config;
	config.SetOptionByName("extension_repository_directory", repository_directory);
	DuckDB db(nullptr, &config);
	Connection con(db);

	auto &instance = *db.instance;
	REQUIRE(!KeyIsTrusted(instance, ExtensionRepositoryType::USER_PROVIDED, "acme", TEST_PUBLIC_KEY));

	REQUIRE_NO_FAIL(con.Query(StringUtil::Format(
	    "CREATE EXTENSION REPOSITORY acme WITH PREFIX 'http://acme.com' USING PUBLIC KEY '%s'", TEST_PUBLIC_KEY)));
	REQUIRE(KeyIsTrusted(instance, ExtensionRepositoryType::USER_PROVIDED, "acme", TEST_PUBLIC_KEY));

	// the key of a user provided repository is never trusted for extensions that come from anywhere else
	REQUIRE(!KeyIsTrusted(instance, ExtensionRepositoryType::CORE, "acme", TEST_PUBLIC_KEY));
	REQUIRE(!KeyIsTrusted(instance, ExtensionRepositoryType::COMMUNITY, "acme", TEST_PUBLIC_KEY));

	// ... nor for extensions that come from a different user provided repository
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format(
	    "CREATE EXTENSION REPOSITORY other WITH PREFIX 'http://other.com' USING PUBLIC KEY '%s'", OTHER_PUBLIC_KEY)));
	REQUIRE(!KeyIsTrusted(instance, ExtensionRepositoryType::USER_PROVIDED, "other", TEST_PUBLIC_KEY));
	REQUIRE(KeyIsTrusted(instance, ExtensionRepositoryType::USER_PROVIDED, "other", OTHER_PUBLIC_KEY));
	REQUIRE(!KeyIsTrusted(instance, ExtensionRepositoryType::USER_PROVIDED, "acme", OTHER_PUBLIC_KEY));

	// an unknown repository name has no trusted keys at all
	REQUIRE(ExtensionHelper::GetTrustedPublicKeys(instance, ExtensionRepositoryType::USER_PROVIDED, "unknown").empty());

	REQUIRE_NO_FAIL(con.Query("DROP EXTENSION REPOSITORY acme"));
	REQUIRE(!KeyIsTrusted(instance, ExtensionRepositoryType::USER_PROVIDED, "acme", TEST_PUBLIC_KEY));
}

TEST_CASE("Test that the core and community signing keys are separate", "[api]") {
	DuckDB db(nullptr);

	auto core_keys = ExtensionHelper::GetTrustedPublicKeys(*db.instance, ExtensionRepositoryType::CORE, "core");
	auto community_keys =
	    ExtensionHelper::GetTrustedPublicKeys(*db.instance, ExtensionRepositoryType::COMMUNITY, "community");
	REQUIRE(!core_keys.empty());
	REQUIRE(!community_keys.empty());

	for (auto &community_key : community_keys) {
		for (auto &core_key : core_keys) {
			REQUIRE(core_key != community_key);
		}
	}
}

TEST_CASE("Test that trusted extension repositories can be disabled", "[api]") {
	auto repository_directory = TestCreatePath("extension_repositories_disabled");

	DBConfig config;
	config.SetOptionByName("extension_repository_directory", repository_directory);
	DuckDB db(nullptr, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query(StringUtil::Format(
	    "CREATE EXTENSION REPOSITORY acme WITH PREFIX 'http://acme.com' USING PUBLIC KEY '%s'", TEST_PUBLIC_KEY)));
	REQUIRE(KeyIsTrusted(*db.instance, ExtensionRepositoryType::USER_PROVIDED, "acme", TEST_PUBLIC_KEY));

	// after disabling trusted repositories the key of the repository is no longer trusted
	REQUIRE_NO_FAIL(con.Query("SET allow_extension_repositories=false"));
	REQUIRE(!KeyIsTrusted(*db.instance, ExtensionRepositoryType::USER_PROVIDED, "acme", TEST_PUBLIC_KEY));
}
