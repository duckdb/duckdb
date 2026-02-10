#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/common/encryption_key_manager.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

using namespace duckdb;
using namespace std;

bool VerifyWipe(const void *input_string, size_t string_size) {
	const volatile char *p = static_cast<const volatile char *>(input_string);
	for (size_t i = 0; i < string_size; ++i) {
		if (p[i] != 0) {
			return false;
		}
	}
	return true;
}

void GenerateSalt(uint8_t *salt) {
	// Similar to generate DB identifier
	memset(salt, 0, MainHeader::DB_IDENTIFIER_LEN);
	RandomEngine engine;
	engine.RandomData(salt, MainHeader::DB_IDENTIFIER_LEN);
}

TEST_CASE("Base64 input key derivation and wiping", "[encryption]") {
	// Mimics the encryption key flow
	// user key -> derived key -> key in encryption key cache

	DuckDB db;
	Connection con(db);
	auto &context = *con.context;

	auto &key_cache = EncryptionKeyManager::Get(context);
	std::string base64_key = "7nS+mQWj8vN2KzBfXzR4uL9kP1hT6mY3aI5oV8bC2uE=";
	uint8_t salt[MainHeader::DB_IDENTIFIER_LEN];
	GenerateSalt(salt);

	data_t derived_key[MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH];
	EncryptionKeyManager::DeriveKey(base64_key, salt, derived_key);

	key_cache.AddKey("base64_key", derived_key);

	// check if both keys are correctly wiped
	REQUIRE(VerifyWipe(base64_key.data(), base64_key.size()) == true);
	REQUIRE(VerifyWipe(derived_key, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH) == true);
}

TEST_CASE("Delete encryption key from cache", "[encryption]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &key_cache = EncryptionKeyManager::Get(context);

	// mimics a derived 32-byte key
	std::string derived_key = "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6";

	key_cache.AddKey("test_key", data_ptr_t(derived_key.data()));
	REQUIRE(VerifyWipe(derived_key.data(), derived_key.size()) == true);

	// DeleteKey() (contains ClearKey() and EraseKey()), we thus erase the value
	// In that case, we cannot test whether the memory is zeroed out
	auto key_data_ptr = key_cache.GetKey("test_key");
	key_cache.ClearKey("test_key");
	REQUIRE(VerifyWipe(key_data_ptr, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH) == true);
}

TEST_CASE("EncryptionEngine key management testing", "[encryption]") {
	// Wrapper functions for key management
	DuckDB db;
	Connection con(db);
	EncryptionEngine engine;

	// mimics a derived 32-byte key
	std::string derived_key = "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6";
	engine.AddKeyToCache(*db.instance, data_ptr_t(derived_key.data()), "engine_key");
	REQUIRE(VerifyWipe(derived_key.data(), derived_key.size()) == true);

	// try to add an encryption key with the same name
	std::string second_derived_key = "bbbbb3d4e5f6g7h8i9j0k1l2m3n4o5p6";
	engine.AddKeyToCache(*db.instance, data_ptr_t(second_derived_key.data()), "engine_key");
	REQUIRE(VerifyWipe(second_derived_key.data(), second_derived_key.size()) == true);
}
