#include "test_capi_v2.hpp"

#include <cstring>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 crypto tests: the primitives on the built-in provider, and a provider of our own.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

using Bytes = std::vector<uint8_t>;

Bytes ToBytes(const std::string &str) {
	return Bytes(str.begin(), str.end());
}

std::string ToHex(const uint8_t *data, size_t length) {
	static const char *HEX = "0123456789abcdef";
	std::string result;
	for (size_t i = 0; i < length; i++) {
		result.push_back(HEX[data[i] >> 4]);
		result.push_back(HEX[data[i] & 0xf]);
	}
	return result;
}

const Bytes KEY_128(16, 0x42);
const Bytes KEY_256(32, 0x24);

// Runs the whole stream and returns the output, leaving the tag in `tag` (GCM).
DUCKDB_V2_ERROR RunAes(duckdb_v2_instance_handle instance, DUCKDB_V2_CRYPTO_CIPHER cipher, DUCKDB_V2_CRYPTO_MODE mode,
                       const Bytes &key, const Bytes &iv, const Bytes &aad, const Bytes &input, Bytes &output,
                       Bytes &tag) {
	duckdb_v2_crypto_cipher_handle stream = nullptr;
	auto rc = duckdb_v2_crypto_cipher_create(instance, cipher, mode, key.data(), key.size(), &stream, nullptr);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return rc;
	}
	rc = duckdb_v2_crypto_cipher_init(stream, iv.data(), iv.size(), aad.empty() ? nullptr : aad.data(), aad.size(),
	                                  nullptr);
	if (rc == DUCKDB_V2_ERROR_NONE) {
		output.assign(input.size(), 0);
		idx_t written = 0;
		rc = duckdb_v2_crypto_cipher_update(stream, input.data(), input.size(), output.data(), output.size(), &written,
		                                    nullptr);
		output.resize(written);
	}
	if (rc == DUCKDB_V2_ERROR_NONE) {
		if (cipher == DUCKDB_V2_CRYPTO_CIPHER_AES_GCM) {
			tag.resize(16, 0);
			rc = duckdb_v2_crypto_cipher_finalize(stream, tag.data(), tag.size(), nullptr);
		} else {
			rc = duckdb_v2_crypto_cipher_finalize(stream, nullptr, 0, nullptr);
		}
	}
	duckdb_v2_crypto_cipher_destroy(&stream);
	return rc;
}

std::string Hash(duckdb_v2_instance_handle instance, DUCKDB_V2_CRYPTO_HASH_FUNCTION function, const Bytes &data,
                 DUCKDB_V2_ERROR *rc_out = nullptr) {
	uint8_t digest[32];
	idx_t length = 0;
	auto rc =
	    duckdb_v2_crypto_hash(instance, function, data.data(), data.size(), digest, sizeof(digest), &length, nullptr);
	if (rc_out) {
		*rc_out = rc;
	}
	return rc == DUCKDB_V2_ERROR_NONE ? ToHex(digest, length) : std::string();
}

std::string Hmac(duckdb_v2_instance_handle instance, DUCKDB_V2_CRYPTO_HASH_FUNCTION function, const Bytes &key,
                 const Bytes &data) {
	uint8_t mac[32];
	idx_t length = 0;
	REQUIRE(duckdb_v2_crypto_hmac(instance, function, key.data(), key.size(), data.data(), data.size(), mac,
	                              sizeof(mac), &length, nullptr) == DUCKDB_V2_ERROR_NONE);
	return ToHex(mac, length);
}

// Selects the built-in provider for writing, as there is no secure provider in the test binary.
void AllowBuiltInWrites(EnvFixture &fx) {
	REQUIRE(RunSql(fx.conn, "SET force_mbedtls_unsafe = true") == DUCKDB_V2_ERROR_NONE);
}

} // namespace

TEST_CASE("V2 crypto: hashing and HMAC", "[capi_v2][crypto]") {
	EnvFixture fx;
	auto abc = ToBytes("abc");
	REQUIRE(Hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, abc) ==
	        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
	REQUIRE(Hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA1, abc) == "a9993e364706816aba3e25717850c26c9cd0d89d");
	REQUIRE(Hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_MD5, abc) == "900150983cd24fb0d6963f7d28e17f72");
	// The empty input needs no data pointer.
	uint8_t digest[32];
	idx_t length = 0;
	REQUIRE(duckdb_v2_crypto_hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, nullptr, 0, digest, sizeof(digest),
	                              &length, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ToHex(digest, length) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

	// RFC 4231, test case 2.
	REQUIRE(Hmac(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, ToBytes("Jefe"), ToBytes("what do ya want for nothing?")) ==
	        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");

	// Digest sizes, and support.
	idx_t size = 0;
	REQUIRE(duckdb_v2_crypto_hash_digest_size(DUCKDB_V2_CRYPTO_HASH_MD5, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 16);
	REQUIRE(duckdb_v2_crypto_hash_digest_size(DUCKDB_V2_CRYPTO_HASH_SHA1, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 20);
	REQUIRE(duckdb_v2_crypto_hash_digest_size(DUCKDB_V2_CRYPTO_HASH_SHA256, &size, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(size == 32);
	REQUIRE(duckdb_v2_crypto_hash_digest_size(static_cast<DUCKDB_V2_CRYPTO_HASH_FUNCTION>(99), &size, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	bool supported = false;
	REQUIRE(duckdb_v2_crypto_supports_hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, &supported, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(supported);
	REQUIRE(duckdb_v2_crypto_supports_hmac(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, &supported, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(supported);

	// A digest buffer that is too small, and null arguments.
	REQUIRE(duckdb_v2_crypto_hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, abc.data(), abc.size(), digest, 31,
	                              &length, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_hash(nullptr, DUCKDB_V2_CRYPTO_HASH_SHA256, abc.data(), abc.size(), digest, sizeof(digest),
	                              &length, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, nullptr, 3, digest, sizeof(digest),
	                              &length, nullptr) != DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 crypto: AES-GCM round trip with AAD", "[capi_v2][crypto]") {
	EnvFixture fx;
	AllowBuiltInWrites(fx);
	Bytes iv(12, 7);
	auto aad = ToBytes("header");
	auto plaintext = ToBytes("the quick brown fox jumps over the lazy dog");

	for (auto &key : {KEY_128, KEY_256}) {
		Bytes ciphertext, tag;
		REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_ENCRYPT, key, iv, aad,
		               plaintext, ciphertext, tag) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(ciphertext.size() == plaintext.size());
		REQUIRE(ciphertext != plaintext);

		Bytes decrypted, unused;
		auto tag_copy = tag;
		REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_DECRYPT, key, iv, aad,
		               ciphertext, decrypted, tag_copy) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(decrypted == plaintext);

		// A different AAD, or a tampered tag, fails authentication.
		tag_copy = tag;
		REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_DECRYPT, key, iv,
		               ToBytes("other!"), ciphertext, decrypted, tag_copy) != DUCKDB_V2_ERROR_NONE);
		tag_copy = tag;
		tag_copy[0] ^= 1;
		REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_DECRYPT, key, iv, aad,
		               ciphertext, decrypted, tag_copy) != DUCKDB_V2_ERROR_NONE);
	}
}

TEST_CASE("V2 crypto: AES-CTR round trip, in-place and in chunks", "[capi_v2][crypto]") {
	EnvFixture fx;
	AllowBuiltInWrites(fx);
	Bytes iv(16, 3);
	auto plaintext = ToBytes("counter mode needs no tag and no padding");
	Bytes ciphertext, tag;
	REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_CTR, DUCKDB_V2_CRYPTO_MODE_ENCRYPT, KEY_128, iv, {},
	               plaintext, ciphertext, tag) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ciphertext != plaintext);

	// Decrypt in two chunks, in place.
	duckdb_v2_crypto_cipher_handle stream = nullptr;
	REQUIRE(duckdb_v2_crypto_cipher_create(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_CTR, DUCKDB_V2_CRYPTO_MODE_DECRYPT,
	                                       KEY_128.data(), KEY_128.size(), &stream, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_cipher_init(stream, iv.data(), iv.size(), nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto buffer = ciphertext;
	idx_t first = 0, second = 0;
	REQUIRE(duckdb_v2_crypto_cipher_update(stream, buffer.data(), 10, buffer.data(), 10, &first, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_cipher_update(stream, buffer.data() + 10, buffer.size() - 10, buffer.data() + 10,
	                                       buffer.size() - 10, &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(first + second == buffer.size());
	REQUIRE(duckdb_v2_crypto_cipher_finalize(stream, nullptr, 0, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(buffer == plaintext);
	duckdb_v2_crypto_cipher_destroy(&stream);
	REQUIRE(stream == nullptr);
	REQUIRE(duckdb_v2_crypto_cipher_destroy(&stream) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 crypto: invalid AES usage is reported", "[capi_v2][crypto]") {
	EnvFixture fx;
	AllowBuiltInWrites(fx);
	duckdb_v2_crypto_cipher_handle stream = nullptr;
	Bytes bad_key(15, 1);
	REQUIRE(duckdb_v2_crypto_cipher_create(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_ENCRYPT,
	                                       bad_key.data(), bad_key.size(), &stream, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(stream == nullptr);

	REQUIRE(duckdb_v2_crypto_cipher_create(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_ENCRYPT,
	                                       KEY_128.data(), KEY_128.size(), &stream, nullptr) == DUCKDB_V2_ERROR_NONE);
	Bytes buffer(16, 0);
	idx_t written = 0;
	// Updating before initializing.
	REQUIRE(duckdb_v2_crypto_cipher_update(stream, buffer.data(), 16, buffer.data(), 16, &written, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	// A CTR-sized IV for GCM.
	Bytes iv16(16, 0);
	REQUIRE(duckdb_v2_crypto_cipher_init(stream, iv16.data(), iv16.size(), nullptr, 0, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	Bytes iv12(12, 0);
	REQUIRE(duckdb_v2_crypto_cipher_init(stream, iv12.data(), iv12.size(), nullptr, 0, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_cipher_init(stream, iv12.data(), iv12.size(), nullptr, 0, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	// An output buffer smaller than the input.
	REQUIRE(duckdb_v2_crypto_cipher_update(stream, buffer.data(), 16, buffer.data(), 8, &written, nullptr) !=
	        DUCKDB_V2_ERROR_NONE);
	// A tag of the wrong size.
	REQUIRE(duckdb_v2_crypto_cipher_finalize(stream, buffer.data(), 8, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_cipher_finalize(stream, buffer.data(), 16, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_cipher_finalize(stream, buffer.data(), 16, nullptr) != DUCKDB_V2_ERROR_NONE);
	duckdb_v2_crypto_cipher_destroy(&stream);

	// Without a secure provider, encrypting fails but decrypting works.
	EnvFixture other;
	REQUIRE(duckdb_v2_crypto_cipher_create(other.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM,
	                                       DUCKDB_V2_CRYPTO_MODE_ENCRYPT, KEY_128.data(), KEY_128.size(), &stream,
	                                       nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_cipher_create(other.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM,
	                                       DUCKDB_V2_CRYPTO_MODE_DECRYPT, KEY_128.data(), KEY_128.size(), &stream,
	                                       nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_crypto_cipher_destroy(&stream);
}

//----------------------------------------------------------------------------------------------------------------------
// A provider of our own: a toy XOR cipher, so the results are recognizable.
//----------------------------------------------------------------------------------------------------------------------
namespace {

struct ProviderStats {
	idx_t creates = 0;
	idx_t destroys = 0;
	idx_t hash_calls = 0;
	idx_t hmac_calls = 0;
	idx_t random_calls = 0;
	DUCKDB_V2_CRYPTO_MODE last_mode = DUCKDB_V2_CRYPTO_MODE_ENCRYPT;
	DUCKDB_V2_CRYPTO_CIPHER last_cipher = DUCKDB_V2_CRYPTO_CIPHER_AES_GCM;
	idx_t last_aad_length = 0;
	bool *destroyed = nullptr;
};

struct XorStream {
	DUCKDB_V2_CRYPTO_MODE mode;
	uint8_t key = 0;
	uint8_t iv = 0;
};

void DestroyStats(void *ptr) {
	auto stats = static_cast<ProviderStats *>(ptr);
	if (stats->destroyed) {
		*stats->destroyed = true;
	}
	delete stats;
}

void XorCreate(void *user_data, DUCKDB_V2_CRYPTO_CIPHER cipher, DUCKDB_V2_CRYPTO_MODE mode, const uint8_t *key, idx_t,
               void **stream, duckdb_v2_error_info_handle *) {
	auto &stats = *static_cast<ProviderStats *>(user_data);
	stats.creates++;
	stats.last_mode = mode;
	stats.last_cipher = cipher;
	auto xor_stream = new XorStream();
	xor_stream->mode = mode;
	xor_stream->key = key[0];
	*stream = xor_stream;
}

void XorInit(void *user_data, void *stream, const uint8_t *iv, idx_t, const uint8_t *, idx_t aad_length,
             duckdb_v2_error_info_handle *) {
	static_cast<ProviderStats *>(user_data)->last_aad_length = aad_length;
	static_cast<XorStream *>(stream)->iv = iv[0];
}

void XorUpdate(void *, void *stream, const uint8_t *in, idx_t in_length, uint8_t *out, idx_t, idx_t *out_length,
               duckdb_v2_error_info_handle *) {
	auto &xor_stream = *static_cast<XorStream *>(stream);
	for (idx_t i = 0; i < in_length; i++) {
		out[i] = in[i] ^ xor_stream.key ^ xor_stream.iv;
	}
	*out_length = in_length;
}

void XorFinalize(void *, void *stream, uint8_t *tag, idx_t tag_length, duckdb_v2_error_info_handle *err) {
	auto &xor_stream = *static_cast<XorStream *>(stream);
	for (idx_t i = 0; i < tag_length; i++) {
		if (xor_stream.mode == DUCKDB_V2_CRYPTO_MODE_ENCRYPT) {
			tag[i] = 0xAB;
		} else if (tag[i] != 0xAB) {
			duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
			return;
		}
	}
}

void XorDestroy(void *user_data, void *stream) {
	static_cast<ProviderStats *>(user_data)->destroys++;
	delete static_cast<XorStream *>(stream);
}

// Serves SHA-256 only, with a recognizable digest.
void FixedHash(void *user_data, DUCKDB_V2_CRYPTO_HASH_FUNCTION function, const uint8_t *, idx_t, uint8_t *out,
               duckdb_v2_error_info_handle *) {
	static_cast<ProviderStats *>(user_data)->hash_calls++;
	idx_t size = 0;
	duckdb_v2_crypto_hash_digest_size(function, &size, nullptr);
	memset(out, 0x11, size);
}

// Serves HMAC-SHA1 only
void FixedHmac(void *user_data, DUCKDB_V2_CRYPTO_HASH_FUNCTION function, const uint8_t *, idx_t, const uint8_t *, idx_t,
               uint8_t *out, duckdb_v2_error_info_handle *) {
	static_cast<ProviderStats *>(user_data)->hmac_calls++;
	idx_t size = 0;
	duckdb_v2_crypto_hash_digest_size(function, &size, nullptr);
	memset(out, 0x22, size);
}

void FixedRandom(void *user_data, uint8_t *out, idx_t length, duckdb_v2_error_info_handle *) {
	static_cast<ProviderStats *>(user_data)->random_calls++;
	memset(out, 0x5A, length);
}

duckdb_v2_crypto_provider_handle MakeProvider(duckdb_v2_factory_handle factory, ProviderStats *stats,
                                              bool with_aes = true) {
	duckdb_v2_crypto_provider_handle provider = nullptr;
	REQUIRE(duckdb_v2_crypto_provider_create(factory, &provider, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_opaque data = {stats, DestroyStats, nullptr};
	REQUIRE(duckdb_v2_crypto_provider_set_user_data(provider, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (with_aes) {
		REQUIRE(duckdb_v2_crypto_provider_set_cipher_create_callback(provider, XorCreate, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_crypto_provider_set_cipher_init_callback(provider, XorInit, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_crypto_provider_set_cipher_update_callback(provider, XorUpdate, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_crypto_provider_set_cipher_finalize_callback(provider, XorFinalize, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_crypto_provider_set_cipher_destroy_callback(provider, XorDestroy, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
	}
	DUCKDB_V2_CRYPTO_HASH_FUNCTION hashes[] = {DUCKDB_V2_CRYPTO_HASH_SHA256};
	REQUIRE(duckdb_v2_crypto_provider_set_hash_callback(provider, FixedHash, hashes, 1, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	DUCKDB_V2_CRYPTO_HASH_FUNCTION hmacs[] = {DUCKDB_V2_CRYPTO_HASH_SHA1};
	REQUIRE(duckdb_v2_crypto_provider_set_hmac_callback(provider, FixedHmac, hmacs, 1, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_crypto_provider_set_random_callback(provider, FixedRandom, nullptr) == DUCKDB_V2_ERROR_NONE);
	return provider;
}

} // namespace

TEST_CASE("V2 crypto provider: the primitives run on a registered provider", "[capi_v2][crypto]") {
	EnvFixture fx;
	auto stats = new ProviderStats();
	auto provider = MakeProvider(fx.factory, stats);
	REQUIRE(duckdb_v2_instance_register_crypto_provider(fx.instance, provider, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_crypto_provider_destroy(&provider);

	// Encrypting needs no force setting: the registered provider is the secure one.
	Bytes iv(12, 5), aad = ToBytes("aad");
	auto plaintext = ToBytes("hello");
	Bytes ciphertext, tag;
	REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_ENCRYPT, KEY_128, iv, aad,
	               plaintext, ciphertext, tag) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stats->creates == 1);
	REQUIRE(stats->last_mode == DUCKDB_V2_CRYPTO_MODE_ENCRYPT);
	REQUIRE(stats->last_cipher == DUCKDB_V2_CRYPTO_CIPHER_AES_GCM);
	REQUIRE(stats->last_aad_length == aad.size());
	REQUIRE(ciphertext[0] == (uint8_t('h') ^ 0x42 ^ 5));
	REQUIRE(tag == Bytes(16, 0xAB));

	Bytes decrypted, tag_copy = tag;
	REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_DECRYPT, KEY_128, iv, aad,
	               ciphertext, decrypted, tag_copy) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(decrypted == plaintext);

	// An error the callback reports surfaces from finalize.
	tag_copy[3] = 0;
	REQUIRE(RunAes(fx.instance, DUCKDB_V2_CRYPTO_CIPHER_AES_GCM, DUCKDB_V2_CRYPTO_MODE_DECRYPT, KEY_128, iv, aad,
	               ciphertext, decrypted, tag_copy) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(stats->destroys == stats->creates);

	// Functions the hash callback declares go to it, the others fall back to the built-in implementation.
	REQUIRE(Hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, ToBytes("abc")) == std::string(64, '1'));
	REQUIRE(stats->hash_calls == 1);
	REQUIRE(Hash(fx.instance, DUCKDB_V2_CRYPTO_HASH_MD5, ToBytes("abc")) == "900150983cd24fb0d6963f7d28e17f72");
	REQUIRE(stats->hash_calls == 1);
	REQUIRE(Hmac(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA1, ToBytes("k"), ToBytes("d")) == std::string(40, '2'));
	REQUIRE(stats->hmac_calls == 1);
	REQUIRE(Hmac(fx.instance, DUCKDB_V2_CRYPTO_HASH_SHA256, ToBytes("Jefe"), ToBytes("what do ya want for nothing?")) ==
	        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
	REQUIRE(stats->hmac_calls == 1);
	uint8_t random[8];
	REQUIRE(duckdb_v2_crypto_random_bytes(fx.instance, random, sizeof(random), nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ToHex(random, 8) == "5a5a5a5a5a5a5a5a");
	REQUIRE(stats->random_calls == 1);

	// SQL goes through the provider as well.
	REQUIRE(RunSql(fx.conn, "SELECT sha256('abc')") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(stats->hash_calls == 2);
}

TEST_CASE("V2 crypto provider: registration rules", "[capi_v2][crypto]") {
	EnvFixture fx;

	// The AES callbacks are required.
	auto incomplete = MakeProvider(fx.factory, new ProviderStats(), false);
	REQUIRE(duckdb_v2_instance_register_crypto_provider(fx.instance, incomplete, nullptr) != DUCKDB_V2_ERROR_NONE);
	duckdb_v2_crypto_provider_destroy(&incomplete);

	// A provider belongs to the instance of its factory.
	EnvFixture other;
	auto foreign = MakeProvider(other.factory, new ProviderStats());
	REQUIRE(duckdb_v2_instance_register_crypto_provider(fx.instance, foreign, nullptr) != DUCKDB_V2_ERROR_NONE);
	duckdb_v2_crypto_provider_destroy(&foreign);

	// Only one provider can be registered.
	bool destroyed_first = false, destroyed_second = false;
	auto first_stats = new ProviderStats();
	first_stats->destroyed = &destroyed_first;
	auto second_stats = new ProviderStats();
	second_stats->destroyed = &destroyed_second;
	auto first = MakeProvider(fx.factory, first_stats);
	auto second = MakeProvider(fx.factory, second_stats);
	REQUIRE(duckdb_v2_instance_register_crypto_provider(fx.instance, first, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_register_crypto_provider(fx.instance, second, nullptr) != DUCKDB_V2_ERROR_NONE);
	duckdb_v2_crypto_provider_destroy(&first);
	duckdb_v2_crypto_provider_destroy(&second);

	// The unregistered provider frees its user data with the handle, the registered one with the instance.
	REQUIRE(destroyed_second);
	REQUIRE_FALSE(destroyed_first);
	// (the fixture of `fx` is destroyed at the end of the scope)
}

} // namespace test_capi_v2
