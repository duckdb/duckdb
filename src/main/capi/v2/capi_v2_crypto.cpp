#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/encryption_state.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb::capiv2 {

//----------------------------------------------------------------------------------------------------------------------
// Provider callbacks
//----------------------------------------------------------------------------------------------------------------------
//! Runs a provider callback, rethrowing the error it reports as the matching exception.
template <class FUNC>
static void InvokeCallback(FUNC &&func) {
	CV2ErrorInfo info;
	auto handle = Convert(&info);
	func(&handle);
	if (info.HasError()) {
		info.ThrowAsException();
	}
}

static uint32_t HashFunctionBit(CryptoHashFunction function) {
	return 1U << static_cast<uint8_t>(function);
}

static CryptoHashFunction ConvertHashFunction(DUCKDB_V2_CRYPTO_HASH_FUNCTION function) {
	switch (function) {
	case DUCKDB_V2_CRYPTO_HASH_MD5:
		return CryptoHashFunction::MD5;
	case DUCKDB_V2_CRYPTO_HASH_SHA1:
		return CryptoHashFunction::SHA1;
	case DUCKDB_V2_CRYPTO_HASH_SHA256:
		return CryptoHashFunction::SHA256;
	default:
		throw InvalidInputException("Unknown hash function");
	}
}

struct CV2CryptoCallbacks {
	duckdb_v2_crypto_provider_aes_create_callback_fn aes_create = nullptr;
	duckdb_v2_crypto_provider_aes_init_callback_fn aes_init = nullptr;
	duckdb_v2_crypto_provider_aes_update_callback_fn aes_update = nullptr;
	duckdb_v2_crypto_provider_aes_finalize_callback_fn aes_finalize = nullptr;
	duckdb_v2_crypto_provider_aes_destroy_callback_fn aes_destroy = nullptr;
	duckdb_v2_crypto_provider_hash_callback_fn hash = nullptr;
	duckdb_v2_crypto_provider_hmac_callback_fn hmac = nullptr;
	//! Bit masks of the CryptoHashFunction values the callbacks serve
	uint32_t hash_functions = 0;
	uint32_t hmac_functions = 0;
	duckdb_v2_crypto_provider_random_callback_fn random = nullptr;
};

//! The AES stream of a provider: created on initialization, once key, direction and IV are known
class CV2ProviderEncryptionState final : public EncryptionState {
public:
	CV2ProviderEncryptionState(unique_ptr<EncryptionStateMetadata> metadata, const CV2CryptoCallbacks &callbacks,
	                           shared_ptr<CV2UserData> user_data, shared_ptr<EncryptionState> fallback)
	    : EncryptionState(std::move(metadata)), callbacks(callbacks), user_data(std::move(user_data)),
	      fallback(std::move(fallback)) {
	}

	~CV2ProviderEncryptionState() override {
		if (stream && callbacks.aes_destroy) {
			callbacks.aes_destroy(GetUserData(), stream);
		}
	}

	void InitializeEncryption(EncryptionNonce &nonce, const_data_ptr_t key, const_data_ptr_t aad,
	                          idx_t aad_len) override {
		Initialize(DUCKDB_V2_CRYPTO_MODE_ENCRYPT, nonce, key, aad, aad_len);
	}

	void InitializeDecryption(EncryptionNonce &nonce, const_data_ptr_t key, const_data_ptr_t aad,
	                          idx_t aad_len) override {
		Initialize(DUCKDB_V2_CRYPTO_MODE_DECRYPT, nonce, key, aad, aad_len);
	}

	size_t Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len) override {
		CheckInitialized();
		idx_t written = 0;
		InvokeCallback([&](duckdb_v2_error_info_handle *err) {
			callbacks.aes_update(GetUserData(), stream, in, in_len, out, out_len, &written, err);
		});
		return written;
	}

	size_t Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len) override {
		CheckInitialized();
		auto is_gcm = GetCipher() == EncryptionTypes::GCM;
		InvokeCallback([&](duckdb_v2_error_info_handle *err) {
			callbacks.aes_finalize(GetUserData(), stream, is_gcm ? tag : nullptr, is_gcm ? tag_len : 0, err);
		});
		return 0;
	}

	void GenerateRandomData(data_ptr_t data, idx_t len) override {
		if (!callbacks.random) {
			fallback->GenerateRandomData(data, len);
			return;
		}
		InvokeCallback([&](duckdb_v2_error_info_handle *err) { callbacks.random(GetUserData(), data, len, err); });
	}

private:
	void *GetUserData() const {
		return user_data ? user_data->GetData() : nullptr;
	}

	void CheckInitialized() const {
		if (!stream) {
			throw InvalidInputException("The AES stream has not been initialized");
		}
	}

	void Initialize(DUCKDB_V2_CRYPTO_MODE mode, EncryptionNonce &nonce, const_data_ptr_t key, const_data_ptr_t aad,
	                idx_t aad_len) {
		if (stream) {
			throw InvalidInputException("The AES stream has already been initialized");
		}
		auto cipher = static_cast<DUCKDB_V2_CRYPTO_CIPHER>(GetCipher());
		InvokeCallback([&](duckdb_v2_error_info_handle *err) {
			callbacks.aes_create(GetUserData(), cipher, mode, key, metadata->GetKeyLen(), &stream, err);
		});
		InvokeCallback([&](duckdb_v2_error_info_handle *err) {
			callbacks.aes_init(GetUserData(), stream, nonce.data(), nonce.total_size(), aad, aad_len, err);
		});
	}

private:
	CV2CryptoCallbacks callbacks;
	shared_ptr<CV2UserData> user_data;
	//! The built-in state, for what the provider does not implement
	shared_ptr<EncryptionState> fallback;
	void *stream = nullptr;
};

//! The crypto provider supplied through the C API. Hashing, HMAC and random bytes fall back to the built-in provider.
class CV2EncryptionUtil final : public EncryptionUtil {
public:
	CV2EncryptionUtil(const CV2CryptoCallbacks &callbacks, shared_ptr<CV2UserData> user_data,
	                  shared_ptr<EncryptionUtil> fallback)
	    : callbacks(callbacks), user_data(std::move(user_data)), fallback(std::move(fallback)) {
	}

	shared_ptr<EncryptionState> CreateEncryptionState(unique_ptr<EncryptionStateMetadata> metadata) const override {
		auto fallback_state = fallback->CreateEncryptionState(
		    make_uniq<EncryptionStateMetadata>(metadata->GetCipher(), metadata->GetKeyLen(), metadata->GetVersion()));
		return make_shared_ptr<CV2ProviderEncryptionState>(std::move(metadata), callbacks, user_data,
		                                                   std::move(fallback_state));
	}

	void Hash(CryptoHashFunction function, const_data_ptr_t input, idx_t input_len, data_ptr_t output) const override {
		if (!callbacks.hash || !(callbacks.hash_functions & HashFunctionBit(function))) {
			fallback->Hash(function, input, input_len, output);
			return;
		}
		auto c_function = static_cast<DUCKDB_V2_CRYPTO_HASH_FUNCTION>(function);
		InvokeCallback([&](duckdb_v2_error_info_handle *err) {
			callbacks.hash(GetUserData(), c_function, input, input_len, output, err);
		});
	}

	void Hmac(CryptoHashFunction function, const_data_ptr_t key, idx_t key_len, const_data_ptr_t input, idx_t input_len,
	          data_ptr_t output) const override {
		if (!callbacks.hmac || !(callbacks.hmac_functions & HashFunctionBit(function))) {
			fallback->Hmac(function, key, key_len, input, input_len, output);
			return;
		}
		auto c_function = static_cast<DUCKDB_V2_CRYPTO_HASH_FUNCTION>(function);
		InvokeCallback([&](duckdb_v2_error_info_handle *err) {
			callbacks.hmac(GetUserData(), c_function, key, key_len, input, input_len, output, err);
		});
	}

	bool SupportsHash(CryptoHashFunction function) const override {
		return (callbacks.hash && (callbacks.hash_functions & HashFunctionBit(function))) ||
		       fallback->SupportsHash(function);
	}

	bool SupportsHmac(CryptoHashFunction function) const override {
		return (callbacks.hmac && (callbacks.hmac_functions & HashFunctionBit(function))) ||
		       fallback->SupportsHmac(function);
	}

private:
	void *GetUserData() const {
		return user_data ? user_data->GetData() : nullptr;
	}

private:
	CV2CryptoCallbacks callbacks;
	shared_ptr<CV2UserData> user_data;
	shared_ptr<EncryptionUtil> fallback;
};

class CV2CryptoProvider {
public:
	explicit CV2CryptoProvider(DatabaseInstance &db) : db(db) {
	}

	void Register(DatabaseInstance &target) {
		CheckRegistrationTarget(db, target, "crypto provider");
		if (!callbacks.aes_create || !callbacks.aes_init || !callbacks.aes_update || !callbacks.aes_finalize) {
			throw InvalidInputException("The AES callbacks must be set for the crypto provider.");
		}
		auto &config = DBConfig::GetConfig(target);
		if (config.encryption_util) {
			throw InvalidInputException("A crypto provider is already registered on this database.");
		}
		config.encryption_util = make_shared_ptr<CV2EncryptionUtil>(callbacks, user_data, target.GetMbedTLSUtil(false));
	}

public:
	//! The database it was created for: the only one it can be registered on.
	DatabaseInstance &db;
	CV2CryptoCallbacks callbacks;
	shared_ptr<CV2UserData> user_data = nullptr;
};

static auto Convert(duckdb_v2_crypto_provider_handle provider) -> CV2CryptoProvider * {
	return reinterpret_cast<CV2CryptoProvider *>(provider);
}
static auto Convert(CV2CryptoProvider *provider) -> duckdb_v2_crypto_provider_handle {
	return reinterpret_cast<duckdb_v2_crypto_provider_handle>(provider);
}

//----------------------------------------------------------------------------------------------------------------------
// Primitives
//----------------------------------------------------------------------------------------------------------------------
//! A streaming AES cipher on the instance's crypto provider
class CV2Aes {
public:
	CV2Aes(DatabaseInstance &db, DUCKDB_V2_CRYPTO_CIPHER cipher_p, DUCKDB_V2_CRYPTO_MODE mode_p, const uint8_t *key_p,
	       idx_t key_length)
	    : mode(mode_p) {
		switch (cipher_p) {
		case DUCKDB_V2_CRYPTO_CIPHER_AES_GCM:
			cipher = EncryptionTypes::GCM;
			break;
		case DUCKDB_V2_CRYPTO_CIPHER_AES_CTR:
			cipher = EncryptionTypes::CTR;
			break;
		default:
			throw InvalidInputException("Unknown AES cipher");
		}
		if (mode != DUCKDB_V2_CRYPTO_MODE_ENCRYPT && mode != DUCKDB_V2_CRYPTO_MODE_DECRYPT) {
			throw InvalidInputException("Unknown AES mode");
		}
		if (key_length != 16 && key_length != 24 && key_length != 32) {
			throw InvalidInputException("AES keys must be 16, 24 or 32 bytes, got %llu",
			                            static_cast<uint64_t>(key_length));
		}
		// writing needs a secure provider, reading falls back to the built-in one
		auto util = db.GetEncryptionUtil(mode == DUCKDB_V2_CRYPTO_MODE_DECRYPT);
		state = util->CreateEncryptionState(
		    make_uniq<EncryptionStateMetadata>(cipher, key_length, EncryptionTypes::EncryptionVersion::V0_1));
		key.assign(reinterpret_cast<const char *>(key_p), key_length);
	}

	~CV2Aes() {
		// volatile writes so the wipe is not optimized away
		volatile char *volatile_key = key.empty() ? nullptr : &key[0];
		for (idx_t i = 0; i < key.size(); i++) {
			volatile_key[i] = 0;
		}
	}

	void Init(const uint8_t *iv, idx_t iv_length, const uint8_t *aad, idx_t aad_length) {
		if (initialized) {
			throw InvalidInputException("The AES cipher has already been initialized");
		}
		auto expected_iv_length = cipher == EncryptionTypes::GCM ? 12 : 16;
		if (iv_length != expected_iv_length) {
			throw InvalidInputException("The IV must be %llu bytes for %s, got %llu",
			                            static_cast<uint64_t>(expected_iv_length),
			                            EncryptionTypes::CipherToString(cipher), static_cast<uint64_t>(iv_length));
		}
		if (aad_length > 0 && (cipher != EncryptionTypes::GCM || !aad)) {
			throw InvalidInputException("Additional authenticated data is only valid for GCM");
		}
		EncryptionNonce nonce(cipher, EncryptionTypes::EncryptionVersion::V0_1);
		memcpy(nonce.data(), iv, iv_length);
		auto key_ptr = const_data_ptr_cast(key.data());
		if (mode == DUCKDB_V2_CRYPTO_MODE_ENCRYPT) {
			state->InitializeEncryption(nonce, key_ptr, aad, aad_length);
		} else {
			state->InitializeDecryption(nonce, key_ptr, aad, aad_length);
		}
		initialized = true;
	}

	idx_t Update(const uint8_t *in, idx_t in_length, uint8_t *out, idx_t out_capacity) {
		CheckActive();
		if (out_capacity < in_length) {
			throw Exception(ExceptionType::OBJECT_SIZE, "out_capacity must be at least in_length");
		}
		return state->Process(in, in_length, out, out_capacity);
	}

	void Finalize(uint8_t *tag, idx_t tag_length) {
		CheckActive();
		auto expected_tag_length = cipher == EncryptionTypes::GCM ? 16 : 0;
		if (tag_length != expected_tag_length || (tag_length > 0 && !tag)) {
			throw InvalidInputException("The tag must be %llu bytes for %s", static_cast<uint64_t>(expected_tag_length),
			                            EncryptionTypes::CipherToString(cipher));
		}
		// ciphers may flush a final block, which for these stream modes is always empty
		data_t scratch[32];
		state->Finalize(scratch, sizeof(scratch), tag, tag_length);
		finalized = true;
	}

private:
	void CheckActive() const {
		if (!initialized) {
			throw InvalidInputException("The AES cipher has not been initialized");
		}
		if (finalized) {
			throw InvalidInputException("The AES cipher has already been finalized");
		}
	}

private:
	EncryptionTypes::CipherType cipher;
	DUCKDB_V2_CRYPTO_MODE mode;
	string key;
	shared_ptr<EncryptionState> state;
	bool initialized = false;
	bool finalized = false;
};

static auto Convert(duckdb_v2_crypto_aes_handle aes) -> CV2Aes * {
	return reinterpret_cast<CV2Aes *>(aes);
}
static auto Convert(CV2Aes *aes) -> duckdb_v2_crypto_aes_handle {
	return reinterpret_cast<duckdb_v2_crypto_aes_handle>(aes);
}

static DatabaseInstance &GetInstance(duckdb_v2_instance_handle instance) {
	return *Convert(instance)->GetDatabase().instance;
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_crypto_hash_digest_size(DUCKDB_V2_CRYPTO_HASH_FUNCTION function, idx_t *size,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(size);
	return WithErrorHandler(err, [&]() { *size = duckdb::CryptoHash::GetDigestSize(ConvertHashFunction(function)); });
}

DUCKDB_V2_ERROR duckdb_v2_crypto_supports_hash(duckdb_v2_instance_handle instance,
                                               DUCKDB_V2_CRYPTO_HASH_FUNCTION function, bool *supported,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(supported);
	return WithErrorHandler(err, [&]() {
		*supported = GetInstance(instance).GetEncryptionUtil(true)->SupportsHash(ConvertHashFunction(function));
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_supports_hmac(duckdb_v2_instance_handle instance,
                                               DUCKDB_V2_CRYPTO_HASH_FUNCTION function, bool *supported,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(supported);
	return WithErrorHandler(err, [&]() {
		*supported = GetInstance(instance).GetEncryptionUtil(true)->SupportsHmac(ConvertHashFunction(function));
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_hash(duckdb_v2_instance_handle instance, DUCKDB_V2_CRYPTO_HASH_FUNCTION function,
                                      const uint8_t *data, idx_t length, uint8_t *digest, idx_t digest_capacity,
                                      idx_t *digest_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(digest);
	DUCKDB_CHECK_ARG(digest_length);
	if (length > 0) {
		DUCKDB_CHECK_ARG(data);
	}
	*digest_length = 0;
	return WithErrorHandler(err, [&]() {
		auto hash_function = ConvertHashFunction(function);
		auto size = duckdb::CryptoHash::GetDigestSize(hash_function);
		if (digest_capacity < size) {
			throw duckdb::Exception(duckdb::ExceptionType::OBJECT_SIZE, "digest_capacity is smaller than the digest");
		}
		auto util = GetInstance(instance).GetEncryptionUtil(true);
		if (!util->SupportsHash(hash_function)) {
			throw duckdb::NotImplementedException("The crypto provider does not support this hash function");
		}
		util->Hash(hash_function, data, length, digest);
		*digest_length = size;
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_hmac(duckdb_v2_instance_handle instance, DUCKDB_V2_CRYPTO_HASH_FUNCTION function,
                                      const uint8_t *key, idx_t key_length, const uint8_t *data, idx_t length,
                                      uint8_t *mac, idx_t mac_capacity, idx_t *mac_length,
                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(key);
	DUCKDB_CHECK_ARG(mac);
	DUCKDB_CHECK_ARG(mac_length);
	if (length > 0) {
		DUCKDB_CHECK_ARG(data);
	}
	*mac_length = 0;
	return WithErrorHandler(err, [&]() {
		auto hash_function = ConvertHashFunction(function);
		auto size = duckdb::CryptoHash::GetDigestSize(hash_function);
		if (mac_capacity < size) {
			throw duckdb::Exception(duckdb::ExceptionType::OBJECT_SIZE, "mac_capacity is smaller than the MAC");
		}
		auto util = GetInstance(instance).GetEncryptionUtil(true);
		if (!util->SupportsHmac(hash_function)) {
			throw duckdb::NotImplementedException("The crypto provider does not support HMAC with this hash function");
		}
		util->Hmac(hash_function, key, key_length, data, length, mac);
		*mac_length = size;
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_random_bytes(duckdb_v2_instance_handle instance, uint8_t *data, idx_t length,
                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		auto util = GetInstance(instance).GetEncryptionUtil(false);
		auto state = util->CreateEncryptionState(duckdb::make_uniq<duckdb::EncryptionStateMetadata>(
		    duckdb::EncryptionTypes::GCM, 16, duckdb::EncryptionTypes::EncryptionVersion::V0_1));
		state->GenerateRandomData(data, length);
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_aes_create(duckdb_v2_instance_handle instance, DUCKDB_V2_CRYPTO_CIPHER cipher,
                                            DUCKDB_V2_CRYPTO_MODE mode, const uint8_t *key, idx_t key_length,
                                            duckdb_v2_crypto_aes_handle *aes, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(key);
	DUCKDB_CHECK_ARG(aes);
	*aes = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2Aes>(GetInstance(instance), cipher, mode, key, key_length);
		*aes = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_aes_init(duckdb_v2_crypto_aes_handle aes, const uint8_t *iv, idx_t iv_length,
                                          const uint8_t *aad, idx_t aad_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(aes);
	DUCKDB_CHECK_ARG(iv);
	return WithErrorHandler(err, [&]() { Convert(aes)->Init(iv, iv_length, aad, aad_length); });
}

DUCKDB_V2_ERROR duckdb_v2_crypto_aes_update(duckdb_v2_crypto_aes_handle aes, const uint8_t *in, idx_t in_length,
                                            uint8_t *out, idx_t out_capacity, idx_t *out_length,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(aes);
	DUCKDB_CHECK_ARG(out_length);
	if (in_length > 0) {
		DUCKDB_CHECK_ARG(in);
		DUCKDB_CHECK_ARG(out);
	}
	*out_length = 0;
	return WithErrorHandler(err, [&]() { *out_length = Convert(aes)->Update(in, in_length, out, out_capacity); });
}

DUCKDB_V2_ERROR duckdb_v2_crypto_aes_finalize(duckdb_v2_crypto_aes_handle aes, uint8_t *tag, idx_t tag_length,
                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(aes);
	return WithErrorHandler(err, [&]() { Convert(aes)->Finalize(tag, tag_length); });
}

DUCKDB_V2_ERROR duckdb_v2_crypto_aes_destroy(duckdb_v2_crypto_aes_handle *aes) {
	return WithErrorHandler(nullptr, [&]() {
		if (!aes) {
			return;
		}
		if (*aes) {
			delete Convert(*aes);
			*aes = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_provider_create(duckdb_v2_factory_handle factory,
                                                 duckdb_v2_crypto_provider_handle *provider,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(provider);
	*provider = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2CryptoProvider>(Convert(factory)->GetDatabase());
		*provider = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_provider_set_user_data(duckdb_v2_crypto_provider_handle provider,
                                                        duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(provider);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() {
		Convert(provider)->user_data = duckdb::make_shared_ptr<CV2UserData>(data->ptr, data->destroy, data->equals);
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_provider_set_aes_callbacks(duckdb_v2_crypto_provider_handle provider,
                                                            duckdb_v2_crypto_provider_aes_create_callback_fn create,
                                                            duckdb_v2_crypto_provider_aes_init_callback_fn init,
                                                            duckdb_v2_crypto_provider_aes_update_callback_fn update,
                                                            duckdb_v2_crypto_provider_aes_finalize_callback_fn finalize,
                                                            duckdb_v2_crypto_provider_aes_destroy_callback_fn destroy,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(provider);
	DUCKDB_CHECK_ARG(create);
	DUCKDB_CHECK_ARG(init);
	DUCKDB_CHECK_ARG(update);
	DUCKDB_CHECK_ARG(finalize);
	return WithErrorHandler(err, [&]() {
		auto &callbacks = Convert(provider)->callbacks;
		callbacks.aes_create = create;
		callbacks.aes_init = init;
		callbacks.aes_update = update;
		callbacks.aes_finalize = finalize;
		callbacks.aes_destroy = destroy;
	});
}

static uint32_t ToHashFunctionMask(const DUCKDB_V2_CRYPTO_HASH_FUNCTION *functions, idx_t function_count) {
	uint32_t mask = 0;
	for (idx_t i = 0; i < function_count; i++) {
		mask |= HashFunctionBit(ConvertHashFunction(functions[i]));
	}
	return mask;
}

DUCKDB_V2_ERROR duckdb_v2_crypto_provider_set_hash_callback(duckdb_v2_crypto_provider_handle provider,
                                                            duckdb_v2_crypto_provider_hash_callback_fn callback,
                                                            const DUCKDB_V2_CRYPTO_HASH_FUNCTION *functions,
                                                            idx_t function_count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(provider);
	if (function_count > 0) {
		DUCKDB_CHECK_ARG(functions);
	}
	return WithErrorHandler(err, [&]() {
		auto mask = ToHashFunctionMask(functions, function_count);
		auto &callbacks = Convert(provider)->callbacks;
		callbacks.hash = callback;
		callbacks.hash_functions = callback ? mask : 0;
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_provider_set_hmac_callback(duckdb_v2_crypto_provider_handle provider,
                                                            duckdb_v2_crypto_provider_hmac_callback_fn callback,
                                                            const DUCKDB_V2_CRYPTO_HASH_FUNCTION *functions,
                                                            idx_t function_count, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(provider);
	if (function_count > 0) {
		DUCKDB_CHECK_ARG(functions);
	}
	return WithErrorHandler(err, [&]() {
		auto mask = ToHashFunctionMask(functions, function_count);
		auto &callbacks = Convert(provider)->callbacks;
		callbacks.hmac = callback;
		callbacks.hmac_functions = callback ? mask : 0;
	});
}

DUCKDB_V2_ERROR duckdb_v2_crypto_provider_set_random_callback(duckdb_v2_crypto_provider_handle provider,
                                                              duckdb_v2_crypto_provider_random_callback_fn callback,
                                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(provider);
	return WithErrorHandler(err, [&]() { Convert(provider)->callbacks.random = callback; });
}

DUCKDB_V2_ERROR duckdb_v2_instance_register_crypto_provider(duckdb_v2_instance_handle instance,
                                                            duckdb_v2_crypto_provider_handle provider,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(provider);
	return WithErrorHandler(err, [&]() { Convert(provider)->Register(GetInstance(instance)); });
}

DUCKDB_V2_ERROR duckdb_v2_extension_register_crypto_provider(duckdb_v2_extension_handle extension,
                                                             duckdb_v2_crypto_provider_handle provider,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(extension);
	DUCKDB_CHECK_ARG(provider);
	return WithErrorHandler(
	    err, [&]() { Convert(provider)->Register(GetExtensionLoader(extension).GetDatabaseInstance()); });
}

DUCKDB_V2_ERROR duckdb_v2_crypto_provider_destroy(duckdb_v2_crypto_provider_handle *provider) {
	return WithErrorHandler(nullptr, [&]() {
		if (!provider) {
			return;
		}
		if (*provider) {
			delete Convert(*provider);
			*provider = nullptr;
		}
	});
}
