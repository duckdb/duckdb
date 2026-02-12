//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/encryption_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/encryption_types.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

struct EncryptionNonce;

struct EncryptionStateMetadata {
private:
	const EncryptionTypes::CipherType cipher;
	const idx_t key_len;
	const EncryptionTypes::EncryptionVersion version;

public:
	EncryptionStateMetadata() : cipher(EncryptionTypes::INVALID), key_len(0), version(EncryptionTypes::NONE) {
	}

	EncryptionStateMetadata(EncryptionTypes::CipherType cipher_p, idx_t key_len_p,
	                        EncryptionTypes::EncryptionVersion version_p)
	    : cipher(cipher_p), key_len(key_len_p), version(version_p) {
	}

	bool IsValid() const {
		if (!(cipher != EncryptionTypes::INVALID && key_len > 0)) {
			throw InvalidInputException("EncryptionStateMetadata is not initialized!");
		}
		return true;
	}

	EncryptionTypes::CipherType GetCipher() const {
		return cipher;
	}

	idx_t GetKeyLen() const {
		return key_len;
	}

	EncryptionTypes::EncryptionVersion GetVersion() const {
		return version;
	}
};

class EncryptionState {
public:
	DUCKDB_API explicit EncryptionState(unique_ptr<EncryptionStateMetadata> metadata);
	DUCKDB_API virtual ~EncryptionState();

public:
	DUCKDB_API virtual void InitializeEncryption(EncryptionNonce &nonce, const_data_ptr_t key,
	                                             const_data_ptr_t aad = nullptr, idx_t aad_len = 0);
	DUCKDB_API virtual void InitializeDecryption(EncryptionNonce &nonce, const_data_ptr_t key,
	                                             const_data_ptr_t aad = nullptr, idx_t aad_len = 0);
	DUCKDB_API virtual size_t Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len);
	DUCKDB_API virtual size_t Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len);
	DUCKDB_API virtual void GenerateRandomData(data_ptr_t data, idx_t len);

public:
	unique_ptr<EncryptionStateMetadata> metadata;

public:
	EncryptionTypes::CipherType GetCipher() const {
		return metadata->GetCipher();
	}
};

class EncryptionUtil {
public:
	DUCKDB_API explicit EncryptionUtil() {};

public:
	virtual shared_ptr<EncryptionState> CreateEncryptionState(unique_ptr<EncryptionStateMetadata> metadata) const {
		return make_shared_ptr<EncryptionState>(std::move(metadata));
	}

	virtual ~EncryptionUtil() {
	}

	DUCKDB_API virtual void OverrideEncryptionUtil() {
		throw InvalidInputException("Abstract Method");
	}
};

} // namespace duckdb
