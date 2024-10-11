//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/encryption_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class EncryptionState {

public:
	DUCKDB_API EncryptionState();
	DUCKDB_API virtual ~EncryptionState();

public:
	DUCKDB_API virtual bool IsOpenSSL();
	DUCKDB_API virtual void InitializeEncryption(const_data_ptr_t iv, idx_t iv_len, const std::string *key);
	DUCKDB_API virtual void InitializeDecryption(const_data_ptr_t iv, idx_t iv_len, const std::string *key);
	DUCKDB_API virtual size_t Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len);
	DUCKDB_API virtual size_t Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len);
	DUCKDB_API virtual void GenerateRandomData(data_ptr_t data, idx_t len);

public:
	enum Mode { ENCRYPT, DECRYPT };
};

class EncryptionUtil {

public:
	DUCKDB_API explicit EncryptionUtil() {};

public:
	virtual shared_ptr<EncryptionState> CreateEncryptionState() const {
		return make_shared_ptr<EncryptionState>();
	}

	virtual ~EncryptionUtil() {
	}
};

} // namespace duckdb
