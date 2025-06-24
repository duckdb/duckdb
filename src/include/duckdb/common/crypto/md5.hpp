//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/crypto/md5.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

class MD5Context {
public:
	static constexpr idx_t MD5_HASH_LENGTH_BINARY = 16;
	static constexpr idx_t MD5_HASH_LENGTH_TEXT = 32;

public:
	MD5Context();

	void Add(const_data_ptr_t data, idx_t len) {
		MD5Update(data, len);
	}
	void Add(const char *data);
	void Add(string_t string) {
		MD5Update(const_data_ptr_cast(string.GetData()), string.GetSize());
	}
	void Add(const string &data) {
		MD5Update(const_data_ptr_cast(data.c_str()), data.size());
	}

	//! Write the 16-byte (binary) digest to the specified location
	void Finish(data_ptr_t out_digest);
	//! Write the 32-character digest (in hexadecimal format) to the specified location
	void FinishHex(char *out_digest);
	//! Returns the 32-character digest (in hexadecimal format) as a string
	string FinishHex();

private:
	void MD5Update(const_data_ptr_t data, idx_t len);

	uint32_t buf[4];
	uint32_t bits[2];
	unsigned char in[64];
};

} // namespace duckdb
