//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/fsst_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/string_vector.hpp"

namespace duckdb {

class VectorFSSTStringBuffer : public VectorStringBuffer {
public:
	VectorFSSTStringBuffer();

public:
	void AddDecoder(buffer_ptr<void> &duckdb_fsst_decoder_p, const idx_t string_block_limit) {
		duckdb_fsst_decoder = duckdb_fsst_decoder_p;
		decompress_buffer.resize(string_block_limit + 1);
	}
	void *GetDecoder() {
		return duckdb_fsst_decoder.get();
	}
	vector<unsigned char> &GetDecompressBuffer() {
		return decompress_buffer;
	}
	void SetCount(idx_t count) {
		total_string_count = count;
	}
	idx_t GetCount() {
		return total_string_count;
	}

private:
	buffer_ptr<void> duckdb_fsst_decoder;
	idx_t total_string_count = 0;
	vector<unsigned char> decompress_buffer;
};

struct FSSTVector {
	static inline const ValidityMask &Validity(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.validity;
	}
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.validity;
	}
	static inline void SetValidity(Vector &vector, ValidityMask &new_validity) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		vector.validity.Initialize(new_validity);
	}
	static inline const string_t *GetCompressedData(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return reinterpret_cast<string_t *>(vector.buffer->GetData());
	}
	static inline string_t *GetCompressedData(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return reinterpret_cast<string_t *>(vector.buffer->GetData());
	}
	//! Decompresses an FSST_VECTOR into a FLAT_VECTOR. Note: validity is not copied.
	static void DecompressVector(const Vector &src, Vector &dst, idx_t src_offset, idx_t dst_offset, idx_t copy_count,
	                             const SelectionVector *sel);

	DUCKDB_API static string_t AddCompressedString(Vector &vector, string_t data);
	DUCKDB_API static string_t AddCompressedString(Vector &vector, const char *data, idx_t len);
	DUCKDB_API static void RegisterDecoder(Vector &vector, buffer_ptr<void> &duckdb_fsst_decoder,
	                                       const idx_t string_block_limit);
	DUCKDB_API static void *GetDecoder(const Vector &vector);
	DUCKDB_API static vector<unsigned char> &GetDecompressBuffer(const Vector &vector);
	//! Setting the string count is required to be able to correctly flatten the vector
	DUCKDB_API static void SetCount(Vector &vector, idx_t count);
	DUCKDB_API static idx_t GetCount(Vector &vector);
};

} // namespace duckdb
