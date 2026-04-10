//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/shredded_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class ShreddedVectorBuffer : public VectorBuffer {
public:
	explicit ShreddedVectorBuffer(Vector &shredded_data);
	~ShreddedVectorBuffer() override;

public:
	Vector &GetChild() {
		return *shredded_data;
	}
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

public:
	idx_t GetAllocationSize() const override;
	string ToString(const LogicalType &type, idx_t count) const override;
	Value GetValue(const LogicalType &type, idx_t index) const override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;

private:
	unique_ptr<Vector> shredded_data;
};

struct ShreddedVector {
	static void VerifyShreddedVector(const Vector &vector) {
#ifdef DUCKDB_DEBUG_NO_SAFETY
		D_ASSERT(vector.GetVectorType() == VectorType::SHREDDED_VECTOR);
#else
		if (vector.GetVectorType() != VectorType::SHREDDED_VECTOR) {
			throw InternalException("Operation requires a shredded vector but a non-shredded vector was encountered");
		}
#endif
	}
	//! Get the underlying vector holding the unshredded data
	DUCKDB_API static const Vector &GetUnshreddedVector(const Vector &vec);
	//! Get the underlying vector holding the unshredded data
	DUCKDB_API static Vector &GetUnshreddedVector(Vector &vec);
	//! Get the underlying vector holding the shredded data
	DUCKDB_API static const Vector &GetShreddedVector(const Vector &vec);
	//! Get the underlying vector holding the shredded data
	DUCKDB_API static Vector &GetShreddedVector(Vector &vec);

	//! Unshred a shredded vector
	DUCKDB_API static void Unshred(const Vector &vec, idx_t count);
	DUCKDB_API static void Unshred(const Vector &vec, const SelectionVector &sel, idx_t count);

	//! Returns whether or not the vector is fully shredded
	DUCKDB_API static bool IsFullyShredded(Vector &vec);
};

} // namespace duckdb
