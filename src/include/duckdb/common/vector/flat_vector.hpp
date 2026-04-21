//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/flat_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"

namespace duckdb {

class StandardVectorBuffer : public VectorBuffer {
public:
	StandardVectorBuffer(Allocator &allocator, idx_t capacity, idx_t type_size);
	explicit StandardVectorBuffer(idx_t capacity, idx_t type_size);
	explicit StandardVectorBuffer(data_ptr_t data_ptr_p, idx_t capacity, idx_t type_size);
	explicit StandardVectorBuffer(AllocatedData &&data_p, idx_t capacity, idx_t type_size);

public:
	data_ptr_t GetData() override {
		return data_ptr;
	}
	idx_t Capacity() const override {
		return capacity;
	}
	void ResetCapacity(idx_t capacity) override;
	ValidityMask &GetValidityMask() override {
		return validity;
	}
	const ValidityMask &GetValidityMask() const override {
		return validity;
	}
	void SetVectorType(VectorType vector_type) override;

	optional_ptr<Allocator> GetAllocator() const override {
		return allocated_data.GetAllocator();
	}

public:
	idx_t GetAllocationSize() const override;
	void ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const override;
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;
	Value GetValue(const LogicalType &type, idx_t index) const override;
	void SetValue(const LogicalType &type, idx_t index, const Value &val) override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;
	void Resize(idx_t current_size, idx_t new_size) override;

protected:
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, idx_t offset, idx_t end) override;
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

	virtual buffer_ptr<VectorBuffer> CreateBuffer(AllocatedData &&new_data, idx_t capacity) const;

protected:
	ValidityMask validity;
	data_ptr_t data_ptr;
	idx_t type_size;
	idx_t capacity;
	AllocatedData allocated_data;
};

template <class T>
struct VectorWriter;

struct FlatVector {
	static void VerifyFlatVector(const Vector &vector) {
#ifdef DUCKDB_DEBUG_NO_SAFETY
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
#else
		if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			throw InternalException("Operation requires a flat vector but a non-flat vector was encountered");
		}
#endif
	}
	static void VerifyFlatOrConst(const Vector &vector) {
#ifdef DUCKDB_DEBUG_NO_SAFETY
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR ||
		         vector.GetVectorType() == VectorType::FLAT_VECTOR);
#else
		if (vector.GetVectorType() != VectorType::CONSTANT_VECTOR &&
		    vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			throw InternalException(
			    "Operation requires a flat or constant vector but a non-flat/non-constant vector was encountered");
		}
#endif
	}
	static inline const_data_ptr_t GetData(Vector &vector) {
		VerifyFlatOrConst(vector);
		return GetDataUnsafe(vector);
	}
	static inline const_data_ptr_t GetData(const Vector &vector) {
		VerifyFlatOrConst(vector);
		return GetDataUnsafe(vector);
	}
	static inline data_ptr_t GetDataMutable(Vector &vector) {
		VerifyFlatOrConst(vector);
		return GetDataMutableUnsafe(vector);
	}
	static inline const_data_ptr_t GetDataUnsafe(const Vector &vector) {
		return vector.GetBufferRef() ? vector.GetBufferRef()->GetData() : nullptr;
	}
	static inline data_ptr_t GetDataMutableUnsafe(Vector &vector) {
		return vector.GetBufferRef() ? vector.BufferMutable().GetData() : nullptr;
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		ConstantVector::VerifyVectorType<T>(vector);
		return GetDataUnsafe<T>(vector);
	}
	template <class T>
	static inline T *GetDataMutable(Vector &vector) {
		ConstantVector::VerifyVectorType<T>(vector);
		return GetDataMutableUnsafe<T>(vector);
	}
	static inline idx_t GetCapacity(const Vector &vector) {
		auto &buffer_ref = vector.GetBufferRef();
		if (!buffer_ref) {
			return 0;
		}
		auto &buffer = *buffer_ref;
		if (buffer.GetVectorType() != VectorType::FLAT_VECTOR) {
			throw InternalException("FlatVector::GetCapacity requires a flat vector buffer");
		}
		return buffer.Capacity();
	}
	template <class T>
	static inline const T *GetDataUnsafe(const Vector &vector) {
		return reinterpret_cast<const T *>(GetData(vector));
	}
	template <class T>
	static inline T *GetDataMutableUnsafe(Vector &vector) {
		return reinterpret_cast<T *>(GetDataMutableUnsafe(vector));
	}
	static void SetData(Vector &vector, data_ptr_t data, idx_t capacity);
	template <class T>
	static inline T GetValue(Vector &vector, idx_t idx) {
		VerifyFlatVector(vector);
		return FlatVector::GetData<T>(vector)[idx];
	}
	static inline const ValidityMask &Validity(const Vector &vector) {
		VerifyFlatVector(vector);
		return vector.Buffer().GetValidityMask();
	}
	static inline ValidityMask &ValidityMutable(Vector &vector) {
		VerifyFlatVector(vector);
		return vector.BufferMutable().GetValidityMask();
	}
	static inline void SetValidity(Vector &vector, const ValidityMask &new_validity) {
		VerifyFlatVector(vector);
		auto &validity = vector.BufferMutable().GetValidityMask();
		validity.Initialize(new_validity);
	}
	DUCKDB_API static void SetNull(Vector &vector, idx_t idx, bool is_null);
	static inline bool IsNull(const Vector &vector, idx_t idx) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		auto &validity = vector.Buffer().GetValidityMask();
		return !validity.RowIsValid(idx);
	}
	DUCKDB_API static const SelectionVector *IncrementalSelectionVector();

	template <class T>
	static VectorWriter<T> Writer(Vector &vector, idx_t count) {
		return VectorWriter<T>(vector, count);
	}
	template <class T>
	static auto Writer(Vector &vector) -> decltype(Writer<T>(vector, NumericLimits<idx_t>::Maximum())) {
		return Writer<T>(vector, NumericLimits<idx_t>::Maximum());
	}
};

} // namespace duckdb

#include "duckdb/common/vector/vector_writer.hpp"
