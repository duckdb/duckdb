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
	StandardVectorBuffer(Allocator &allocator, idx_t data_size);
	explicit StandardVectorBuffer(idx_t data_size);
	explicit StandardVectorBuffer(data_ptr_t data_ptr_p);
	explicit StandardVectorBuffer(AllocatedData &&data_p);

public:
	data_ptr_t GetData() override {
		return data_ptr;
	}
	ValidityMask &GetValidityMask() override {
		return validity;
	}

	optional_ptr<Allocator> GetAllocator() const override {
		return allocated_data.GetAllocator();
	}

protected:
	ValidityMask validity;
	data_ptr_t data_ptr;
	AllocatedData allocated_data;
};

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

	static inline data_ptr_t GetData(Vector &vector) {
		return ConstantVector::GetData(vector);
	}
	static inline const_data_ptr_t GetData(const Vector &vector) {
		return ConstantVector::GetData(vector);
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	template <class T>
	static inline T *GetData(Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	template <class T>
	static inline const T *GetDataUnsafe(const Vector &vector) {
		return ConstantVector::GetDataUnsafe<T>(vector);
	}
	template <class T>
	static inline T *GetDataUnsafe(Vector &vector) {
		return ConstantVector::GetDataUnsafe<T>(vector);
	}
	static void SetData(Vector &vector, data_ptr_t data);
	template <class T>
	static inline T GetValue(Vector &vector, idx_t idx) {
		VerifyFlatVector(vector);
		return FlatVector::GetData<T>(vector)[idx];
	}
	static inline const ValidityMask &Validity(const Vector &vector) {
		VerifyFlatVector(vector);
		return vector.buffer->GetValidityMask();
	}
	static inline ValidityMask &Validity(Vector &vector) {
		VerifyFlatVector(vector);
		return vector.buffer->GetValidityMask();
	}
	static inline void SetValidity(Vector &vector, const ValidityMask &new_validity) {
		VerifyFlatVector(vector);
		auto &validity = vector.buffer->GetValidityMask();
		validity.Initialize(new_validity);
	}
	DUCKDB_API static void SetNull(Vector &vector, idx_t idx, bool is_null);
	static inline bool IsNull(const Vector &vector, idx_t idx) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		auto &validity = vector.buffer->GetValidityMask();
		return !validity.RowIsValid(idx);
	}
	DUCKDB_API static const SelectionVector *IncrementalSelectionVector();

private:
	template <class T>
	struct FlatVectorWriter {
		FlatVectorWriter(Vector &vector, idx_t count)
		    : data(FlatVector::GetData<T>(vector)), validity(FlatVector::Validity(vector)),
		      capacity(validity.Capacity()) {
			(void)count;
		}

		void SetInvalid(idx_t idx) {
			D_ASSERT(idx < capacity);
			validity.SetInvalid(idx);
		}

		T &operator[](idx_t idx) {
			D_ASSERT(idx < capacity);
			return data[idx];
		}

	private:
		T *data;
		ValidityMask &validity;
		idx_t capacity;
	};

public:
	template <class T>
	static FlatVectorWriter<T> Writer(Vector &vector, idx_t count) {
		return FlatVectorWriter<T>(vector, count);
	}
};

} // namespace duckdb
