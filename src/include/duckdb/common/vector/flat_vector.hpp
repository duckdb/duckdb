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

	optional_ptr<Allocator> GetAllocator() const override {
		return allocated_data.GetAllocator();
	}

protected:
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
		return vector.validity;
	}
	static inline ValidityMask &Validity(Vector &vector) {
		VerifyFlatVector(vector);
		return vector.validity;
	}
	static inline void SetValidity(Vector &vector, const ValidityMask &new_validity) {
		VerifyFlatVector(vector);
		vector.validity.Initialize(new_validity);
	}
	DUCKDB_API static void SetNull(Vector &vector, idx_t idx, bool is_null);
	static inline bool IsNull(const Vector &vector, idx_t idx) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return !vector.validity.RowIsValid(idx);
	}
	DUCKDB_API static const SelectionVector *IncrementalSelectionVector();

private:
	template <class T>
	struct FlatVectorWriter {
		FlatVectorWriter(Vector &vector, idx_t count)
		    : data(FlatVector::GetData<T>(vector)), validity(FlatVector::Validity(vector)), count(count) {
		}

		void SetInvalid(idx_t idx) {
			D_ASSERT(idx < count);
			validity.SetInvalid(idx);
		}

		T &operator[](idx_t idx) {
			D_ASSERT(idx < count);
			return data[idx];
		}

	private:
		T *data;
		ValidityMask &validity;
		idx_t count;
	};

	struct StringElement {
		StringElement(Vector &vector, string_t *data, idx_t idx) : vector(vector), data(data), idx(idx) {
		}

		//! Constructs an empty string of a given length and returns it
		//! Note: the empty string must be filled and .Finalize() must be called on it
		DUCKDB_API string_t &EmptyString(idx_t length);
		DUCKDB_API string_t &operator=(string_t val);
		inline char *GetDataWriteable() {
			return data[idx].GetDataWriteable();
		}
		inline void Finalize() {
			data[idx].Finalize();
		}
		inline string GetString() {
			return data[idx].GetString();
		}

		operator string_t() const { // NOLINT: allow implicit conversion
			return data[idx];
		}

	private:
		Vector &vector;
		string_t *data;
		idx_t idx;
	};

public:
	struct FlatStringWriter {
		FlatStringWriter(Vector &vector, idx_t count)
		    : vector(vector), data(FlatVector::GetData<string_t>(vector)), validity(FlatVector::Validity(vector)),
		      count(count) {
		}

		void SetInvalid(idx_t idx) {
			D_ASSERT(idx < count);
			validity.SetInvalid(idx);
		}

		StringElement operator[](idx_t idx) {
			D_ASSERT(idx < count);
			return StringElement(vector, data, idx);
		}

	private:
		Vector &vector;
		string_t *data;
		ValidityMask &validity;
		idx_t count;
	};

	template <class T>
	static auto Writer(Vector &vector, idx_t count) {
		if constexpr (std::is_same_v<T, string_t>) {
			return FlatStringWriter(vector, count);
		} else {
			return FlatVectorWriter<T>(vector, count);
		}
	}
	template <class T>
	static FlatVectorWriter<T> Writer(Vector &vector) {
		return Writer<T>(vector, NumericLimits<idx_t>::Maximum());
	}
};

} // namespace duckdb
