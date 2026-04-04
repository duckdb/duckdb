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
#include "duckdb/common/types/string_heap.hpp"

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

public:
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

	struct FlatStringWriter;
	struct StringElement {
		StringElement(FlatStringWriter &writer, string_t *data, idx_t idx) : writer(writer), data(data), idx(idx) {
		}

		//! Constructs an empty string of a given length and returns it
		//! Note: the empty string must be filled and .Finalize() must be called on it
		inline string_t &EmptyString(idx_t length) {
			auto &heap = writer.GetHeap();
			data[idx] = heap.EmptyString(length);
			return data[idx];
		}
		inline string_t &operator=(string_t val) {
			auto &heap = writer.GetHeap();
			data[idx] = heap.AddBlob(val);
			return data[idx];
		}
		inline void AssignWithoutCopying(string_t val) {
			data[idx] = val;
		}
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
		FlatStringWriter &writer;
		string_t *data;
		idx_t idx;
	};

public:
	struct FlatStringWriter {
		FlatStringWriter(Vector &vector, idx_t count);

		inline void SetInvalid(idx_t idx) {
			D_ASSERT(idx < count);
			validity.SetInvalid(idx);
		}

		inline StringElement operator[](idx_t idx) {
			D_ASSERT(idx < count);
			return StringElement(*this, data, idx);
		}

		inline StringHeap &GetHeap() {
			return heap;
		}

	private:
		Vector &vector;
		string_t *data;
		ValidityMask &validity;
		StringHeap &heap;
		idx_t count;
	};

	template <class T, typename std::enable_if<std::is_same<T, string_t>::value, int>::type = 0>
	static FlatStringWriter Writer(Vector &vector, idx_t count) {
		return FlatStringWriter(vector, count);
	}
	template <class T, typename std::enable_if<!std::is_same<T, string_t>::value, int>::type = 0>
	static FlatVectorWriter<T> Writer(Vector &vector, idx_t count) {
		return FlatVectorWriter<T>(vector, count);
	}
	template <class T>
	static auto Writer(Vector &vector) -> decltype(Writer<T>(vector, NumericLimits<idx_t>::Maximum())) {
		return Writer<T>(vector, NumericLimits<idx_t>::Maximum());
	}
};

} // namespace duckdb
