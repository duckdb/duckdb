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
	StandardVectorBuffer(Allocator &allocator, idx_t capacity, idx_t type_size);
	explicit StandardVectorBuffer(idx_t capacity, idx_t type_size);
	explicit StandardVectorBuffer(data_ptr_t data_ptr_p, idx_t capacity);
	explicit StandardVectorBuffer(AllocatedData &&data_p, idx_t capacity);

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
	buffer_ptr<VectorBuffer> Resize(const LogicalType &type, idx_t current_size, idx_t new_size) override;

protected:
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, idx_t offset, idx_t end) override;
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

	virtual buffer_ptr<VectorBuffer> CreateBuffer(AllocatedData &&new_data, idx_t capacity) const;

protected:
	ValidityMask validity;
	data_ptr_t data_ptr;
	idx_t capacity;
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
		return vector.buffer ? vector.buffer->GetData() : nullptr;
	}
	static inline const_data_ptr_t GetData(const Vector &vector) {
		VerifyFlatOrConst(vector);
		return vector.buffer ? vector.buffer->GetData() : nullptr;
	}
	static inline data_ptr_t GetDataMutable(Vector &vector) {
		VerifyFlatOrConst(vector);
		return vector.buffer ? vector.buffer->GetData() : nullptr;
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		ConstantVector::VerifyVectorType<T>(vector);
		return GetDataUnsafe<T>(vector);
	}
	template <class T>
	static inline const T *GetData(Vector &vector) {
		ConstantVector::VerifyVectorType<T>(vector);
		return GetDataUnsafe<T>(vector);
	}
	template <class T>
	static inline T *GetDataMutable(Vector &vector) {
		ConstantVector::VerifyVectorType<T>(vector);
		return GetDataMutableUnsafe<T>(vector);
	}
	static inline idx_t GetCapacity(const Vector &vector) {
		VerifyFlatVector(vector);
		return vector.buffer ? vector.buffer->Capacity() : 0;
	}
	template <class T>
	static inline const T *GetDataUnsafe(const Vector &vector) {
		return reinterpret_cast<const T *>(GetData(vector));
	}
	template <class T>
	static inline const T *GetDataUnsafe(Vector &vector) {
		return reinterpret_cast<const T *>(GetData(vector));
	}
	template <class T>
	static inline T *GetDataMutableUnsafe(Vector &vector) {
		return reinterpret_cast<T *>(GetDataMutable(vector));
	}
	static void SetData(Vector &vector, data_ptr_t data, idx_t capacity);
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

public:
	template <class T>
	struct FlatVectorWriter {
		FlatVectorWriter(Vector &vector, idx_t count)
		    : data(GetDataMutable<T>(vector)), validity(Validity(vector)), count(count) {
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
			if (length <= string_t::INLINE_LENGTH) {
				data[idx] = string_t(UnsafeNumericCast<uint32_t>(length));
			} else {
				auto &heap = writer.GetHeap();
				data[idx] = heap.CreateEmptyStringInHeap(length);
			}
			return data[idx];
		}
		inline string_t &operator=(string_t val) {
			if (val.IsInlined()) {
				data[idx] = val;
			} else {
				auto &heap = writer.GetHeap();
				data[idx] = heap.AddBlobToHeap(val.GetData(), val.GetSize());
			}
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
			if (!heap) {
				InitializeHeap();
			}
			return *heap;
		}

	private:
		void InitializeHeap();

	private:
		Vector &vector;
		string_t *data;
		ValidityMask &validity;
		optional_ptr<StringHeap> heap;
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
