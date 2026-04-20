//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/vector_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/string_heap.hpp"

namespace duckdb {

template <class T>
struct VectorWriter {
	VectorWriter(Vector &vector, idx_t count)
	    : data(FlatVector::GetDataMutable<T>(vector)), validity(FlatVector::ValidityMutable(vector)), count(count),
	      current_idx(0) {
	}

	void SetInvalid(idx_t idx) {
		D_ASSERT(idx < count);
		validity.SetInvalid(idx);
	}

	T &operator[](idx_t idx) {
		D_ASSERT(idx < count);
		return data[idx];
	}

	void PushValue(const T &value) {
		D_ASSERT(current_idx < count);
		data[current_idx] = value;
		current_idx++;
	}

	void PushInvalid() {
		D_ASSERT(current_idx < count);
		validity.SetInvalid(current_idx);
		current_idx++;
	}

private:
	T *data;
	ValidityMask &validity;
	idx_t count;
	idx_t current_idx;
};

template <>
struct VectorWriter<string_t> {
	struct StringElement {
		StringElement(VectorWriter<string_t> &writer, string_t *data, idx_t idx)
		    : writer(writer), data(data), idx(idx) {
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
		VectorWriter<string_t> &writer;
		string_t *data;
		idx_t idx;
	};

	VectorWriter(Vector &vector, idx_t count);

	inline void SetInvalid(idx_t idx) {
		D_ASSERT(idx < count);
		validity.SetInvalid(idx);
	}

	inline StringElement operator[](idx_t idx) {
		D_ASSERT(idx < count);
		return StringElement(*this, data, idx);
	}

	inline void PushValue(string_t val) {
		D_ASSERT(current_idx < count);
		StringElement(*this, data, current_idx) = val;
		current_idx++;
	}

	inline void PushInvalid() {
		D_ASSERT(current_idx < count);
		validity.SetInvalid(current_idx);
		current_idx++;
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
	idx_t current_idx;
};

} // namespace duckdb
