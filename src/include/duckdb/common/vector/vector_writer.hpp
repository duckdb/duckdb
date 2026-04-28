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
	VectorWriter(Vector &vector, idx_t count, idx_t offset)
	    : data(FlatVector::GetDataMutable<T>(vector)), validity(FlatVector::ValidityMutable(vector)),
	      count(offset + count), current_idx(offset) {
	}
	~VectorWriter() {
		// ensure that all values we said we were going to write have been written
		D_ASSERT(Exception::UncaughtException() || current_idx == count);
	}

	void WriteValue(const T &value) {
		D_ASSERT(current_idx < count);
		data[current_idx] = value;
		current_idx++;
	}

	void WriteNull() {
		D_ASSERT(current_idx < count);
		validity.SetInvalid(current_idx);
		current_idx++;
	}

	void WriteNull(const T &value) {
		D_ASSERT(current_idx < count);
		data[current_idx] = value;
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
	VectorWriter(Vector &vector, idx_t count, idx_t offset);
	~VectorWriter() {
		D_ASSERT(Exception::UncaughtException() || current_idx == count);
	}

	inline const string_t &WriteValue(string_t val) {
		D_ASSERT(current_idx < count);
		AssignString(current_idx, val);
		current_idx++;
		return data[current_idx - 1];
	}

	inline void WriteStringRef(string_t val) {
		D_ASSERT(current_idx < count);
		data[current_idx] = val;
		current_idx++;
	}

	inline void WriteNull() {
		D_ASSERT(current_idx < count);
		validity.SetInvalid(current_idx);
		current_idx++;
	}

	inline string_t &WriteEmptyString(idx_t length) {
		if (length <= string_t::INLINE_LENGTH) {
			data[current_idx] = string_t(UnsafeNumericCast<uint32_t>(length));
		} else {
			data[current_idx] = GetHeap().CreateEmptyStringInHeap(length);
		}
		auto &res = data[current_idx];
		current_idx++;
		return res;
	}

	inline StringHeap &GetHeap() {
		if (!heap) {
			InitializeHeap();
		}
		return *heap;
	}

private:
	void InitializeHeap();

	inline void AssignString(idx_t idx, string_t val) {
		if (val.IsInlined()) {
			data[idx] = val;
		} else {
			auto &string_heap = GetHeap();
			data[idx] = string_heap.AddBlobToHeap(val.GetData(), val.GetSize());
		}
	}

private:
	Vector &vector;
	string_t *data;
	ValidityMask &validity;
	optional_ptr<StringHeap> heap;
	idx_t count;
	idx_t current_idx;
};

template <class T>
struct VectorScatterWriter {
	explicit VectorScatterWriter(Vector &vector)
	    : data(FlatVector::GetDataMutable<T>(vector)), validity(FlatVector::ValidityMutable(vector)) {
	}

	void SetInvalid(idx_t idx) {
		validity.SetInvalid(idx);
	}

	T &operator[](idx_t idx) {
		return data[idx];
	}

private:
	T *data;
	ValidityMask &validity;
};

template <>
struct VectorScatterWriter<string_t> {
	struct StringElement {
		StringElement(VectorScatterWriter<string_t> &writer, string_t *data, idx_t idx)
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
		VectorScatterWriter<string_t> &writer;
		string_t *data;
		idx_t idx;
	};

	explicit VectorScatterWriter(Vector &vector);

	inline void SetInvalid(idx_t idx) {
		validity.SetInvalid(idx);
	}

	inline StringElement operator[](idx_t idx) {
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
};

} // namespace duckdb
