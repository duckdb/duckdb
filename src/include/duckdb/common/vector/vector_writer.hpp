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

#include <tuple>
#include <utility>

namespace duckdb {

//! Returns StructVector::GetEntries(vector) (mutable) without requiring struct_vector.hpp
//! to be complete in this header. Defined in struct_vector.cpp.
DUCKDB_API vector<Vector> &VectorWriterGetStructEntries(Vector &vector);

template <class T>
struct VectorWriter {
	VectorWriter(Vector &vector, idx_t count, idx_t offset)
	    : data(FlatVector::GetDataMutable<T>(vector)), validity(FlatVector::ValidityMutable(vector)),
	      count(offset + count), current_idx(offset) {
	}
	VectorWriter(VectorWriter &&other) noexcept
	    : data(other.data), validity(other.validity), count(other.count), current_idx(other.current_idx) {
		other.count = other.current_idx;
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
	VectorWriter(VectorWriter &&other) noexcept
	    : vector(other.vector), data(other.data), validity(other.validity), heap(other.heap), count(other.count),
	      current_idx(other.current_idx) {
		other.count = other.current_idx;
	}
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

//! Specialization of VectorWriter for VectorStructType<Args...>.
//! Writes rows to a struct vector with NULL propagation to all children.
//! Supports recursive nesting: a child writer may itself be a struct writer.
template <class... Args>
struct VectorWriter<VectorStructType<Args...>> {
private:
	static_assert(sizeof...(Args) > 0, "VectorStructType must have at least one child type");

	using ChildWriters = std::tuple<VectorWriter<Args>...>;

public:
	VectorWriter(Vector &vector, idx_t count, idx_t offset)
	    : validity(FlatVector::ValidityMutable(vector)), count(offset + count), current_idx(offset),
	      children(MakeChildren(vector, count, offset, std::index_sequence_for<Args...> {})) {
	}
	~VectorWriter() {
		D_ASSERT(Exception::UncaughtException() || current_idx == count);
	}

	//! Write a NULL for this struct row. Also writes NULL to every child so all
	//! per-child row counters stay in sync with the top-level counter.
	void WriteNull() {
		D_ASSERT(current_idx < count);
		validity.SetInvalid(current_idx);
		WriteNullToChildren(std::index_sequence_for<Args...> {});
		current_idx++;
	}

	//! Write a non-NULL row by calling fun(child0, child1, ...) with each child
	//! writer as a separate argument. fun is responsible for writing one value to
	//! each child. Advances the top-level row counter automatically.
	template <class FUN>
	void WriteValue(FUN &&fun) {
		D_ASSERT(current_idx < count);
		std::apply(std::forward<FUN>(fun), children);
		current_idx++;
	}

	//! Call fun(child_writer) for each child in declaration order, then advance
	//! the top-level row counter. Useful when all children share the same type
	//! and the same operation is applied to each.
	template <class FUN>
	void ForEach(FUN &&fun) {
		D_ASSERT(current_idx < count);
		ForEachImpl(std::forward<FUN>(fun), std::index_sequence_for<Args...> {});
		current_idx++;
	}

private:
	template <std::size_t... Is>
	void WriteNullToChildren(std::index_sequence<Is...>) {
		(std::get<Is>(children).WriteNull(), ...);
	}

	template <class FUN, std::size_t... Is>
	void ForEachImpl(FUN &&fun, std::index_sequence<Is...>) {
		(fun(std::get<Is>(children)), ...);
	}

	template <std::size_t... Is>
	static ChildWriters MakeChildren(Vector &vector, idx_t count, idx_t offset, std::index_sequence<Is...>) {
		auto &entries = VectorWriterGetStructEntries(vector);
		D_ASSERT(entries.size() >= sizeof...(Is));
		return ChildWriters(VectorWriter<Args>(entries[Is], count, offset)...);
	}

private:
	ValidityMask &validity;
	idx_t count;
	idx_t current_idx;
	ChildWriters children;
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
