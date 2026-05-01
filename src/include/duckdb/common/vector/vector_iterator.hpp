//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/vector_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

#include <tuple>
#include <utility>

namespace duckdb {

//! Returns StructVector::GetEntries(vector) without requiring StructVector to
//! be complete in this header (avoids a circular include via flat_vector.hpp).
//! Defined in struct_vector.cpp.
DUCKDB_API const vector<Vector> &VectorIteratorGetStructEntries(const Vector &vector);

//! Returns ListVector::GetChild(vector) / ListVector::GetListSize(vector) without
//! requiring list_vector.hpp to be complete in this header.
//! Defined in list_vector.cpp.
DUCKDB_API const Vector &VectorIteratorGetListChild(const Vector &vector);
DUCKDB_API idx_t VectorIteratorGetListSize(const Vector &vector);

class VectorValidityIterator {
public:
	VectorValidityIterator(const Vector &vector, idx_t count) : count(count) {
		vector.ToUnifiedFormat(count, format);
	}

	bool IsValid(idx_t i) const {
		return format.validity.RowIsValid(format.sel->get_index(i));
	}
	bool CanHaveNull() const {
		return format.validity.CanHaveNull();
	}
	idx_t size() const {
		return count;
	}

private:
	UnifiedVectorFormat format;
	idx_t count;
};

template <class T>
class VectorIterator {
public:
	VectorIterator(const Vector &vector, idx_t count) : count(count) {
		vector.ToUnifiedFormat(count, format);
		data = UnifiedVectorFormat::GetData<T>(format);
	}

public:
	struct ValueEntry {
		ValueEntry(const UnifiedVectorFormat &format, const T *data, idx_t index)
		    : format(format), data(data), index(index) {
			sel_index = format.sel->get_index(index);
		}

		//! Return the data - the value must be valid
		const T &GetValue() const {
			D_ASSERT(IsValid());
			return GetValueUnsafe();
		}
		//! Return the underlying data. If the data is not valid then uninitialized memory is returned.
		const T &GetValueUnsafe() const {
			return data[sel_index];
		}
		bool IsValid() const {
			return format.validity.RowIsValid(sel_index);
		}
		idx_t GetIndex() const {
			return index;
		}

	private:
		const UnifiedVectorFormat &format;
		const T *data;
		idx_t sel_index;
		idx_t index;
	};

private:
	class Iterator {
	public:
		explicit Iterator(UnifiedVectorFormat &format, const T *data, idx_t index)
		    : format(format), data(data), index(index) {
		}

	public:
		Iterator &operator++() { // NOLINT: match stl API
			++index;
			return *this;
		}
		Iterator operator++(int) { // NOLINT: match stl API
			auto tmp = *this;
			++index;
			return tmp;
		}
		Iterator &operator--() { // NOLINT: match stl API
			--index;
			return *this;
		}
		Iterator &operator+=(idx_t n) {
			index += n;
			return *this;
		}
		Iterator &operator-=(idx_t n) {
			index -= n;
			return *this;
		}
		Iterator operator+(idx_t n) const {
			return Iterator(format, data, index + n);
		}
		Iterator operator-(idx_t n) const {
			return Iterator(format, data, index - n);
		}
		int64_t operator-(const Iterator &other) const {
			return static_cast<int64_t>(index) - static_cast<int64_t>(other.index);
		}
		bool operator==(const Iterator &other) const {
			return index == other.index;
		}
		bool operator!=(const Iterator &other) const {
			return index != other.index;
		}
		bool operator<(const Iterator &other) const {
			return index < other.index;
		}
		bool operator<=(const Iterator &other) const {
			return index <= other.index;
		}
		bool operator>(const Iterator &other) const {
			return index > other.index;
		}
		bool operator>=(const Iterator &other) const {
			return index >= other.index;
		}
		ValueEntry operator*() const {
			return GetEntry(index);
		}
		ValueEntry operator[](idx_t n) const {
			return GetEntry(index + n);
		}

	private:
		ValueEntry GetEntry(idx_t i) const {
			return ValueEntry(format, data, i);
		}

	private:
		UnifiedVectorFormat &format;
		const T *data;
		idx_t index;
	};

public:
	Iterator begin() { // NOLINT: match stl API
		return Iterator(format, data, 0);
	}
	Iterator end() { // NOLINT: match stl API
		return Iterator(format, data, count);
	}
	idx_t size() const {
		return count;
	}
	ValueEntry operator[](idx_t i) const {
		return ValueEntry(format, data, i);
	}
	//! Returns the value at the specified location without checking the NULL mask
	T GetValueUnsafe(idx_t i) const {
		return data[format.sel->get_index(i)];
	}
	bool CanHaveNull() const {
		return format.validity.CanHaveNull();
	}

private:
	UnifiedVectorFormat format;
	const T *data;
	idx_t count;
};

//! Specialization of VectorIterator for FlatStruct<Args...>.
//! Iterates over a struct vector, exposing per-row top-level NULL information
//! and per-child VectorIterator<T>::ValueEntry access via the compile-time
//! GetValue<I>() accessor. Supports heterogeneous child types.
template <class... Args>
class VectorIterator<VectorStructType<Args...>> {
private:
	static_assert(sizeof...(Args) > 0, "FlatStruct must have at least one child type");

	template <idx_t I>
	using ChildTypeAt = typename std::tuple_element<I, std::tuple<Args...>>::type;
	template <idx_t I>
	using ChildIteratorAt = VectorIterator<ChildTypeAt<I>>;
	template <idx_t I>
	using ChildEntryAt = typename ChildIteratorAt<I>::ValueEntry;

	using ChildIterators = std::tuple<VectorIterator<Args>...>;

public:
	static constexpr idx_t WIDTH = sizeof...(Args);

	VectorIterator(const Vector &vector, idx_t count)
	    : children(MakeChildren(vector, count, std::index_sequence_for<Args...> {})), count(count) {
		vector.ToUnifiedFormat(count, format);
	}

public:
	struct ValueEntry {
		ValueEntry(const UnifiedVectorFormat &format, const ChildIterators &children, idx_t index)
		    : format(format), children(children), index(index) {
			sel_index = format.sel->get_index(index);
		}

		//! Returns true if the top-level struct entry is not NULL
		bool IsValid() const {
			return format.validity.RowIsValid(sel_index);
		}
		idx_t GetIndex() const {
			return index;
		}
		//! Returns the ValueEntry for the I-th child at this row
		template <idx_t I>
		ChildEntryAt<I> GetChildValue() const {
			static_assert(I < sizeof...(Args), "FlatStruct child index out of range");
			return std::get<I>(children)[index];
		}
		//! Invokes fun(child_entry) for each child in declaration order.
		//! For homogeneous structs the lambda's argument can be a concrete
		//! VectorIterator<T>::ValueEntry; for heterogeneous structs use auto.
		template <class FUN>
		void ForEach(FUN &&fun) const {
			ForEachImpl(std::forward<FUN>(fun), std::index_sequence_for<Args...> {});
		}

	private:
		template <class FUN, std::size_t... Is>
		void ForEachImpl(FUN &&fun, std::index_sequence<Is...>) const {
			(fun(GetChildValue<Is>()), ...);
		}

	private:
		const UnifiedVectorFormat &format;
		const ChildIterators &children;
		idx_t sel_index;
		idx_t index;
	};

private:
	template <std::size_t... Is>
	static ChildIterators MakeChildren(const Vector &vector, idx_t count, std::index_sequence<Is...>) {
		auto &entries = VectorIteratorGetStructEntries(vector);
		D_ASSERT(entries.size() >= sizeof...(Is));
		return ChildIterators(VectorIterator<Args>(entries[Is], count)...);
	}

	class Iterator {
	public:
		Iterator(const UnifiedVectorFormat &format, const ChildIterators &children, idx_t index)
		    : format(format), children(children), index(index) {
		}

	public:
		Iterator &operator++() { // NOLINT: match stl API
			++index;
			return *this;
		}
		Iterator operator++(int) { // NOLINT: match stl API
			auto tmp = *this;
			++index;
			return tmp;
		}
		bool operator==(const Iterator &other) const {
			return index == other.index;
		}
		bool operator!=(const Iterator &other) const {
			return index != other.index;
		}
		ValueEntry operator*() const {
			return ValueEntry(format, children, index);
		}
		ValueEntry operator[](idx_t n) const {
			return ValueEntry(format, children, index + n);
		}

	private:
		const UnifiedVectorFormat &format;
		const ChildIterators &children;
		idx_t index;
	};

public:
	Iterator begin() { // NOLINT: match stl API
		return Iterator(format, children, 0);
	}
	Iterator end() { // NOLINT: match stl API
		return Iterator(format, children, count);
	}
	idx_t size() const {
		return count;
	}
	ValueEntry operator[](idx_t i) const {
		return ValueEntry(format, children, i);
	}
	bool CanHaveNull() const {
		return format.validity.CanHaveNull();
	}

private:
	UnifiedVectorFormat format;
	ChildIterators children;
	idx_t count;
};

//! Specialization of VectorIterator for VectorListType<T>.
//! Iterates over a list vector, exposing per-row NULL information and access to
//! the child entries via GetChildValue(idx) / GetChildValues().  Supports
//! recursive nesting: VectorListType<VectorListType<T>>, etc.
//!
//! The source vector must not be a DICTIONARY_VECTOR; call Flatten() first if needed.
template <class T>
class VectorIterator<VectorListType<T>> {
private:
	using ChildIteratorType = VectorIterator<T>;
	using ChildEntryType = typename ChildIteratorType::ValueEntry;

public:
	VectorIterator(const Vector &vector, idx_t count)
	    : count(count), child_iter(VectorIteratorGetListChild(vector), VectorIteratorGetListSize(vector)) {
		vector.ToUnifiedFormat(count, format);
		list_data = UnifiedVectorFormat::GetData<list_entry_t>(format);
	}

public:
	//! Range object returned by ValueEntry::GetChildValues() for range-based for loops.
	struct ChildRange {
	private:
		struct RangeIterator {
		public:
			RangeIterator(const ChildIteratorType &child_iter, idx_t pos) : child_iter(child_iter), pos(pos) {
			}
			RangeIterator &operator++() { // NOLINT: match stl API
				++pos;
				return *this;
			}
			RangeIterator operator++(int) { // NOLINT: match stl API
				auto tmp = *this;
				++pos;
				return tmp;
			}
			bool operator==(const RangeIterator &other) const {
				return pos == other.pos;
			}
			bool operator!=(const RangeIterator &other) const {
				return pos != other.pos;
			}
			ChildEntryType operator*() const {
				return child_iter[pos];
			}

		private:
			const ChildIteratorType &child_iter;
			idx_t pos;
		};

	public:
		ChildRange(const ChildIteratorType &child_iter, idx_t offset, idx_t length)
		    : child_iter(child_iter), offset(offset), length(length) {
		}
		RangeIterator begin() const { // NOLINT: match stl API
			return RangeIterator(child_iter, offset);
		}
		RangeIterator end() const { // NOLINT: match stl API
			return RangeIterator(child_iter, offset + length);
		}
		idx_t size() const {
			return length;
		}

	private:
		const ChildIteratorType &child_iter;
		idx_t offset;
		idx_t length;
	};

	struct ValueEntry {
		ValueEntry(const UnifiedVectorFormat &format, const list_entry_t *list_data,
		           const ChildIteratorType &child_iter, idx_t index)
		    : format(format), list_data(list_data), child_iter(child_iter), index(index) {
			sel_index = format.sel->get_index(index);
		}

		bool IsValid() const {
			return format.validity.RowIsValid(sel_index);
		}
		idx_t GetIndex() const {
			return index;
		}
		//! Returns the list_entry_t (offset + length). The entry must be valid.
		const list_entry_t &GetValue() const {
			D_ASSERT(IsValid());
			return list_data[sel_index];
		}
		//! Returns the number of child elements in this list entry.
		idx_t GetListLength() const {
			return GetValue().length;
		}
		//! Returns the child entry at position idx within this list.
		ChildEntryType GetChildValue(idx_t idx) const {
			return child_iter[GetValue().offset + idx];
		}
		//! Returns an iterable range over all child entries in this list.
		ChildRange GetChildValues() const {
			const auto &entry = GetValue();
			return ChildRange(child_iter, entry.offset, entry.length);
		}

	private:
		const UnifiedVectorFormat &format;
		const list_entry_t *list_data;
		const ChildIteratorType &child_iter;
		idx_t sel_index;
		idx_t index;
	};

private:
	class Iterator {
	public:
		Iterator(const UnifiedVectorFormat &format, const list_entry_t *list_data, const ChildIteratorType &child_iter,
		         idx_t index)
		    : format(format), list_data(list_data), child_iter(child_iter), index(index) {
		}

	public:
		Iterator &operator++() { // NOLINT: match stl API
			++index;
			return *this;
		}
		Iterator operator++(int) { // NOLINT: match stl API
			auto tmp = *this;
			++index;
			return tmp;
		}
		bool operator==(const Iterator &other) const {
			return index == other.index;
		}
		bool operator!=(const Iterator &other) const {
			return index != other.index;
		}
		ValueEntry operator*() const {
			return ValueEntry(format, list_data, child_iter, index);
		}
		ValueEntry operator[](idx_t n) const {
			return ValueEntry(format, list_data, child_iter, index + n);
		}

	private:
		const UnifiedVectorFormat &format;
		const list_entry_t *list_data;
		const ChildIteratorType &child_iter;
		idx_t index;
	};

public:
	Iterator begin() { // NOLINT: match stl API
		return Iterator(format, list_data, child_iter, 0);
	}
	Iterator end() { // NOLINT: match stl API
		return Iterator(format, list_data, child_iter, count);
	}
	idx_t size() const {
		return count;
	}
	ValueEntry operator[](idx_t i) const {
		return ValueEntry(format, list_data, child_iter, i);
	}
	bool CanHaveNull() const {
		return format.validity.CanHaveNull();
	}

private:
	UnifiedVectorFormat format;
	const list_entry_t *list_data;
	ChildIteratorType child_iter;
	idx_t count;
};

template <class T>
class VectorValidValueIterator {
public:
	VectorValidValueIterator(const Vector &vector, idx_t count) : count(count) {
		vector.ToUnifiedFormat(count, format);
		data = UnifiedVectorFormat::GetData<T>(format);
	}

private:
	class VectorScanIterator;

public:
	struct VectorValueEntry {
		const T &GetValue() const {
			return value;
		}
		idx_t GetIndex() const {
			return index;
		}

	private:
		idx_t index;
		T value;
		friend class VectorScanIterator;
	};

private:
	class VectorScanIterator {
	public:
		explicit VectorScanIterator(UnifiedVectorFormat &format, const T *data, idx_t index, idx_t count)
		    : format(format), data(data), count(count), can_have_null(format.validity.CanHaveNull()) {
			r.index = index;
			AdvanceToValid();
		}

	public:
		VectorScanIterator &operator++() {
			++r.index;
			AdvanceToValid();
			return *this;
		}
		VectorScanIterator operator++(int) {
			auto tmp = *this;
			++(*this);
			return tmp;
		}
		bool operator!=(const VectorScanIterator &other) const {
			return r.index != other.r.index;
		}
		const VectorValueEntry &operator*() const {
			return r;
		}

	private:
		void AdvanceToValid() {
			if (!can_have_null) {
				if (r.index < count) {
					// we know this value is valid
					r.value = data[format.sel->get_index(r.index)];
				}
				return;
			}
			for (; r.index < count; r.index++) {
				auto idx = format.sel->get_index(r.index);
				if (format.validity.RowIsValid(idx)) {
					// found a valid value - stop
					r.value = data[idx];
					break;
				}
			}
		}

	private:
		UnifiedVectorFormat &format;
		const T *data;
		VectorValueEntry r;
		idx_t count;
		bool can_have_null;
	};

public:
	VectorScanIterator begin() { // NOLINT: match stl API
		return VectorScanIterator(format, data, 0, count);
	}
	VectorScanIterator end() { // NOLINT: match stl API
		return VectorScanIterator(format, data, count, count);
	}
	idx_t size() const {
		return count;
	}

private:
	UnifiedVectorFormat format;
	const T *data;
	idx_t count;
};

template <class T>
inline VectorIterator<T> Vector::Values(idx_t count) const {
	return VectorIterator<T>(*this, count);
}

template <class T>
inline VectorValidValueIterator<T> Vector::ValidValues(idx_t count) const {
	return VectorValidValueIterator<T>(*this, count);
}

inline VectorValidityIterator Vector::Validity(idx_t count) const {
	return VectorValidityIterator(*this, count);
}

} // namespace duckdb
