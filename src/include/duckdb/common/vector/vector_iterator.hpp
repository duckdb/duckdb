//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/vector_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

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
class VectorValueIterator {
public:
	VectorValueIterator(const Vector &vector, idx_t count) : count(count) {
		vector.ToUnifiedFormat(count, format);
		data = UnifiedVectorFormat::GetData<T>(format);
	}

public:
	struct VectorValueEntry {
		VectorValueEntry(const UnifiedVectorFormat &format, const T *data, idx_t index)
		    : format(format), data(data), index(index) {
			sel_index = format.sel->get_index(index);
		}

		const T &GetValue() const {
			D_ASSERT(IsValid());
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
	class VectorIterator {
	public:
		explicit VectorIterator(UnifiedVectorFormat &format, const T *data, idx_t index)
		    : format(format), data(data), index(index) {
		}

	public:
		VectorIterator &operator++() { // NOLINT: match stl API
			++index;
			return *this;
		}
		VectorIterator operator++(int) { // NOLINT: match stl API
			auto tmp = *this;
			++index;
			return tmp;
		}
		VectorIterator &operator--() { // NOLINT: match stl API
			--index;
			return *this;
		}
		VectorIterator &operator+=(idx_t n) {
			index += n;
			return *this;
		}
		VectorIterator &operator-=(idx_t n) {
			index -= n;
			return *this;
		}
		VectorIterator operator+(idx_t n) const {
			return VectorIterator(format, data, index + n);
		}
		VectorIterator operator-(idx_t n) const {
			return VectorIterator(format, data, index - n);
		}
		int64_t operator-(const VectorIterator &other) const {
			return static_cast<int64_t>(index) - static_cast<int64_t>(other.index);
		}
		bool operator==(const VectorIterator &other) const {
			return index == other.index;
		}
		bool operator!=(const VectorIterator &other) const {
			return index != other.index;
		}
		bool operator<(const VectorIterator &other) const {
			return index < other.index;
		}
		bool operator<=(const VectorIterator &other) const {
			return index <= other.index;
		}
		bool operator>(const VectorIterator &other) const {
			return index > other.index;
		}
		bool operator>=(const VectorIterator &other) const {
			return index >= other.index;
		}
		VectorValueEntry operator*() const {
			return GetEntry(index);
		}
		VectorValueEntry operator[](idx_t n) const {
			return GetEntry(index + n);
		}

	private:
		VectorValueEntry GetEntry(idx_t i) const {
			return VectorValueEntry(format, data, i);
		}

	private:
		UnifiedVectorFormat &format;
		const T *data;
		idx_t index;
	};

public:
	VectorIterator begin() { // NOLINT: match stl API
		return VectorIterator(format, data, 0);
	}
	VectorIterator end() { // NOLINT: match stl API
		return VectorIterator(format, data, count);
	}
	idx_t size() const {
		return count;
	}
	VectorValueEntry operator[](idx_t i) const {
		return VectorValueEntry(format, data, i);
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
inline VectorValueIterator<T> Vector::Values(idx_t count) const {
	return VectorValueIterator<T>(*this, count);
}

template <class T>
inline VectorValidValueIterator<T> Vector::ValidValues(idx_t count) const {
	return VectorValidValueIterator<T>(*this, count);
}

inline VectorValidityIterator Vector::Validity(idx_t count) const {
	return VectorValidityIterator(*this, count);
}

} // namespace duckdb
