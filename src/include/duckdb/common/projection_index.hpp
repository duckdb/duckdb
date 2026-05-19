//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/projection_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include <functional>

namespace duckdb {

//! ProjectionIndex refers to an index within the projection list of a node in the planner
struct ProjectionIndex {
	ProjectionIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit ProjectionIndex(idx_t index) : index(index) {
	}

	operator idx_t() const { // NOLINT: allow implicit conversion
		return GetIndex();
	}
	idx_t GetIndex() const {
		if (!IsValid()) {
			throw InternalException("ProjectionIndex::GetIndex called on invalid index");
		}
		return index;
	}
	inline bool operator==(const ProjectionIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator<(const ProjectionIndex &rhs) const {
		return index < rhs.index;
	};
	bool operator!=(const ProjectionIndex &other) const {
		return !(*this == other);
	}
	bool operator>(const ProjectionIndex &other) const {
		return other < *this;
	}
	bool operator<=(const ProjectionIndex &other) const {
		return !(other < *this);
	}
	bool operator>=(const ProjectionIndex &other) const {
		return !(*this < other);
	}
	ProjectionIndex &operator++() {
		index++;
		return *this;
	}
	ProjectionIndex operator++(int) {
		ProjectionIndex tmp(*this);
		index++;
		return tmp;
	}
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}

	struct IndexRange {
		struct Iterator {
			idx_t current;

			explicit Iterator(idx_t val) : current(val) {
			}
			ProjectionIndex operator*() const {
				return ProjectionIndex(current);
			}
			Iterator &operator++() {
				++current;
				return *this;
			}
			bool operator!=(const Iterator &other) const {
				return current != other.current;
			}
		};

		idx_t count;
		explicit IndexRange(idx_t count) : count(count) {
		}
		Iterator begin() const {
			return Iterator(0);
		}
		Iterator end() const {
			return Iterator(count);
		}
	};

	static IndexRange GetIndexes(idx_t count) {
		return IndexRange(count);
	}

	idx_t GetIndexUnsafe() const {
		return index;
	}

private:
	idx_t index;
};

} // namespace duckdb

namespace std {

template <>
struct hash<duckdb::ProjectionIndex> {
	size_t operator()(const duckdb::ProjectionIndex &tbl_index) const {
		return std::hash<uint64_t> {}(tbl_index.GetIndexUnsafe());
	}
};
} // namespace std
