//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/index_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

template <class T, class INDEX_TYPE>
class IndexVector {
public:
	void push_back(T element) {
		internal_vector.push_back(move(element));
	}

	T &operator[](INDEX_TYPE idx) {
		return internal_vector[idx.index];
	}

	const T &operator[](INDEX_TYPE idx) const {
		return internal_vector[idx.index];
	}

	idx_t size() const {
		return internal_vector.size();
	}

	bool empty() const {
		return internal_vector.empty();
	}

	void reserve(idx_t size) {
		internal_vector.reserve(size);
	}

	typename vector<T>::iterator begin() {
		return internal_vector.begin();
	}
	typename vector<T>::iterator end() {
		return internal_vector.end();
	}
	typename vector<T>::const_iterator cbegin() {
		return internal_vector.cbegin();
	}
	typename vector<T>::const_iterator cend() {
		return internal_vector.cend();
	}
	typename vector<T>::const_iterator begin() const {
		return internal_vector.begin();
	}
	typename vector<T>::const_iterator end() const {
		return internal_vector.end();
	}

private:
	vector<T> internal_vector;
};

template <typename T>
using physical_index_vector_t = IndexVector<T, PhysicalIndex>;

template <typename T>
using logical_index_vector_t = IndexVector<T, LogicalIndex>;

} // namespace duckdb
