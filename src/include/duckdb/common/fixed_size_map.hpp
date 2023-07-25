//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/fixed_size_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

template <typename T>
struct fixed_size_map_iterator_t;

template <typename T>
struct const_fixed_size_map_iterator_t;

template <typename T>
class fixed_size_map_t {
	friend struct fixed_size_map_iterator_t<T>;
	friend struct const_fixed_size_map_iterator_t<T>;

public:
	using key_type = idx_t;
	using mapped_type = T;

public:
	explicit fixed_size_map_t(idx_t capacity_p = 0) : capacity(capacity_p) {
		resize(capacity);
	}

	idx_t size() const {
		return count;
	}

	void resize(idx_t capacity_p) {
		capacity = capacity_p;
		occupied = ValidityMask(capacity);
		values = make_unsafe_uniq_array<T>(capacity + 1);
		clear();
	}

	void clear() {
		count = 0;
		occupied.SetAllInvalid(capacity);
	}

	T &operator[](const idx_t &key) {
		D_ASSERT(key < capacity);
		count += 1 - occupied.RowIsValid(key);
		occupied.SetValidUnsafe(key);
		return values[key];
	}

	const T &operator[](const idx_t &key) const {
		D_ASSERT(key < capacity);
		return values[key];
	}

	fixed_size_map_iterator_t<T> begin() {
		return fixed_size_map_iterator_t<T>(begin_internal(), *this);
	}

	const_fixed_size_map_iterator_t<T> begin() const {
		return const_fixed_size_map_iterator_t<T>(begin_internal(), *this);
	}

	fixed_size_map_iterator_t<T> end() {
		return fixed_size_map_iterator_t<T>(capacity, *this);
	}

	const_fixed_size_map_iterator_t<T> end() const {
		return const_fixed_size_map_iterator_t<T>(capacity, *this);
	}

	fixed_size_map_iterator_t<T> find(const idx_t &index) {
		if (occupied.RowIsValid(index)) {
			return fixed_size_map_iterator_t<T>(index, *this);
		} else {
			return end();
		}
	}

	const_fixed_size_map_iterator_t<T> find(const idx_t &index) const {
		if (occupied.RowIsValid(index)) {
			return const_fixed_size_map_iterator_t<T>(index, *this);
		} else {
			return end();
		}
	}

private:
	idx_t begin_internal() const {
		idx_t index;
		for (index = 0; index < capacity; index++) {
			if (occupied.RowIsValid(index)) {
				break;
			}
		}
		return index;
	}

private:
	idx_t capacity;
	idx_t count;

	ValidityMask occupied;
	unsafe_unique_array<T> values;
};

template <typename T>
struct fixed_size_map_iterator_t {
public:
	fixed_size_map_iterator_t(idx_t index_p, fixed_size_map_t<T> &map_p) : map(map_p), current(index_p) {
	}

	fixed_size_map_iterator_t<T> &operator++() {
		for (current++; current < map.capacity; current++) {
			if (map.occupied.RowIsValidUnsafe(current)) {
				break;
			}
		}
		return *this;
	}

	fixed_size_map_iterator_t<T> operator++(int) {
		fixed_size_map_iterator_t<T> tmp = *this;
		++(*this);
		return tmp;
	}

	idx_t &GetKey() {
		return current;
	}

	const idx_t &GetKey() const {
		return current;
	}

	T &GetValue() {
		return map.values[current];
	}

	const T &GetValue() const {
		return map.values[current];
	}

	friend bool operator==(const fixed_size_map_iterator_t<T> &a, const fixed_size_map_iterator_t<T> &b) {
		return a.current == b.current;
	}

	friend bool operator!=(const fixed_size_map_iterator_t<T> &a, const fixed_size_map_iterator_t<T> &b) {
		return !(a == b);
	}

private:
	fixed_size_map_t<T> &map;
	idx_t current;
};

template <typename T>
struct const_fixed_size_map_iterator_t {
public:
	const_fixed_size_map_iterator_t(idx_t index_p, const fixed_size_map_t<T> &map_p) : map(map_p), current(index_p) {
	}

	const_fixed_size_map_iterator_t<T> &operator++() {
		for (current++; current < map.capacity; current++) {
			if (map.occupied.RowIsValidUnsafe(current)) {
				break;
			}
		}
		return *this;
	}

	const_fixed_size_map_iterator_t<T> operator++(int) {
		const_fixed_size_map_iterator_t<T> tmp = *this;
		++(*this);
		return tmp;
	}

	const idx_t &GetKey() const {
		return current;
	}

	const T &GetValue() const {
		return map.values[current];
	}

	friend bool operator==(const const_fixed_size_map_iterator_t<T> &a, const const_fixed_size_map_iterator_t<T> &b) {
		return a.current == b.current;
	}

	friend bool operator!=(const const_fixed_size_map_iterator_t<T> &a, const const_fixed_size_map_iterator_t<T> &b) {
		return !(a == b);
	}

private:
	const fixed_size_map_t<T> &map;
	idx_t current;
};

} // namespace duckdb
