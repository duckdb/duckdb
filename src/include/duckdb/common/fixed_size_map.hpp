//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/fixed_size_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

template <class T>
class fixed_size_map_iterator; // NOLINT: match stl case

template <class T>
class fixed_size_map_const_iterator; // NOLINT: match stl case

template <class T>
class fixed_size_map_t { // NOLINT: match stl case
	friend class fixed_size_map_iterator<T>;
	friend class fixed_size_map_const_iterator<T>;

public:
	using key_type = idx_t;
	using mapped_type = T;
	using iterator = fixed_size_map_iterator<mapped_type>;
	using const_iterator = fixed_size_map_const_iterator<mapped_type>;

public:
	explicit fixed_size_map_t(idx_t capacity_p = 0) : capacity(capacity_p) {
		resize(capacity);
	}

	idx_t size() const { // NOLINT: match stl case
		return count;
	}

	void resize(idx_t capacity_p) { // NOLINT: match stl case
		capacity = capacity_p;
		occupied = ValidityMask(capacity);
		values = make_unsafe_uniq_array_for_override<mapped_type>(capacity + 1);
		clear();
	}

	void clear() { // NOLINT: match stl case
		count = 0;
		occupied.SetAllInvalid(capacity);
	}

	mapped_type &operator[](const key_type &key) {
		D_ASSERT(key < capacity);
		count += 1 - occupied.RowIsValid(key);
		occupied.SetValidUnsafe(key);
		return values[key];
	}

	const mapped_type &operator[](const key_type &key) const {
		D_ASSERT(key < capacity);
		return values[key];
	}

	iterator begin() { // NOLINT: match stl case
		return iterator(begin_internal(), *this);
	}

	const_iterator begin() const { // NOLINT: match stl case
		return const_iterator(begin_internal(), *this);
	}

	iterator end() { // NOLINT: match stl case
		return iterator(capacity, *this);
	}

	const_iterator end() const { // NOLINT: match stl case
		return const_iterator(capacity, *this);
	}

	iterator find(const idx_t &index) { // NOLINT: match stl case
		return occupied.RowIsValid(index) ? iterator(index, *this) : end();
	}

	const_iterator find(const idx_t &index) const { // NOLINT: match stl case
		return occupied.RowIsValid(index) ? const_iterator(index, *this) : end();
	}

private:
	idx_t begin_internal() const { // NOLINT: match stl case
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
	unsafe_unique_array<mapped_type> values;
};

template <class T>
class fixed_size_map_iterator { // NOLINT: match stl case
public:
	using key_type = idx_t;
	using mapped_type = T;

public:
	fixed_size_map_iterator(idx_t index_p, fixed_size_map_t<mapped_type> &map_p) : map(map_p), current(index_p) {
	}

	fixed_size_map_iterator &operator++() {
		for (current++; current < map.capacity; current++) {
			if (map.occupied.RowIsValidUnsafe(current)) {
				break;
			}
		}
		return *this;
	}

	fixed_size_map_iterator operator++(int) {
		fixed_size_map_iterator tmp = *this;
		++(*this);
		return tmp;
	}

	key_type &GetKey() {
		return current;
	}

	const key_type &GetKey() const {
		return current;
	}

	mapped_type &GetValue() {
		return map.values[current];
	}

	const mapped_type &GetValue() const {
		return map.values[current];
	}

	friend bool operator==(const fixed_size_map_iterator &a, const fixed_size_map_iterator &b) {
		return a.current == b.current;
	}

	friend bool operator!=(const fixed_size_map_iterator &a, const fixed_size_map_iterator &b) {
		return !(a == b);
	}

private:
	fixed_size_map_t<mapped_type> &map;
	idx_t current;
};

template <class T>
class fixed_size_map_const_iterator { // NOLINT: match stl case
public:
	using key_type = idx_t;
	using mapped_type = T;

public:
	fixed_size_map_const_iterator(const idx_t index_p, const fixed_size_map_t<mapped_type> &map_p)
	    : map(map_p), current(index_p) {
	}

	fixed_size_map_const_iterator &operator++() {
		for (current++; current < map.capacity; current++) {
			if (map.occupied.RowIsValidUnsafe(current)) {
				break;
			}
		}
		return *this;
	}

	fixed_size_map_const_iterator operator++(int) {
		fixed_size_map_const_iterator tmp = *this;
		++(*this);
		return tmp;
	}

	const key_type &GetKey() const {
		return current;
	}

	const mapped_type &GetValue() const {
		return map.values[current];
	}

	friend bool operator==(const fixed_size_map_const_iterator &a, const fixed_size_map_const_iterator &b) {
		return a.current == b.current;
	}

	friend bool operator!=(const fixed_size_map_const_iterator &a, const fixed_size_map_const_iterator &b) {
		return !(a == b);
	}

private:
	const fixed_size_map_t<mapped_type> &map;
	idx_t current;
};

//! Some helper functors so we can template functions to use either an unordered map or a fixed size map

// LCOV_EXCL_START
template <class MAP_TYPE>
struct UnorderedMapGetter {
private:
	using key_type = typename MAP_TYPE::key_type;
	using mapped_type = typename MAP_TYPE::mapped_type;
	using iterator = typename MAP_TYPE::iterator;
	using const_iterator = typename MAP_TYPE::const_iterator;

public:
	static const key_type &GetKey(iterator &it) {
		return it->first;
	}

	static const key_type &GetKey(const const_iterator &it) {
		return it->first;
	}

	static mapped_type &GetValue(iterator &iterator) {
		return iterator->second;
	}

	static const mapped_type &GetValue(const const_iterator &iterator) {
		return iterator->second;
	}
};

template <class MAP_TYPE>
struct FixedSizeMapGetter {
private:
	using key_type = typename MAP_TYPE::key_type;
	using mapped_type = typename MAP_TYPE::mapped_type;
	using iterator = typename MAP_TYPE::iterator;
	using const_iterator = typename MAP_TYPE::const_iterator;

public:
	static const key_type &GetKey(iterator &it) {
		return it.GetKey();
	}

	static const idx_t &GetKey(const const_iterator &it) {
		return it.GetKey();
	}

	static mapped_type &GetValue(iterator &it) {
		return it.GetValue();
	}

	static const mapped_type &GetValue(const const_iterator &iterator) {
		return iterator.GetValue();
	}
};
// LCOV_EXCL_STOP

} // namespace duckdb
