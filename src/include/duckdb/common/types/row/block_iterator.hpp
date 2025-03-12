//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/block_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Shared state for block iterators that iterate over the same data
template <class T>
class block_iterator_state_t { // NOLINT: not using camelcase on purpose here
	using value_type = T;

public:
	block_iterator_state_t(const vector<data_ptr_t> &block_ptrs_p, const idx_t &tuples_per_block,
	                       const idx_t &tuple_count_p)
	    : block_ptrs(ConvertBlockPointers(block_ptrs_p)), fast_mod(tuples_per_block), tuple_count(tuple_count_p) {
	}

private:
	static unsafe_vector<value_type *const> ConvertBlockPointers(const vector<data_ptr_t> &block_ptrs_p) {
		unsafe_vector<value_type *const> converted_block_ptrs;
		converted_block_ptrs.reserve(block_ptrs_p.size());
		for (const auto &block_ptr : block_ptrs_p) {
			converted_block_ptrs.emplace_back(reinterpret_cast<value_type *const>(block_ptr));
		}
		return converted_block_ptrs;
	}

public:
	const unsafe_vector<value_type *const> block_ptrs;
	const FastMod<idx_t> fast_mod;
	const idx_t tuple_count;
};

//! Iterator for data spread out over multiple blocks
template <class T>
class block_iterator_t { // NOLINT: not using camelcase on purpose here
public:
	using iterator_category = std::random_access_iterator_tag;
	using value_type = T;
	using pointer = value_type *;
	using reference = value_type &;
	using difference_type = idx_t;
	using traits = std::iterator_traits<block_iterator_t>;

public:
	block_iterator_t(const block_iterator_state_t<value_type> &state_p, const difference_type &index_p)
	    : state(state_p), index(index_p) {
	}

	block_iterator_t(const block_iterator_t &other) : state(other.state), index(other.index) {
	}

	block_iterator_t &operator=(const block_iterator_t &other) {
		D_ASSERT(RefersToSameObject(state, other.state));
		index = other.index;
		return *this;
	}

public:
	//! (De-)referencing
	reference operator*() const {
		return operator[](index);
	}
	pointer operator->() const {
		return &operator*();
	}

	//! Prefix and postfix increment and decrement
	block_iterator_t &operator++() {
		index++;
		return *this;
	}
	block_iterator_t operator++(int) {
		block_iterator_t tmp = *this;
		++(*this);
		return tmp;
	}
	block_iterator_t &operator--() {
		index--;
		return *this;
	}
	block_iterator_t operator--(int) {
		block_iterator_t tmp = *this;
		--(*this);
		return tmp;
	}

	//! Random access
	block_iterator_t &operator+=(const difference_type &n) {
		index += n;
		return *this;
	}
	block_iterator_t &operator-=(const difference_type &n) {
		D_ASSERT(index >= n);
		index -= n;
		return *this;
	}
	block_iterator_t operator+(const difference_type &n) const {
		return block_iterator_t(state, index + n);
	}
	block_iterator_t operator-(const difference_type &n) const {
		D_ASSERT(index >= n);
		return block_iterator_t(state, index - n);
	}

	reference operator[](const difference_type &n) const {
		D_ASSERT(n < state.tuple_count);
		const auto quotient = state.fast_mod.Div(n);
		return state.block_ptrs[quotient][state.fast_mod.Mod(n, quotient)];
	}

	//! Difference between iterators
	difference_type operator-(const block_iterator_t &other) const {
		D_ASSERT(index >= other.index);
		return index - other.index;
	}

	//! Comparison operators
	bool operator==(const block_iterator_t &other) const {
		return index == other.index;
	}
	bool operator!=(const block_iterator_t &other) const {
		return index != other.index;
	}
	bool operator<(const block_iterator_t &other) const {
		return index < other.index;
	}
	bool operator>(const block_iterator_t &other) const {
		return index > other.index;
	}
	bool operator<=(const block_iterator_t &other) const {
		return index <= other.index;
	}
	bool operator>=(const block_iterator_t &other) const {
		return index >= other.index;
	}

private:
	const block_iterator_state_t<value_type> &state;
	difference_type index;
};

} // namespace duckdb
