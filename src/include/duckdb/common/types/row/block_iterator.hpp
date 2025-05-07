//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/block_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "tuple_data_collection.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

class TupleDataCollection;

enum class BlockIteratorStateType : int8_t {
	//! For fixed_in_memory_block_iterator_state_t
	FIXED_IN_MEMORY,
	//! For variable_in_memory_block_iterator_state_t
	VARIABLE_IN_MEMORY,
	//! For fixed_external_block_iterator_state_t
	FIXED_EXTERNAL,
	//! For variable_external_block_iterator_state_t
	VARIABLE_EXTERNAL,
};

BlockIteratorStateType GetBlockIteratorStateType(const bool &fixed_blocks, const bool &external);

class FixedInMemoryBlockIteratorState {
public:
	explicit FixedInMemoryBlockIteratorState(const TupleDataCollection &key_data);

public:
	template <class T>
	T &GetValueAtIndex(const idx_t &block_idx, const idx_t &tuple_idx) const {
		return reinterpret_cast<T *const>(block_ptrs[block_idx])[tuple_idx];
	}

	template <class T>
	T &GetValueAtIndex(const idx_t &n) const {
		D_ASSERT(n < tuple_count);
		const auto quotient = fast_mod.Div(n);
		return GetValueAtIndex<T>(quotient, fast_mod.Mod(n, quotient));
	}

	void RandomAccess(idx_t &block_idx, idx_t &tuple_idx, const idx_t &index) const {
		block_idx = fast_mod.Div(index);
		tuple_idx = fast_mod.Mod(index, block_idx);
	}

	void Add(idx_t &block_idx, idx_t &tuple_idx, const idx_t &value) const {
		tuple_idx += value;
		if (tuple_idx >= fast_mod.GetDivisor()) {
			const auto div = fast_mod.Div(tuple_idx);
			tuple_idx -= div * fast_mod.GetDivisor();
			block_idx += div;
		}
	}

	void Subtract(idx_t &block_idx, idx_t &tuple_idx, const idx_t &value) const {
		tuple_idx -= value;
		if (tuple_idx >= fast_mod.GetDivisor()) {
			const auto div = fast_mod.Div(-tuple_idx);
			tuple_idx += (div + 1) * fast_mod.GetDivisor();
			block_idx -= div + 1;
		}
	}

	void Increment(idx_t &block_idx, idx_t &tuple_idx) const {
		const auto passed_boundary = ++tuple_idx == fast_mod.GetDivisor();
		block_idx += passed_boundary;
		tuple_idx *= !passed_boundary;
	}

	void Decrement(idx_t &block_idx, idx_t &tuple_idx) const {
		const auto crossed_boundary = tuple_idx-- == 0;
		block_idx -= crossed_boundary;
		tuple_idx += crossed_boundary * fast_mod.GetDivisor();
	}

	idx_t GetIndex(const idx_t &block_idx, const idx_t &tuple_idx) const {
		return block_idx * fast_mod.GetDivisor() + tuple_idx;
	}

private:
	static unsafe_vector<data_ptr_t> ConvertBlockPointers(const vector<data_ptr_t> &block_ptrs);

private:
	const unsafe_vector<data_ptr_t> block_ptrs;
	const FastMod<idx_t> fast_mod;
	const idx_t tuple_count;
};

class VariableInMemoryBlockIteratorState {};

class FixedExternalBlockIteratorState {
public:
	explicit FixedExternalBlockIteratorState(TupleDataCollection &key_data,
	                                         optional_ptr<TupleDataCollection> payload_data);

public:
	template <class T>
	T &GetValueAtIndex(const idx_t &chunk_idx, const idx_t &tuple_idx) {
		if (chunk_idx != current_chunk_idx) {
			InitializeChunk<T>(chunk_idx);
		}
		return reinterpret_cast<T *const>(key_ptrs)[tuple_idx];
	}

	template <class T>
	T &GetValueAtIndex(const idx_t &n) {
		D_ASSERT(n < tuple_count);
		return GetValueAtIndex<T>(n / STANDARD_VECTOR_SIZE, n % STANDARD_VECTOR_SIZE);
	}

	void RandomAccess(idx_t &chunk_idx, idx_t &tuple_idx, const idx_t &index) const {
		chunk_idx = index / STANDARD_VECTOR_SIZE;
		tuple_idx = index % STANDARD_VECTOR_SIZE;
	}

	void Add(idx_t &chunk_idx, idx_t &tuple_idx, const idx_t &value) const {
		tuple_idx += value;
		if (tuple_idx >= STANDARD_VECTOR_SIZE) {
			const auto div = tuple_idx / STANDARD_VECTOR_SIZE;
			tuple_idx -= div * STANDARD_VECTOR_SIZE;
			chunk_idx += div;
		}
	}

	void Subtract(idx_t &chunk_idx, idx_t &tuple_idx, const idx_t &value) const {
		tuple_idx -= value;
		if (tuple_idx >= STANDARD_VECTOR_SIZE) {
			const auto div = -tuple_idx / STANDARD_VECTOR_SIZE;
			tuple_idx += (div + 1) * STANDARD_VECTOR_SIZE;
			chunk_idx -= div + 1;
		}
	}

	void Increment(idx_t &chunk_idx, idx_t &tuple_idx) const {
		const auto passed_boundary = ++tuple_idx == STANDARD_VECTOR_SIZE;
		chunk_idx += passed_boundary;
		tuple_idx *= !passed_boundary;
	}

	void Decrement(idx_t &chunk_idx, idx_t &tuple_idx) const {
		const auto crossed_boundary = tuple_idx-- == 0;
		chunk_idx -= crossed_boundary;
		tuple_idx += crossed_boundary * STANDARD_VECTOR_SIZE;
	}

	idx_t GetIndex(const idx_t &chunk_idx, const idx_t &tuple_idx) const {
		return chunk_idx * STANDARD_VECTOR_SIZE + tuple_idx;
	}

private:
	template <class T>
	void InitializeChunk(const idx_t &chunk_idx) {
		key_data.FetchChunk(key_scan_state, 0, chunk_idx, false);
		if (payload_data) {
			const auto chunk_count = payload_data->FetchChunk(payload_scan_state, 0, chunk_idx, false);
			const auto sort_keys = FlatVector::GetData<T *>(key_scan_state.chunk_state.row_locations);
			const auto payload_ptrs = FlatVector::GetData<data_ptr_t>(payload_scan_state.chunk_state.row_locations);
			for (idx_t i = 0; i < chunk_count; i++) {
				sort_keys[i]->SetPayload(payload_ptrs[i]);
			}
		}
		current_chunk_idx = chunk_idx;
	}

private:
	const idx_t tuple_count;
	idx_t current_chunk_idx;

	TupleDataCollection &key_data;
	TupleDataScanState key_scan_state;
	data_ptr_t *key_ptrs;

	optional_ptr<TupleDataCollection> payload_data;
	TupleDataScanState payload_scan_state;
};

class VariableExternalBlockIteratorState {};

//! Utility so we can get the state using the type
template <BlockIteratorStateType T>
using BlockIteratorState = typename std::conditional<
    T == BlockIteratorStateType::FIXED_IN_MEMORY, FixedInMemoryBlockIteratorState,
    typename std::conditional<
        T == BlockIteratorStateType::VARIABLE_IN_MEMORY, VariableInMemoryBlockIteratorState,
        typename std::conditional<T == BlockIteratorStateType::FIXED_EXTERNAL, FixedExternalBlockIteratorState,
                                  typename std::conditional<T == BlockIteratorStateType::VARIABLE_EXTERNAL,
                                                            VariableExternalBlockIteratorState,
                                                            void // Throws error if we get here
                                                            >::type>::type>::type>::type;

//! Iterator for data spread out over multiple blocks
template <class STATE, class T>
class block_iterator_t { // NOLINT: match stl case
public:
	using iterator_category = std::random_access_iterator_tag;
	using value_type = T;
	using pointer = value_type *;
	using reference = value_type &;
	using difference_type = idx_t;
	using traits = std::iterator_traits<block_iterator_t>;

public:
	explicit block_iterator_t(STATE &state_p) : state(&state_p), block_idx(0), tuple_idx(0) {
	}

	explicit block_iterator_t() : state(nullptr), block_idx(0), tuple_idx(0) {
	}

	block_iterator_t(STATE &state_p, const idx_t &index) : state(&state_p) {
		state->RandomAccess(block_idx, tuple_idx, index);
	}

	block_iterator_t(STATE &state_p, const idx_t &block_idx_p, const idx_t &tuple_idx_p)
	    : state(&state_p), block_idx(block_idx_p), tuple_idx(tuple_idx_p) {
	}

	block_iterator_t(const block_iterator_t &other)
	    : state(other.state), block_idx(other.block_idx), tuple_idx(other.tuple_idx) {
	}

	block_iterator_t &operator=(const block_iterator_t &other) {
		D_ASSERT(RefersToSameObject(state, other.state));
		block_idx = other.block_idx;
		tuple_idx = other.tuple_idx;
		return *this;
	}

public:
	//! (De-)referencing
	reference operator*() const {
		return state->template GetValueAtIndex<T>(block_idx, tuple_idx);
	}
	pointer operator->() const {
		return &operator*();
	}

	//! Prefix and postfix increment and decrement
	block_iterator_t &operator++() {
		state->Increment(block_idx, tuple_idx);
		return *this;
	}
	block_iterator_t operator++(int) {
		block_iterator_t tmp = *this;
		++(*this);
		return tmp;
	}
	block_iterator_t &operator--() {
		state->Decrement(block_idx, tuple_idx);
		return *this;
	}
	block_iterator_t operator--(int) {
		block_iterator_t tmp = *this;
		--(*this);
		return tmp;
	}

	//! Random access
	block_iterator_t &operator+=(const difference_type &n) {
		state->Add(block_idx, tuple_idx, n);
		return *this;
	}
	block_iterator_t &operator-=(const difference_type &n) {
		state->Subtract(block_idx, tuple_idx, n);
		return *this;
	}
	block_iterator_t operator+(const difference_type &n) const {
		idx_t new_block_idx = block_idx;
		idx_t new_tuple_idx = tuple_idx;
		state->Add(new_block_idx, new_tuple_idx, n);
		return block_iterator_t(*state, new_block_idx, new_tuple_idx);
	}
	block_iterator_t operator-(const difference_type &n) const {
		idx_t new_block_idx = block_idx;
		idx_t new_tuple_idx = tuple_idx;
		state->Subtract(new_block_idx, new_tuple_idx, n);
		return block_iterator_t(*state, new_block_idx, new_tuple_idx);
	}

	reference operator[](const difference_type &n) const {
		return state->template GetValueAtIndex<T>(n);
	}

	//! Difference between iterators
	difference_type operator-(const block_iterator_t &other) const {
		return state->GetIndex(block_idx, tuple_idx) - other.state->GetIndex(other.block_idx, other.tuple_idx);
	}

	//! Comparison operators
	bool operator==(const block_iterator_t &other) const {
		return block_idx == other.block_idx && tuple_idx == other.tuple_idx;
	}
	bool operator!=(const block_iterator_t &other) const {
		return block_idx != other.block_idx || tuple_idx != other.tuple_idx;
	}
	bool operator<(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx < other.tuple_idx : block_idx < other.block_idx;
	}
	bool operator>(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx > other.tuple_idx : block_idx > other.block_idx;
	}
	bool operator<=(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx <= other.tuple_idx : block_idx <= other.block_idx;
	}
	bool operator>=(const block_iterator_t &other) const {
		return block_idx == other.block_idx ? tuple_idx >= other.tuple_idx : block_idx >= other.block_idx;
	}

private:
	STATE *state;
	idx_t block_idx; // Or chunk index
	idx_t tuple_idx;
};

} // namespace duckdb
