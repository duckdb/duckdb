//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/block_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

class TupleDataCollection;

enum class BlockIteratorStateType : int8_t {
	//! For InMemoryBlockIteratorState
	IN_MEMORY,
	//! For ExternalBlockIteratorState
	EXTERNAL,
};

BlockIteratorStateType GetBlockIteratorStateType(const bool &external);

//! State for iterating over blocks of an in-memory TupleDataCollection
//! Multiple iterators can share the same state, everything is const
class InMemoryBlockIteratorState {
public:
	explicit InMemoryBlockIteratorState(const TupleDataCollection &key_data);

public:
	template <class T>
	T &GetValueAtIndex(const idx_t &block_idx, const idx_t &tuple_idx) const {
		D_ASSERT(GetIndex(block_idx, tuple_idx) < tuple_count);
		return reinterpret_cast<T *const>(block_ptrs[block_idx])[tuple_idx];
	}

	template <class T>
	T &GetValueAtIndex(const idx_t &n) const {
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
		// Might be able to do this more efficiently but at least this is correct
		const auto n = block_idx * fast_mod.GetDivisor() + tuple_idx - value;
		RandomAccess(block_idx, tuple_idx, n);
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

	void SetKeepPinned(const bool &) {
		// NOP
	}

	void SetPinPayload(const bool &) {
		// NOP
	}

private:
	static unsafe_vector<data_ptr_t> ConvertBlockPointers(const vector<data_ptr_t> &block_ptrs);

private:
	const unsafe_vector<data_ptr_t> block_ptrs;
	const FastMod<idx_t> fast_mod;
	const idx_t tuple_count;
};

//! State for iterating over blocks of an external (larger-than-memory) TupleDataCollection
//! This state cannot be shared by multiple iterators, it is stateful
class ExternalBlockIteratorState {
public:
	explicit ExternalBlockIteratorState(TupleDataCollection &key_data, optional_ptr<TupleDataCollection> payload_data);

public:
	template <class T>
	T &GetValueAtIndex(const idx_t &chunk_idx, const idx_t &tuple_idx) {
		if (chunk_idx != current_chunk_idx) {
			InitializeChunk<T>(chunk_idx);
		}
		return *reinterpret_cast<T **const>(key_ptrs)[tuple_idx];
	}

	template <class T>
	T &GetValueAtIndex(const idx_t &n) {
		D_ASSERT(n < tuple_count);
		return GetValueAtIndex<T>(n / STANDARD_VECTOR_SIZE, n % STANDARD_VECTOR_SIZE);
	}

	static void RandomAccess(idx_t &chunk_idx, idx_t &tuple_idx, const idx_t &index) {
		chunk_idx = index / STANDARD_VECTOR_SIZE;
		tuple_idx = index % STANDARD_VECTOR_SIZE;
	}

	static void Add(idx_t &chunk_idx, idx_t &tuple_idx, const idx_t &value) {
		tuple_idx += value;
		if (tuple_idx >= STANDARD_VECTOR_SIZE) {
			const auto div = tuple_idx / STANDARD_VECTOR_SIZE;
			tuple_idx -= div * STANDARD_VECTOR_SIZE;
			chunk_idx += div;
		}
	}

	static void Subtract(idx_t &chunk_idx, idx_t &tuple_idx, const idx_t &value) {
		tuple_idx -= value;
		if (tuple_idx >= STANDARD_VECTOR_SIZE) {
			const auto div = -tuple_idx / STANDARD_VECTOR_SIZE;
			tuple_idx += (div + 1) * STANDARD_VECTOR_SIZE;
			chunk_idx -= div + 1;
		}
	}

	static void Increment(idx_t &chunk_idx, idx_t &tuple_idx) {
		const auto passed_boundary = ++tuple_idx == STANDARD_VECTOR_SIZE;
		chunk_idx += passed_boundary;
		tuple_idx *= !passed_boundary;
	}

	static void Decrement(idx_t &chunk_idx, idx_t &tuple_idx) {
		const auto crossed_boundary = tuple_idx-- == 0;
		chunk_idx -= crossed_boundary;
		tuple_idx += crossed_boundary * static_cast<idx_t>(STANDARD_VECTOR_SIZE);
	}

	static idx_t GetIndex(const idx_t &chunk_idx, const idx_t &tuple_idx) {
		return chunk_idx * STANDARD_VECTOR_SIZE + tuple_idx;
	}

	void SetKeepPinned(const bool &enable) {
		keep_pinned = enable;
		// Always start with a clean slate when toggling
		pins.clear();
		key_scan_state.pin_state.row_handles.clear();
		key_scan_state.pin_state.heap_handles.clear();
		payload_scan_state.pin_state.row_handles.clear();
		payload_scan_state.pin_state.heap_handles.clear();
		current_chunk_idx = DConstants::INVALID_INDEX;
	}

	void SetPinPayload(const bool &enable) {
		pin_payload = enable;
	}

private:
	template <class T>
	void InitializeChunk(const idx_t &chunk_idx) {
		current_chunk_idx = chunk_idx;
		if (keep_pinned) {
			key_scan_state.pin_state.row_handles.acquire_handles(pins);
			key_scan_state.pin_state.heap_handles.acquire_handles(pins);
		}
		key_data.FetchChunk(key_scan_state, 0, chunk_idx, false);
		if (pin_payload && payload_data) {
			if (keep_pinned) {
				payload_scan_state.pin_state.row_handles.acquire_handles(pins);
				payload_scan_state.pin_state.heap_handles.acquire_handles(pins);
			}
			const auto chunk_count = payload_data->FetchChunk(payload_scan_state, 0, chunk_idx, false);
			const auto sort_keys = reinterpret_cast<T **const>(key_ptrs);
			payload_data->FetchChunk(payload_scan_state, 0, chunk_idx, false);
			const auto payload_ptrs = FlatVector::GetData<data_ptr_t>(payload_scan_state.chunk_state.row_locations);
			for (idx_t i = 0; i < chunk_count; i++) {
				sort_keys[i]->SetPayload(payload_ptrs[i]);
				D_ASSERT(GetValueAtIndex<T>(chunk_idx, i).GetPayload() == payload_ptrs[i]);
			}
		}
	}

private:
	const idx_t tuple_count;
	idx_t current_chunk_idx;

	TupleDataCollection &key_data;
	TupleDataScanState key_scan_state;
	data_ptr_t *key_ptrs;

	optional_ptr<TupleDataCollection> payload_data;
	TupleDataScanState payload_scan_state;

	bool keep_pinned;
	bool pin_payload;
	vector<BufferHandle> pins;
};

//! Utility so we can get the state using the type
template <BlockIteratorStateType T>
using BlockIteratorState = typename std::conditional<
    T == BlockIteratorStateType::IN_MEMORY, InMemoryBlockIteratorState,
    typename std::conditional<T == BlockIteratorStateType::EXTERNAL, ExternalBlockIteratorState,
                              void // Compiler throws error if we get here
                              >::type>::type;

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
	explicit block_iterator_t(STATE &state_p) : state(&state_p), block_or_chunk_idx(0), tuple_idx(0) {
	}

	explicit block_iterator_t()
	    : state(nullptr), block_or_chunk_idx(DConstants::INVALID_INDEX), tuple_idx(DConstants::INVALID_INDEX) {
		// Ideally, we wouldn't have a default constructor, because we always need a state.
		// However, some sorting algorithms use the default constructor before initializing, so we need this
	}

	block_iterator_t(STATE &state_p, const idx_t &index) : state(&state_p) { // NOLINT: uninitialized on purpose
		state->RandomAccess(block_or_chunk_idx, tuple_idx, index);
	}

	block_iterator_t(STATE &state_p, const idx_t &block_idx_p, const idx_t &tuple_idx_p)
	    : state(&state_p), block_or_chunk_idx(block_idx_p), tuple_idx(tuple_idx_p) {
	}

	block_iterator_t(const block_iterator_t &other)
	    : state(other.state), block_or_chunk_idx(other.block_or_chunk_idx), tuple_idx(other.tuple_idx) {
	}

	block_iterator_t &operator=(const block_iterator_t &other) {
		D_ASSERT(!state || RefersToSameObject(*state, *other.state));
		if (this != &other) { // This check is needed to shut clang-tidy up
			block_or_chunk_idx = other.block_or_chunk_idx;
			tuple_idx = other.tuple_idx;
		}
		return *this;
	}

public:
	//! (De-)referencing
	reference operator*() const {
		return state->template GetValueAtIndex<T>(block_or_chunk_idx, tuple_idx);
	}
	pointer operator->() const {
		return &operator*();
	}

	//! Prefix and postfix increment and decrement
	block_iterator_t &operator++() {
		state->Increment(block_or_chunk_idx, tuple_idx);
		return *this;
	}
	block_iterator_t operator++(int) {
		block_iterator_t tmp = *this;
		++(*this);
		return tmp;
	}
	block_iterator_t &operator--() {
		state->Decrement(block_or_chunk_idx, tuple_idx);
		return *this;
	}
	block_iterator_t operator--(int) {
		block_iterator_t tmp = *this;
		--(*this);
		return tmp;
	}

	//! Random access
	block_iterator_t &operator+=(const difference_type &n) {
		state->Add(block_or_chunk_idx, tuple_idx, n);
		return *this;
	}
	block_iterator_t &operator-=(const difference_type &n) {
		state->Subtract(block_or_chunk_idx, tuple_idx, n);
		return *this;
	}
	block_iterator_t operator+(const difference_type &n) const {
		idx_t new_block_idx = block_or_chunk_idx;
		idx_t new_tuple_idx = tuple_idx;
		state->Add(new_block_idx, new_tuple_idx, n);
		return block_iterator_t(*state, new_block_idx, new_tuple_idx);
	}
	block_iterator_t operator-(const difference_type &n) const {
		idx_t new_block_idx = block_or_chunk_idx;
		idx_t new_tuple_idx = tuple_idx;
		state->Subtract(new_block_idx, new_tuple_idx, n);
		return block_iterator_t(*state, new_block_idx, new_tuple_idx);
	}

	reference operator[](const difference_type &n) const {
		return state->template GetValueAtIndex<T>(n);
	}

	//! Difference between iterators
	difference_type operator-(const block_iterator_t &other) const {
		return state->GetIndex(block_or_chunk_idx, tuple_idx) -
		       other.state->GetIndex(other.block_or_chunk_idx, other.tuple_idx);
	}

	//! Comparison operators
	bool operator==(const block_iterator_t &other) const {
		return block_or_chunk_idx == other.block_or_chunk_idx && tuple_idx == other.tuple_idx;
	}
	bool operator!=(const block_iterator_t &other) const {
		return block_or_chunk_idx != other.block_or_chunk_idx || tuple_idx != other.tuple_idx;
	}
	bool operator<(const block_iterator_t &other) const {
		return block_or_chunk_idx == other.block_or_chunk_idx ? tuple_idx < other.tuple_idx
		                                                      : block_or_chunk_idx < other.block_or_chunk_idx;
	}
	bool operator>(const block_iterator_t &other) const {
		return block_or_chunk_idx == other.block_or_chunk_idx ? tuple_idx > other.tuple_idx
		                                                      : block_or_chunk_idx > other.block_or_chunk_idx;
	}
	bool operator<=(const block_iterator_t &other) const {
		return block_or_chunk_idx == other.block_or_chunk_idx ? tuple_idx <= other.tuple_idx
		                                                      : block_or_chunk_idx <= other.block_or_chunk_idx;
	}
	bool operator>=(const block_iterator_t &other) const {
		return block_or_chunk_idx == other.block_or_chunk_idx ? tuple_idx >= other.tuple_idx
		                                                      : block_or_chunk_idx >= other.block_or_chunk_idx;
	}

private:
	STATE *state;
	idx_t block_or_chunk_idx;
	idx_t tuple_idx;
};

} // namespace duckdb
