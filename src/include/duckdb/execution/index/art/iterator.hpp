//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/iterator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/stack.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

//! Keeps track of the byte leading to the currently active child of the node.
struct IteratorEntry {
	IteratorEntry(Node node, uint8_t byte) : node(node), byte(byte) {
	}

	Node node;
	uint8_t byte = 0;
};

//! Keeps track of the current key in the iterator leading down to the top node in the stack.
class IteratorKey {
public:
	//! Pushes a byte into the current key.
	inline void Push(const uint8_t key_byte) {
		key_bytes.push_back(key_byte);
	}
	//! Pops n bytes from the current key.
	inline void Pop(const idx_t n) {
		key_bytes.resize(key_bytes.size() - n);
	}
	//! Returns the byte at idx.
	inline uint8_t &operator[](idx_t idx) {
		D_ASSERT(idx < key_bytes.size());
		return key_bytes[idx];
	}
	// Returns the number of key bytes.
	inline idx_t Size() const {
		return key_bytes.size();
	}
	//! Returns a pointer to the key bytes.
	inline const_data_ptr_t Data() const {
		return const_data_ptr_cast(key_bytes.data());
	}

	//! Returns true, if key_bytes contains all bytes of key.
	bool Contains(const ARTKey &key) const;
	//! Returns true, if key_bytes is greater than [or equal to] the key.
	bool GreaterThan(const ARTKey &key, const bool equal, const uint8_t nested_depth) const;

private:
	unsafe_vector<uint8_t> key_bytes;
};

//===--------------------------------------------------------------------===//
// Scan Output Policies
//===--------------------------------------------------------------------===//

//! Output policy for scanning row IDs only into a set.
struct RowIdSetOutput {
	set<row_t> &row_ids;

	explicit RowIdSetOutput(set<row_t> &row_ids_p) : row_ids(row_ids_p) {
	}

	bool IsFull(idx_t max_count) const {
		return row_ids.size() >= max_count;
	}
	void SetKey(const IteratorKey &, idx_t) {
		// No-op: we don't need keys for row ID output.
	}
	void Emit(row_t rid) {
		row_ids.insert(rid);
	}
};

//! Output policy for scanning keys and row IDs into vectors.
struct KeyVectorOutput {
	ArenaAllocator &arena;
	unsafe_vector<ARTKey> &keys;
	unsafe_vector<ARTKey> &row_id_keys;
	idx_t count = 0;
	const_data_ptr_t key_data = nullptr;
	idx_t key_len = 0;

	KeyVectorOutput(ArenaAllocator &arena_p, unsafe_vector<ARTKey> &keys_p, unsafe_vector<ARTKey> &row_id_keys_p)
	    : arena(arena_p), keys(keys_p), row_id_keys(row_id_keys_p) {
	}

	idx_t Count() const {
		return count;
	}
	void Reset() {
		count = 0;
		arena.Reset();
	}
	bool IsFull(idx_t max_count) const {
		return count >= max_count;
	}
	void SetKey(const IteratorKey &current_key, idx_t column_key_len) {
		key_data = current_key.Data();
		key_len = column_key_len;
	}
	void Emit(row_t rid) {
		keys[count] = ARTKey::CreateKeyFromBytes(arena, key_data, key_len);
		row_id_keys[count] = ARTKey::CreateARTKey<row_t>(arena, rid);
		count++;
	}
};

class Iterator {
public:
	static constexpr uint8_t ROW_ID_SIZE = sizeof(row_t);

public:
	explicit Iterator(ART &art) : art(art), status(GateStatus::GATE_NOT_SET) {};
	//! Holds the current key leading down to the top node on the stack.
	IteratorKey current_key;

public:
	//! Templated scan implementation. Output policy defines how results are emitted.
	//! Returns true if scan completed, false if stopped due to max_count.
	template <typename Output>
	bool Scan(const ARTKey &upper_bound, Output &output, idx_t max_count, bool equal);

	//! Finds the minimum (leaf) of the current subtree.
	void FindMinimum(const Node &node);
	//! Finds the lower bound of the ART and adds the nodes to the stack. Returns false, if the lower
	//! bound exceeds the maximum value of the ART.
	bool LowerBound(const Node &node, const ARTKey &key, const bool equal);

	//! Returns the nested depth.
	uint8_t GetNestedDepth() const {
		return nested_depth;
	}

private:
	//! The ART.
	ART &art;
	//! Stack of nodes from the root to the currently active node.
	stack<IteratorEntry> nodes;
	//! Last visited leaf node.
	Node last_leaf = Node();
	//! Holds the row ID of nested leaves.
	uint8_t row_id[ROW_ID_SIZE];
	//! True, if we passed a gate.
	GateStatus status;
	//! Depth in a nested leaf.
	uint8_t nested_depth = 0;
	//! True, if we entered a nested leaf to retrieve the next node.
	bool entered_nested_leaf = false;

	//! State for resuming within a leaf after early return due to max_count.
	//! For LEAF: cached row IDs and current position.
	set<row_t> cached_row_ids;
	set<row_t>::iterator cached_row_ids_it;
	bool has_cached_row_ids = false;
	//! For nested leaves: current byte position and whether we've started.
	uint8_t nested_byte = 0;
	bool nested_started = false;

private:
	//! Goes to the next leaf in the ART and sets it as last_leaf,
	//! returns false if there is no next leaf.
	bool Next();
	//! Pop the top node from the stack of iterator entries and adjust the current key.
	void PopNode();
};
} // namespace duckdb
