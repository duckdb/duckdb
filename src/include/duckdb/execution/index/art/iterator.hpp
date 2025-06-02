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

	//! Returns true, if key_bytes contains all bytes of key.
	bool Contains(const ARTKey &key) const;
	//! Returns true, if key_bytes is greater than [or equal to] the key.
	bool GreaterThan(const ARTKey &key, const bool equal, const uint8_t nested_depth) const;

private:
	unsafe_vector<uint8_t> key_bytes;
};

class Iterator {
public:
	static constexpr uint8_t ROW_ID_SIZE = sizeof(row_t);

public:
	explicit Iterator(ART &art) : art(art), status(GateStatus::GATE_NOT_SET) {};
	//! Holds the current key leading down to the top node on the stack.
	IteratorKey current_key;

public:
	//! Scans the tree, starting at the current top node on the stack, and ending at upper_bound.
	//! If upper_bound is the empty ARTKey, than there is no upper bound.
	bool Scan(const ARTKey &upper_bound, const idx_t max_count, unsafe_vector<row_t> &row_ids, const bool equal);
	//! Finds the minimum (leaf) of the current subtree.
	void FindMinimum(const Node &node);
	//! Finds the lower bound of the ART and adds the nodes to the stack. Returns false, if the lower
	//! bound exceeds the maximum value of the ART.
	bool LowerBound(const Node &node, const ARTKey &key, const bool equal, idx_t depth);

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

private:
	//! Goes to the next leaf in the ART and sets it as last_leaf,
	//! returns false if there is no next leaf.
	bool Next();
	//! Pop the top node from the stack of iterator entries and adjust the current key.
	void PopNode();
};
} // namespace duckdb
