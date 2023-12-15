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

//! Keeps track of the byte leading to the currently active child of the node
struct IteratorEntry {
	IteratorEntry(Node node, uint8_t byte) : node(node), byte(byte) {
	}

	Node node;
	uint8_t byte = 0;
};

//! Keeps track of the current key in the iterator leading down to the top node in the stack
class IteratorKey {
public:
	//! Pushes a byte into the current key
	inline void Push(const uint8_t key_byte) {
		key_bytes.push_back(key_byte);
	}
	//! Pops n bytes from the current key
	inline void Pop(const idx_t n) {
		key_bytes.resize(key_bytes.size() - n);
	}

	//! Subscript operator
	inline uint8_t &operator[](idx_t idx) {
		D_ASSERT(idx < key_bytes.size());
		return key_bytes[idx];
	}
	//! Greater than operator
	bool operator>(const ARTKey &key) const;
	//! Greater than or equal to operator
	bool operator>=(const ARTKey &key) const;
	//! Equal to operator
	bool operator==(const ARTKey &key) const;

private:
	vector<uint8_t> key_bytes;
};

class Iterator {
public:
	//! Holds the current key leading down to the top node on the stack
	IteratorKey current_key;
	//! Pointer to the ART
	optional_ptr<ART> art = nullptr;

	//! Scans the tree, starting at the current top node on the stack, and ending at upper_bound.
	//! If upper_bound is the empty ARTKey, than there is no upper bound
	bool Scan(const ARTKey &upper_bound, const idx_t max_count, vector<row_t> &result_ids, const bool equal);
	//! Finds the minimum (leaf) of the current subtree
	void FindMinimum(const Node &node);
	//! Finds the lower bound of the ART and adds the nodes to the stack. Returns false, if the lower
	//! bound exceeds the maximum value of the ART
	bool LowerBound(const Node &node, const ARTKey &key, const bool equal, idx_t depth);

private:
	//! Stack of nodes from the root to the currently active node
	stack<IteratorEntry> nodes;
	//! Last visited leaf node
	Node last_leaf = Node();

	//! Goes to the next leaf in the ART and sets it as last_leaf,
	//! returns false if there is no next leaf
	bool Next();
	//! Pop the top node from the stack of iterator entries and adjust the current key
	void PopNode();
};
} // namespace duckdb
