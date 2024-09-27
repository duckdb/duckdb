//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class ARTKey;

//! Prefix is a wrapper class to access a prefix.
//! The prefix contains up to the ART's prefix size bytes and an additional byte for the count.
//! It also contains a Node pointer to a child node.
class Prefix {
public:
	static constexpr NType PREFIX = NType::PREFIX;

	static constexpr uint8_t ROW_ID_SIZE = sizeof(row_t);
	static constexpr uint8_t ROW_ID_COUNT = ROW_ID_SIZE - 1;
	static constexpr uint8_t DEPRECATED_COUNT = 15;
	static constexpr uint8_t METADATA_SIZE = sizeof(Node) + 1;

public:
	Prefix() = delete;
	Prefix(const ART &art, const Node ptr_p, const bool is_mutable = false, const bool set_in_memory = false);
	Prefix(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node ptr_p, const idx_t count);

	data_ptr_t data;
	Node *ptr;
	bool in_memory;

public:
	static inline uint8_t Count(const ART &art) {
		return art.prefix_count;
	}
	static idx_t GetMismatchWithOther(const Prefix &l_prefix, const Prefix &r_prefix, const idx_t max_count);
	static idx_t GetMismatchWithKey(ART &art, const Node &node, const ARTKey &key, idx_t &depth);
	static uint8_t GetByte(const ART &art, const Node &node, const uint8_t pos);

public:
	//! Get a new list of prefix nodes. The node reference holds the last prefix of the list.
	static void New(ART &art, reference<Node> &ref, const ARTKey &key, const idx_t depth, idx_t count);

	//! Free the prefix and its child.
	static void Free(ART &art, Node &node);

	//! Initializes a merge by incrementing the buffer ID of the prefix and its child.
	static void InitializeMerge(ART &art, Node &node, const unsafe_vector<idx_t> &upper_bounds);

	//! Concatenates parent -> byte -> child. Special-handling, if
	//! 1. the byte was in a gate node.
	//! 2. the byte was in PREFIX_INLINED.
	static void Concat(ART &art, Node &parent, uint8_t byte, const GateStatus old_status, const Node &child,
	                   const GateStatus status);

	//! Traverse a prefix and a key until
	//! 1. a non-prefix node.
	//! 2. a mismatching byte.
	//! Early-out, if the next prefix is a gate node.
	static idx_t Traverse(ART &art, reference<const Node> &node, const ARTKey &key, idx_t &depth);
	static idx_t TraverseMutable(ART &art, reference<Node> &node, const ARTKey &key, idx_t &depth);

	//! Traverse two prefixes to find
	//! 1. that they match.
	//! 2. that they mismatch.
	//! 3. that one prefix contains the other prefix.
	static bool Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &pos,
	                     const GateStatus status);

	//! Removes up to pos bytes from the prefix.
	//! Shifts all subsequent bytes by pos. Frees empty nodes.
	static void Reduce(ART &art, Node &node, const idx_t pos);
	//! Splits the prefix at pos.
	//! prefix_node points to the node that replaces the split byte.
	//! child_node points to the remaining node after the split.
	//! Returns INSIDE, if a gate node was freed, else OUTSIDE.
	static GateStatus Split(ART &art, reference<Node> &node, Node &child, const uint8_t pos);

	//! Insert a key into a prefix.
	static bool Insert(ART &art, Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id,
	                   const GateStatus status);

	//! Returns the string representation of the node, or only traverses and verifies the node and its subtree
	static string VerifyAndToString(ART &art, const Node &node, const bool only_verify);
	//! Count the number of prefixes.
	static void VerifyAllocations(ART &art, const Node &node, unordered_map<uint8_t, idx_t> &node_counts);

	//! Vacuum the child of the node.
	static void Vacuum(ART &art, Node &node, const unordered_set<uint8_t> &indexes);

	//! Transform the child of the node.
	static void TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator);

private:
	static Prefix NewInternal(ART &art, Node &node, const data_ptr_t data, const uint8_t count, const idx_t offset,
	                          const NType type);

	static Prefix GetTail(ART &art, const Node &node);

	static void ConcatGate(ART &art, Node &parent, uint8_t byte, const Node &child);
	static void ConcatChildIsGate(ART &art, Node &parent, uint8_t byte, const Node &child);

	Prefix Append(ART &art, const uint8_t byte);
	void Append(ART &art, Node other);
	Prefix TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator, uint8_t byte);

private:
	template <class F, class NODE>
	static void Iterator(ART &art, reference<NODE> &ref, const bool exit_gate, const bool is_mutable, F &&lambda) {
		while (ref.get().HasMetadata() && ref.get().GetType() == PREFIX) {
			Prefix prefix(art, ref, is_mutable);
			lambda(prefix);

			ref = *prefix.ptr;
			if (exit_gate && ref.get().GetGateStatus() == GateStatus::GATE_SET) {
				break;
			}
		}
	}
};
} // namespace duckdb
