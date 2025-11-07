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
	static uint8_t GetByte(const ART &art, const Node &node, const uint8_t pos);

public:
	//! Get a new list of prefix nodes. The node reference holds the child of the last prefix node.
	static void New(ART &art, reference<Node> &ref, const ARTKey &key, const idx_t depth, idx_t count);

	//! Concatenates parent -> prev_node4 -> child.
	static void Concat(ART &art, Node &parent, Node &node4, const Node child, uint8_t byte,
	                   const GateStatus node4_status);

	//! Removes up to pos bytes from the prefix.
	//! Shifts all subsequent bytes by pos. Frees empty nodes.
	static void Reduce(ART &art, Node &node, const idx_t pos);
	//! Splits the prefix at pos.
	//! node references the node that replaces the split byte.
	//! child references the remaining node after the split.
	//! Returns GATE_SET, if a gate node was freed, else GATE_NOT_SET.
	//! If it returns GATE_SET, then the caller must set the gate for the node replacing the split byte,
	//! after its creation.
	static GateStatus Split(ART &art, reference<Node> &node, Node &child, const uint8_t pos);

	//! Traverses and verifies the node and its subtree
	static void Verify(ART &art, const Node &node);
	//! Transform the child of the node.
	static void TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator);

	//! Returns the string representation of the node at indentation level.
	static string ToString(ART &art, const Node &node, idx_t indent_level, bool inside_gate = false,
	                       bool display_ascii = false);

private:
	static Prefix NewInternal(ART &art, Node &node, const data_ptr_t data, const uint8_t count, const idx_t offset);

	static Prefix GetTail(ART &art, const Node &node);

	static void ConcatInternal(ART &art, Node &parent, Node &node4, const Node child, uint8_t byte,
	                           const bool inside_gate);
	static void ConcatNode4WasGate(ART &art, Node &node4, const Node child, uint8_t byte);
	static void ConcatChildIsGate(ART &art, Node &parent, Node &node4, const Node child, uint8_t byte);
	static void ConcatOutsideGate(ART &art, Node &parent, Node &node4, const Node child, uint8_t byte);

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
