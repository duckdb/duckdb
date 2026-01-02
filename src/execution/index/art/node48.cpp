#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

void Node48::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	{
		NodeHandle<Node48> handle(art, node);
		auto &n = handle.Get();

		if (n.count != CAPACITY) {
			// Still space. Insert the child.
			// Find an empty position in the node list.
			auto child_pos = n.count;
			if (n.children[child_pos].HasMetadata()) {
				child_pos = 0;
				while (n.children[child_pos].HasMetadata()) {
					child_pos++;
				}
			}

			n.children[child_pos] = child;
			n.child_index[byte] = child_pos;
			n.count++;
			return;
		}
	}

	// The node is full.
	// Grow to Node256.
	auto node48 = node;
	Node256::GrowNode48(art, node, node48);
	Node256::InsertChild(art, node, byte, child);
}

void Node48::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	{
		NodeHandle<Node48> handle(art, node);
		auto &n = handle.Get();

		// Free the child and decrease the count.
		Node::FreeTree(art, n.children[n.child_index[byte]]);
		n.child_index[byte] = EMPTY_MARKER;
		n.count--;

		if (n.count >= SHRINK_THRESHOLD) {
			return;
		}
	}

	// Shrink to Node16.
	auto node48 = node;
	Node16::ShrinkNode48(art, node, node48);
}

void Node48::GrowNode16(ART &art, Node &node48, Node &node16) {
	{
		NodeHandle<Node16> n16_handle(art, node16);
		auto &n16 = n16_handle.Get();

		auto n48_handle = New(art, node48);
		auto &n48 = n48_handle.Get();
		node48.SetGateStatus(node16.GetGateStatus());

		n48.count = n16.count;
		for (uint8_t i = 0; i < n16.count; i++) {
			n48.child_index[n16.key[i]] = i;
			n48.children[i] = n16.children[i];
		}
	}
	Node::FreeNode(art, node16);
}

void Node48::ShrinkNode256(ART &art, Node &node48, Node &node256) {
	{
		auto n48_handle = New(art, node48);
		auto &n48 = n48_handle.Get();
		node48.SetGateStatus(node256.GetGateStatus());

		NodeHandle<Node256> n256_handle(art, node256);
		auto &n256 = n256_handle.Get();

		n48.count = 0;
		for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
			if (!n256.children[i].HasMetadata()) {
				continue;
			}
			n48.child_index[i] = n48.count;
			n48.children[n48.count] = n256.children[i];
			n48.count++;
		}
	}
	Node::FreeNode(art, node256);
}

} // namespace duckdb
