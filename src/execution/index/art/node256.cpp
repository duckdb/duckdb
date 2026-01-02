#include "duckdb/execution/index/art/node256.hpp"

#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

void Node256::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	NodeHandle<Node256> handle(art, node);
	auto &n = handle.Get();
	n.count++;
	n.children[byte] = child;
}

void Node256::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	{
		NodeHandle<Node256> handle(art, node);
		auto &n = handle.Get();

		// Free the child and decrease the count.
		Node::FreeTree(art, n.children[byte]);
		n.count--;

		if (n.count > SHRINK_THRESHOLD) {
			return;
		}
	}

	// Shrink to Node48.
	auto node256 = node;
	Node48::ShrinkNode256(art, node, node256);
}

void Node256::GrowNode48(ART &art, Node &node256, Node &node48) {
	{
		NodeHandle<Node48> n48_handle(art, node48);
		auto &n48 = n48_handle.Get();

		auto n256_handle = New(art, node256);
		auto &n256 = n256_handle.Get();
		node256.SetGateStatus(node48.GetGateStatus());

		n256.count = n48.count;
		for (uint16_t i = 0; i < CAPACITY; i++) {
			if (n48.child_index[i] != Node48::EMPTY_MARKER) {
				n256.children[i] = n48.children[n48.child_index[i]];
			}
		}
	}
	Node::FreeNode(art, node48);
}

} // namespace duckdb
