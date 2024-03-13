#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

Node256 &Node256::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_256).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_256));
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	n256.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n256.children[i].Clear();
	}

	return n256;
}

void Node256::Free(ART &art, Node &node) {

	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	if (!n256.count) {
		return;
	}

	// free all children
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n256.children[i].HasMetadata()) {
			Node::Free(art, n256.children[i]);
		}
	}
}

Node256 &Node256::GrowNode48(ART &art, Node &node256, Node &node48) {

	auto &n48 = Node::RefMutable<Node48>(art, node48, NType::NODE_48);
	auto &n256 = New(art, node256);

	n256.count = n48.count;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			n256.children[i] = n48.children[n48.child_index[i]];
		} else {
			n256.children[i].Clear();
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n256;
}

void Node256::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			children[i].InitializeMerge(art, flags);
		}
	}
}

void Node256::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	// ensure that there is no other child at the same byte
	D_ASSERT(!n256.children[byte].HasMetadata());

	n256.count++;
	D_ASSERT(n256.count <= Node::NODE_256_CAPACITY);
	n256.children[byte] = child;
}

void Node256::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	// free the child and decrease the count
	Node::Free(art, n256.children[byte]);
	n256.count--;

	// shrink node to Node48
	if (n256.count <= Node::NODE_256_SHRINK_THRESHOLD) {
		auto node256 = node;
		Node48::ShrinkNode256(art, node, node256);
	}
}

optional_ptr<const Node> Node256::GetChild(const uint8_t byte) const {
	if (children[byte].HasMetadata()) {
		return &children[byte];
	}
	return nullptr;
}

optional_ptr<Node> Node256::GetChildMutable(const uint8_t byte) {
	if (children[byte].HasMetadata()) {
		return &children[byte];
	}
	return nullptr;
}

optional_ptr<const Node> Node256::GetNextChild(uint8_t &byte) const {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			byte = UnsafeNumericCast<uint8_t>(i);
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node256::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			byte = UnsafeNumericCast<uint8_t>(i);
			return &children[i];
		}
	}
	return nullptr;
}

void Node256::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			children[i].Vacuum(art, flags);
		}
	}
}

} // namespace duckdb
