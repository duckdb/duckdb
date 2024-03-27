#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

Node48 &Node48::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_48).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_48));
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	n48.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n48.child_index[i] = Node::EMPTY_MARKER;
	}
	for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Clear();
	}

	return n48;
}

void Node48::Free(ART &art, Node &node) {

	D_ASSERT(node.HasMetadata());
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	if (!n48.count) {
		return;
	}

	// free all children
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			Node::Free(art, n48.children[n48.child_index[i]]);
		}
	}
}

Node48 &Node48::GrowNode16(ART &art, Node &node48, Node &node16) {

	auto &n16 = Node::RefMutable<Node16>(art, node16, NType::NODE_16);
	auto &n48 = New(art, node48);

	n48.count = n16.count;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n48.child_index[i] = Node::EMPTY_MARKER;
	}

	for (idx_t i = 0; i < n16.count; i++) {
		n48.child_index[n16.key[i]] = UnsafeNumericCast<uint8_t>(i);
		n48.children[i] = n16.children[i];
	}

	// necessary for faster child insertion/deletion
	for (idx_t i = n16.count; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n16.count = 0;
	Node::Free(art, node16);
	return n48;
}

Node48 &Node48::ShrinkNode256(ART &art, Node &node48, Node &node256) {

	auto &n48 = New(art, node48);
	auto &n256 = Node::RefMutable<Node256>(art, node256, NType::NODE_256);

	n48.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		D_ASSERT(n48.count <= Node::NODE_48_CAPACITY);
		if (n256.children[i].HasMetadata()) {
			n48.child_index[i] = n48.count;
			n48.children[n48.count] = n256.children[i];
			n48.count++;
		} else {
			n48.child_index[i] = Node::EMPTY_MARKER;
		}
	}

	// necessary for faster child insertion/deletion
	for (idx_t i = n48.count; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n256.count = 0;
	Node::Free(art, node256);
	return n48;
}

void Node48::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			children[child_index[i]].InitializeMerge(art, flags);
		}
	}
}

void Node48::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.HasMetadata());
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	// ensure that there is no other child at the same byte
	D_ASSERT(n48.child_index[byte] == Node::EMPTY_MARKER);

	// insert new child node into node
	if (n48.count < Node::NODE_48_CAPACITY) {
		// still space, just insert the child
		idx_t child_pos = n48.count;
		if (n48.children[child_pos].HasMetadata()) {
			// find an empty position in the node list if the current position is occupied
			child_pos = 0;
			while (n48.children[child_pos].HasMetadata()) {
				child_pos++;
			}
		}
		n48.children[child_pos] = child;
		n48.child_index[byte] = UnsafeNumericCast<uint8_t>(child_pos);
		n48.count++;

	} else {
		// node is full, grow to Node256
		auto node48 = node;
		Node256::GrowNode48(art, node, node48);
		Node256::InsertChild(art, node, byte, child);
	}
}

void Node48::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.HasMetadata());
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	// free the child and decrease the count
	Node::Free(art, n48.children[n48.child_index[byte]]);
	n48.child_index[byte] = Node::EMPTY_MARKER;
	n48.count--;

	// shrink node to Node16
	if (n48.count < Node::NODE_48_SHRINK_THRESHOLD) {
		auto node48 = node;
		Node16::ShrinkNode48(art, node, node48);
	}
}

optional_ptr<const Node> Node48::GetChild(const uint8_t byte) const {
	if (child_index[byte] != Node::EMPTY_MARKER) {
		D_ASSERT(children[child_index[byte]].HasMetadata());
		return &children[child_index[byte]];
	}
	return nullptr;
}

optional_ptr<Node> Node48::GetChildMutable(const uint8_t byte) {
	if (child_index[byte] != Node::EMPTY_MARKER) {
		D_ASSERT(children[child_index[byte]].HasMetadata());
		return &children[child_index[byte]];
	}
	return nullptr;
}

optional_ptr<const Node> Node48::GetNextChild(uint8_t &byte) const {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			byte = UnsafeNumericCast<uint8_t>(i);
			D_ASSERT(children[child_index[i]].HasMetadata());
			return &children[child_index[i]];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node48::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			byte = UnsafeNumericCast<uint8_t>(i);
			D_ASSERT(children[child_index[i]].HasMetadata());
			return &children[child_index[i]];
		}
	}
	return nullptr;
}

void Node48::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			children[child_index[i]].Vacuum(art, flags);
		}
	}
}

} // namespace duckdb
