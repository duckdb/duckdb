#include "duckdb/execution/index/art/node16.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

Node16 &Node16::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::NODE_16).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_16));
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	n16.count = 0;
	return n16;
}

void Node16::Free(ART &art, Node &node) {
	D_ASSERT(node.HasMetadata());
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	// Free all children.
	for (idx_t i = 0; i < n16.count; i++) {
		Node::Free(art, n16.children[i]);
	}
}

Node16 &Node16::GrowNode4(ART &art, Node &node16, Node &node4) {
	auto &n4 = Node::RefMutable<Node4>(art, node4, NType::NODE_4);
	auto &n16 = New(art, node16);
	if (node4.IsGate()) {
		node16.SetGate();
	}

	n16.count = n4.count;
	for (idx_t i = 0; i < n4.count; i++) {
		n16.key[i] = n4.key[i];
		n16.children[i] = n4.children[i];
	}

	n4.count = 0;
	Node::Free(art, node4);
	return n16;
}

Node16 &Node16::ShrinkNode48(ART &art, Node &node16, Node &node48) {
	auto &n16 = New(art, node16);
	if (node48.IsGate()) {
		node16.SetGate();
	}
	auto &n48 = Node::RefMutable<Node48>(art, node48, NType::NODE_48);

	n16.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			n16.key[n16.count] = UnsafeNumericCast<uint8_t>(i);
			n16.children[n16.count] = n48.children[n48.child_index[i]];
			n16.count++;
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n16;
}

void Node16::InitializeMerge(ART &art, const unsafe_vector<idx_t> &upper_bounds) {
	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, upper_bounds);
	}
}

void Node16::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	D_ASSERT(node.HasMetadata());
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	// The node is full. Grow to Node48.
	if (n16.count == Node::NODE_16_CAPACITY) {
		auto node16 = node;
		Node48::GrowNode16(art, node, node16);
		Node48::InsertChild(art, node, byte, child);
		return;
	}

	// Still space. Insert the child.
	idx_t child_pos = 0;
	while (child_pos < n16.count && n16.key[child_pos] < byte) {
		child_pos++;
	}

	// Move children backwards to make space.
	for (idx_t i = n16.count; i > child_pos; i--) {
		n16.key[i] = n16.key[i - 1];
		n16.children[i] = n16.children[i - 1];
	}

	n16.key[child_pos] = byte;
	n16.children[child_pos] = child;
	n16.count++;
}

void Node16::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	D_ASSERT(node.HasMetadata());
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	idx_t child_pos = 0;
	for (; child_pos < n16.count; child_pos++) {
		if (n16.key[child_pos] == byte) {
			break;
		}
	}

	D_ASSERT(child_pos < n16.count);

	// Free the child and decrease the count.
	Node::Free(art, n16.children[child_pos]);
	n16.count--;

	// Possibly move children backwards.
	for (idx_t i = child_pos; i < n16.count; i++) {
		n16.key[i] = n16.key[i + 1];
		n16.children[i] = n16.children[i + 1];
	}

	// Shrink node to Node4.
	if (n16.count < Node::NODE_4_CAPACITY) {
		auto node16 = node;
		Node4::ShrinkNode16(art, node, node16);
	}
}

void Node16::ReplaceChild(const uint8_t byte, const Node child) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			auto was_gate = children[i].IsGate();
			children[i] = child;

			if (was_gate && child.HasMetadata()) {
				children[i].SetGate();
			}
			return;
		}
	}
}

const Node *Node16::GetChild(const uint8_t byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

Node *Node16::GetChildMutable(const uint8_t byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

const Node *Node16::GetNextChild(uint8_t &byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

Node *Node16::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

void Node16::Vacuum(ART &art, const unordered_set<uint8_t> &indexes) {
	for (idx_t i = 0; i < count; i++) {
		children[i].Vacuum(art, indexes);
	}
}

void Node16::TransformToDeprecated(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	for (idx_t i = 0; i < count; i++) {
		Node::TransformToDeprecated(art, children[i], allocator);
	}
}

} // namespace duckdb
