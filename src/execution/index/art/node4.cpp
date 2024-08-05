#include "duckdb/execution/index/art/node4.hpp"

#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/leaf.hpp"

namespace duckdb {

Node4 &Node4::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::NODE_4).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_4));
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	n4.count = 0;
	return n4;
}

void Node4::Free(ART &art, Node &node) {
	D_ASSERT(node.HasMetadata());
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	// Free all children.
	for (idx_t i = 0; i < n4.count; i++) {
		Node::Free(art, n4.children[i]);
	}
}

Node4 &Node4::ShrinkNode16(ART &art, Node &node4, Node &node16) {
	auto &n4 = New(art, node4);
	if (node16.IsGate()) {
		node4.SetGate();
	}
	auto &n16 = Node::RefMutable<Node16>(art, node16, NType::NODE_16);

	n4.count = n16.count;
	for (idx_t i = 0; i < n16.count; i++) {
		n4.key[i] = n16.key[i];
		n4.children[i] = n16.children[i];
	}

	n16.count = 0;
	Node::Free(art, node16);
	return n4;
}

void Node4::InitializeMerge(ART &art, const ARTFlags &flags) {
	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, flags);
	}
}

void Node4::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	D_ASSERT(node.HasMetadata());
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	// The node is full. Grow to Node16.
	if (n4.count == Node::NODE_4_CAPACITY) {
		auto node4 = node;
		Node16::GrowNode4(art, node, node4);
		Node16::InsertChild(art, node, byte, child);
		return;
	}

	// Still space. Insert the child.
	idx_t child_pos = 0;
	while (child_pos < n4.count && n4.key[child_pos] < byte) {
		child_pos++;
	}

	// Move children backwards to make space.
	for (idx_t i = n4.count; i > child_pos; i--) {
		n4.key[i] = n4.key[i - 1];
		n4.children[i] = n4.children[i - 1];
	}

	n4.key[child_pos] = byte;
	n4.children[child_pos] = child;
	n4.count++;
}

void Node4::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte) {
	D_ASSERT(node.HasMetadata());
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	idx_t child_pos = 0;
	for (; child_pos < n4.count; child_pos++) {
		if (n4.key[child_pos] == byte) {
			break;
		}
	}

	D_ASSERT(child_pos < n4.count);
	D_ASSERT(n4.count > 1);

	// Free the child and decrease the count.
	Node::Free(art, n4.children[child_pos]);
	n4.count--;

	// Possibly move children backwards.
	for (idx_t i = child_pos; i < n4.count; i++) {
		n4.key[i] = n4.key[i + 1];
		n4.children[i] = n4.children[i + 1];
	}

	// Compress one-way nodes.
	if (n4.count == 1) {
		// We track the old node pointer because Concatenate() might overwrite it.
		auto old_n4_node = node;

		// Get the child and concatenate the prefixes.
		auto child = *n4.GetChildMutable(n4.key[0]);
		D_ASSERT(child.HasMetadata());

		// Inline the leaf.
		auto row_id = Prefix::CanInline(art, prefix, node, byte, child);
		if (row_id != -1) {
			Node::Free(art, prefix);
			Leaf::New(prefix, row_id);
			return;
		}
		Prefix::Concat(art, prefix, n4.key[0], node.IsGate(), child);

		n4.count--;
		Node::Free(art, old_n4_node);
	}
}

void Node4::ReplaceChild(const uint8_t byte, const Node child) {
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

const Node *Node4::GetChild(const uint8_t byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

Node *Node4::GetChildMutable(const uint8_t byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

const Node *Node4::GetNextChild(uint8_t &byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

Node *Node4::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

void Node4::Vacuum(ART &art, const ARTFlags &flags) {
	for (idx_t i = 0; i < count; i++) {
		children[i].Vacuum(art, flags);
	}
}

void Node4::TransformToDeprecated(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	for (idx_t i = 0; i < count; i++) {
		Node::TransformToDeprecated(art, children[i], allocator);
	}
}

} // namespace duckdb
