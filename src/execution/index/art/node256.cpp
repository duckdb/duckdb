#include "duckdb/execution/index/art/node256.hpp"

#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

Node256 &Node256::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_256).New();
	node.SetType((uint8_t)NType::NODE_256);
	auto &n256 = Node256::Get(art, node);

	n256.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n256.children[i].Reset();
	}

	return n256;
}

void Node256::Free(ART &art, Node &node) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());

	auto &n256 = Node256::Get(art, node);

	if (!n256.count) {
		return;
	}

	// free all children
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n256.children[i].IsSet()) {
			Node::Free(art, n256.children[i]);
		}
	}
}

Node256 &Node256::GrowNode48(ART &art, Node &node256, Node &node48) {

	auto &n48 = Node48::Get(art, node48);
	auto &n256 = Node256::New(art, node256);

	n256.count = n48.count;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			n256.children[i] = n48.children[n48.child_index[i]];
		} else {
			n256.children[i].Reset();
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n256;
}

void Node256::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].IsSet()) {
			children[i].InitializeMerge(art, flags);
		}
	}
}

void Node256::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());
	auto &n256 = Node256::Get(art, node);

	// ensure that there is no other child at the same byte
	D_ASSERT(!n256.children[byte].IsSet());

	n256.count++;
	D_ASSERT(n256.count <= Node::NODE_256_CAPACITY);
	n256.children[byte] = child;
}

void Node256::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());
	auto &n256 = Node256::Get(art, node);

	// free the child and decrease the count
	Node::Free(art, n256.children[byte]);
	n256.count--;

	// shrink node to Node48
	if (n256.count <= Node::NODE_256_SHRINK_THRESHOLD) {
		auto node256 = node;
		Node48::ShrinkNode256(art, node, node256);
	}
}

optional_ptr<Node> Node256::GetNextChild(uint8_t &byte) {

	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].IsSet()) {
			byte = i;
			return &children[i];
		}
	}
	return nullptr;
}

BlockPointer Node256::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::NODE_256);
	writer.Write<uint16_t>(count);

	// write child block pointers
	for (auto &child_block_pointer : child_block_pointers) {
		writer.Write(child_block_pointer.block_id);
		writer.Write(child_block_pointer.offset);
	}

	return block_pointer;
}

void Node256::Deserialize(MetaBlockReader &reader) {

	count = reader.Read<uint16_t>();

	// read child block pointers
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		children[i] = Node(reader);
	}
}

void Node256::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].IsSet()) {
			children[i].Vacuum(art, flags);
		}
	}
}

} // namespace duckdb
