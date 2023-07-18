#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

Node48 &Node48::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_48).New();
	node.SetType((uint8_t)NType::NODE_48);
	auto &n48 = Node48::Get(art, node);

	n48.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n48.child_index[i] = Node::EMPTY_MARKER;
	}

	// necessary for faster child insertion/deletion
	for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Reset();
	}

	return n48;
}

void Node48::Free(ART &art, Node &node) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());

	auto &n48 = Node48::Get(art, node);

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

	auto &n16 = Node16::Get(art, node16);
	auto &n48 = Node48::New(art, node48);

	n48.count = n16.count;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n48.child_index[i] = Node::EMPTY_MARKER;
	}

	for (idx_t i = 0; i < n16.count; i++) {
		n48.child_index[n16.key[i]] = i;
		n48.children[i] = n16.children[i];
	}

	// necessary for faster child insertion/deletion
	for (idx_t i = n16.count; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Reset();
	}

	n16.count = 0;
	Node::Free(art, node16);
	return n48;
}

Node48 &Node48::ShrinkNode256(ART &art, Node &node48, Node &node256) {

	auto &n48 = Node48::New(art, node48);
	auto &n256 = Node256::Get(art, node256);

	n48.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		D_ASSERT(n48.count <= Node::NODE_48_CAPACITY);
		if (n256.children[i].IsSet()) {
			n48.child_index[i] = n48.count;
			n48.children[n48.count] = n256.children[i];
			n48.count++;
		} else {
			n48.child_index[i] = Node::EMPTY_MARKER;
		}
	}

	// necessary for faster child insertion/deletion
	for (idx_t i = n48.count; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Reset();
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

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());
	auto &n48 = Node48::Get(art, node);

	// ensure that there is no other child at the same byte
	D_ASSERT(n48.child_index[byte] == Node::EMPTY_MARKER);

	// insert new child node into node
	if (n48.count < Node::NODE_48_CAPACITY) {
		// still space, just insert the child
		idx_t child_pos = n48.count;
		if (n48.children[child_pos].IsSet()) {
			// find an empty position in the node list if the current position is occupied
			child_pos = 0;
			while (n48.children[child_pos].IsSet()) {
				child_pos++;
			}
		}
		n48.children[child_pos] = child;
		n48.child_index[byte] = child_pos;
		n48.count++;

	} else {
		// node is full, grow to Node256
		auto node48 = node;
		Node256::GrowNode48(art, node, node48);
		Node256::InsertChild(art, node, byte, child);
	}
}

void Node48::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());
	auto &n48 = Node48::Get(art, node);

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

optional_ptr<Node> Node48::GetNextChild(uint8_t &byte) {

	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			byte = i;
			D_ASSERT(children[child_index[i]].IsSet());
			return &children[child_index[i]];
		}
	}
	return nullptr;
}

BlockPointer Node48::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::NODE_48);
	writer.Write<uint8_t>(count);

	// write key values
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		writer.Write(child_index[i]);
	}

	// write child block pointers
	for (auto &child_block_pointer : child_block_pointers) {
		writer.Write(child_block_pointer.block_id);
		writer.Write(child_block_pointer.offset);
	}

	return block_pointer;
}

void Node48::Deserialize(MetaBlockReader &reader) {

	count = reader.Read<uint8_t>();

	// read key values
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		child_index[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
		children[i] = Node(reader);
	}
}

void Node48::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			children[child_index[i]].Vacuum(art, flags);
		}
	}
}

} // namespace duckdb
