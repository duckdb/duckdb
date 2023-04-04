#include "duckdb/execution/index/art/node16.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

Node16 *Node16::New(ART &art, ARTNode &node) {

	node.SetPtr(art.n16_nodes->New(), ARTNodeType::NODE_16);
	auto n16 = art.n16_nodes->Get<Node16>(node.GetPtr());

	n16->count = 0;
	n16->prefix.Initialize();

	return n16;
}

void Node16::Free(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto n16 = art.n16_nodes->Get<Node16>(node.GetPtr());

	// free all children
	for (idx_t i = 0; i < n16->count; i++) {
		ARTNode::Free(art, n16->children[i]);
	}
}

Node16 *Node16::GrowNode4(ART &art, ARTNode &node16, ARTNode &node4) {

	auto n4 = art.n4_nodes->Get<Node4>(node4.GetPtr());
	auto n16 = Node16::New(art, node16);

	n16->count = n4->count;
	n16->prefix.Move(n4->prefix);

	for (idx_t i = 0; i < n4->count; i++) {
		n16->key[i] = n4->key[i];
		n16->children[i] = n4->children[i];
	}

	n4->count = 0;
	ARTNode::Free(art, node4);
	return n16;
}

Node16 *Node16::ShrinkNode48(ART &art, ARTNode &node16, ARTNode &node48) {

	auto n16 = Node16::New(art, node16);
	auto n48 = art.n48_nodes->Get<Node48>(node48.GetPtr());

	n16->count = 0;
	n16->prefix.Move(n48->prefix);

	for (idx_t i = 0; i < ARTNode::NODE_256_CAPACITY; i++) {
		if (n48->child_index[i] != ARTNode::EMPTY_MARKER) {
			n16->key[n16->count] = i;
			n16->children[n16->count] = n48->children[n48->child_index[i]];
			n16->count++;
		}
	}

	n48->count = 0;
	ARTNode::Free(art, node48);
	return n16;
}

void Node16::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, flags);
	}
}

void Node16::InsertChild(ART &art, ARTNode &node, const uint8_t byte, const ARTNode child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n16 = art.n16_nodes->Get<Node16>(node.GetPtr());

	// ensure that there is no other child at the same byte
	for (idx_t i = 0; i < n16->count; i++) {
		D_ASSERT(n16->key[i] != byte);
	}

	// insert new child node into node
	if (n16->count < ARTNode::NODE_16_CAPACITY) {
		// still space, just insert the child
		idx_t position = 0;
		while (position < n16->count && n16->key[position] < byte) {
			position++;
		}
		// move children backwards to make space
		for (idx_t i = n16->count; i > position; i--) {
			n16->key[i] = n16->key[i - 1];
			n16->children[i] = n16->children[i - 1];
		}

		n16->key[position] = byte;
		n16->children[position] = child;
		n16->count++;

	} else {
		// node is full, grow to Node48
		auto node16 = node;
		Node48::GrowNode16(art, node, node16);
		Node48::InsertChild(art, node, byte, child);
	}
}

void Node16::DeleteChild(ART &art, ARTNode &node, const idx_t position) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n16 = art.n16_nodes->Get<Node16>(node.GetPtr());

	D_ASSERT(position < n16->count);

	// free the child and decrease the count
	ARTNode::Free(art, n16->children[position]);
	n16->count--;

	// potentially move any children backwards
	for (idx_t i = position; i < n16->count; i++) {
		n16->key[i] = n16->key[i + 1];
		n16->children[i] = n16->children[i + 1];
	}

	// shrink node to Node4
	if (n16->count < ARTNode::NODE_4_CAPACITY) {
		auto node16 = node;
		Node4::ShrinkNode16(art, node, node16);
	}
}

idx_t Node16::GetChildPosition(const uint8_t byte) const {
	for (idx_t position = 0; position < count; position++) {
		if (key[position] == byte) {
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node16::GetChildPositionGreaterEqual(const uint8_t byte, bool &inclusive) const {
	for (idx_t position = 0; position < count; position++) {
		if (key[position] >= byte) {
			inclusive = key[position] == byte;
			return position;
		}
	}
	return DConstants::INVALID_INDEX;
}

uint8_t Node16::GetNextPosition(idx_t &position) const {
	if (position == DConstants::INVALID_INDEX) {
		position = 0;
		return key[position];
	}
	position++;
	if (position < count) {
		return key[position];
	}
	position = DConstants::INVALID_INDEX;
	return 0;
}

BlockPointer Node16::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < count; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}
	for (idx_t i = count; i < ARTNode::NODE_16_CAPACITY; i++) {
		child_block_pointers.emplace_back();
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(ARTNodeType::NODE_16);
	writer.Write<uint8_t>(count);
	prefix.Serialize(art, writer);

	// write key values
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		writer.Write(key[i]);
	}

	// write child block pointers
	for (auto &child_block_pointer : child_block_pointers) {
		writer.Write(child_block_pointer.block_id);
		writer.Write(child_block_pointer.offset);
	}

	return block_pointer;
}

void Node16::Deserialize(ART &art, MetaBlockReader &reader) {

	count = reader.Read<uint8_t>();
	prefix.Deserialize(art, reader);

	// read key values
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		key[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < ARTNode::NODE_16_CAPACITY; i++) {
		children[i] = ARTNode(reader);
	}
}

void Node16::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		ARTNode::Vacuum(art, children[i], flags);
	}
}

} // namespace duckdb
