#include "duckdb/execution/index/art/node16.hpp"

#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"

namespace duckdb {

Node16 &Node16::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_16).New();
	node.SetType((uint8_t)NType::NODE_16);
	auto &n16 = Node16::Get(art, node);

	n16.count = 0;
	return n16;
}

void Node16::Free(ART &art, Node &node) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());

	auto &n16 = Node16::Get(art, node);

	// free all children
	for (idx_t i = 0; i < n16.count; i++) {
		Node::Free(art, n16.children[i]);
	}
}

Node16 &Node16::GrowNode4(ART &art, Node &node16, Node &node4) {

	auto &n4 = Node4::Get(art, node4);
	auto &n16 = Node16::New(art, node16);

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

	auto &n16 = Node16::New(art, node16);
	auto &n48 = Node48::Get(art, node48);

	n16.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		D_ASSERT(n16.count <= Node::NODE_16_CAPACITY);
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			n16.key[n16.count] = i;
			n16.children[n16.count] = n48.children[n48.child_index[i]];
			n16.count++;
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n16;
}

void Node16::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, flags);
	}
}

void Node16::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());
	auto &n16 = Node16::Get(art, node);

	// ensure that there is no other child at the same byte
	for (idx_t i = 0; i < n16.count; i++) {
		D_ASSERT(n16.key[i] != byte);
	}

	// insert new child node into node
	if (n16.count < Node::NODE_16_CAPACITY) {
		// still space, just insert the child
		idx_t child_pos = 0;
		while (child_pos < n16.count && n16.key[child_pos] < byte) {
			child_pos++;
		}
		// move children backwards to make space
		for (idx_t i = n16.count; i > child_pos; i--) {
			n16.key[i] = n16.key[i - 1];
			n16.children[i] = n16.children[i - 1];
		}

		n16.key[child_pos] = byte;
		n16.children[child_pos] = child;
		n16.count++;

	} else {
		// node is full, grow to Node48
		auto node16 = node;
		Node48::GrowNode16(art, node, node16);
		Node48::InsertChild(art, node, byte, child);
	}
}

void Node16::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSerialized());
	auto &n16 = Node16::Get(art, node);

	idx_t child_pos = 0;
	for (; child_pos < n16.count; child_pos++) {
		if (n16.key[child_pos] == byte) {
			break;
		}
	}

	D_ASSERT(child_pos < n16.count);

	// free the child and decrease the count
	Node::Free(art, n16.children[child_pos]);
	n16.count--;

	// potentially move any children backwards
	for (idx_t i = child_pos; i < n16.count; i++) {
		n16.key[i] = n16.key[i + 1];
		n16.children[i] = n16.children[i + 1];
	}

	// shrink node to Node4
	if (n16.count < Node::NODE_4_CAPACITY) {
		auto node16 = node;
		Node4::ShrinkNode16(art, node, node16);
	}
}

void Node16::ReplaceChild(const uint8_t byte, const Node child) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			children[i] = child;
			return;
		}
	}
}

optional_ptr<Node> Node16::GetChild(const uint8_t byte) {

	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].IsSet());
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node16::GetNextChild(uint8_t &byte) {

	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].IsSet());
			return &children[i];
		}
	}
	return nullptr;
}

BlockPointer Node16::Serialize(ART &art, MetadataWriter &writer) {

	// recurse into children and retrieve child block pointers
	vector<BlockPointer> child_block_pointers;
	for (idx_t i = 0; i < count; i++) {
		child_block_pointers.push_back(children[i].Serialize(art, writer));
	}
	for (idx_t i = count; i < Node::NODE_16_CAPACITY; i++) {
		child_block_pointers.emplace_back((block_id_t)DConstants::INVALID_INDEX, 0);
	}

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::NODE_16);
	writer.Write<uint8_t>(count);

	// write key values
	for (idx_t i = 0; i < Node::NODE_16_CAPACITY; i++) {
		writer.Write(key[i]);
	}

	// write child block pointers
	for (auto &child_block_pointer : child_block_pointers) {
		writer.Write(child_block_pointer.block_id);
		writer.Write(child_block_pointer.offset);
	}

	return block_pointer;
}

void Node16::Deserialize(MetadataReader &reader) {

	count = reader.Read<uint8_t>();

	// read key values
	for (idx_t i = 0; i < Node::NODE_16_CAPACITY; i++) {
		key[i] = reader.Read<uint8_t>();
	}

	// read child block pointers
	for (idx_t i = 0; i < Node::NODE_16_CAPACITY; i++) {
		children[i] = Node(reader);
	}
}

void Node16::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		children[i].Vacuum(art, flags);
	}
}

} // namespace duckdb
