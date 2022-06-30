#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"

#include <cstring>

namespace duckdb {

Node16::Node16(size_t compression_length) : Node(NodeType::N16, compression_length) {
	memset(key, 16, sizeof(key));
	for (auto &child : children) {
		child = 0;
	}
}

Node16::~Node16() {
	for (auto &child : children) {
		if (child) {
			if (!IsSwizzled(child)) {
				delete (Node *)child;
			}
		}
	}
}

idx_t Node16::GetChildPos(uint8_t k) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return Node::GetChildPos(k);
}

idx_t Node16::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] >= k) {
			if (key[pos] == k) {
				equal = true;
			} else {
				equal = false;
			}

			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k, equal);
}

idx_t Node16::GetNextPos(idx_t pos) {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

Node *Node16::GetChild(ART &art, idx_t pos) {
	D_ASSERT(pos < count);
	children[pos] = Node::GetChildSwizzled(art, children[pos]);
	return (Node *)children[pos];
}

idx_t Node16::GetMin() {
	return 0;
}

void Node16::ReplaceChildPointer(idx_t pos, Node *node) {
	AssignPointer(children[pos], node);
}

void Node16::Insert(Node *&node, uint8_t key_byte, Node *child) {
	Node16 *n = (Node16 *)node;

	if (n->count < 16) {
		// Insert element
		idx_t pos = 0;
		while (pos < node->count && n->key[pos] < key_byte) {
			pos++;
		}
		if (n->children[pos] != 0) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->children[i] = n->children[i - 1];
			}
		}
		n->key[pos] = key_byte;
		AssignPointer(n->children[pos], child);
		n->count++;
	} else {
		// Grow to Node48
		auto new_node = new Node48(n->prefix_length);
		for (idx_t i = 0; i < node->count; i++) {
			new_node->child_index[n->key[i]] = i;
			new_node->children[i] = n->children[i];
			n->children[i] = 0;
		}
		CopyPrefix(n, new_node);
		new_node->count = node->count;
		delete node;
		node = new_node;

		Node48::Insert(node, key_byte, child);
	}
}

std::pair<idx_t, idx_t> Node16::Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
	// Iterate through children and annotate their offsets
	vector<std::pair<idx_t, idx_t>> child_offsets;
	for (auto &child_ptr : children) {
		if (child_ptr) {
			child_ptr = GetChildSwizzled(art, child_ptr);
			child_offsets.push_back(((Node *)child_ptr)->Serialize(art, writer));
		} else {
			child_offsets.emplace_back(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
		}
	}
	auto block_id = writer.block->id;
	auto offset = writer.offset;
	// Write Node Type
	writer.Write(type);
	writer.Write(count);
	// Write compression Info
	writer.Write(prefix_length);
	for (idx_t i = 0; i < prefix_length; i++) {
		writer.Write(prefix[i]);
	}
	// Write Key values
	for (auto &key_v : key) {
		writer.Write(key_v);
	}
	// Write child offsets
	for (auto &offsets : child_offsets) {
		writer.Write(offsets.first);
		writer.Write(offsets.second);
	}
	return {block_id, offset};
}

Node16 *Node16::Deserialize(duckdb::MetaBlockReader &reader) {
	auto count = reader.Read<uint16_t>();
	auto prefix_length = reader.Read<uint32_t>();
	auto node16 = new Node16(prefix_length);
	node16->count = count;
	node16->prefix_length = prefix_length;
	for (idx_t i = 0; i < prefix_length; i++) {
		node16->prefix[i] = reader.Read<uint8_t>();
	}

	// Get Key values
	for (idx_t i = 0; i < 16; i++) {
		node16->key[i] = reader.Read<uint8_t>();
	}

	// Get Child offsets
	for (idx_t i = 0; i < 16; i++) {
		idx_t block_id = reader.Read<idx_t>();
		idx_t offset = reader.Read<idx_t>();
		node16->children[i] = Node::GenerateSwizzledPointer(block_id, offset);
	}
	return node16;
}

void Node16::Erase(Node *&node, int pos) {
	auto n = (Node16 *)node;
	// erase the child and decrease the count
	if (!IsSwizzled(n->children[pos])) {
		delete (Node *)n->children[pos];
	}
	n->children[pos] = 0;
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->children[pos] = n->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < 16; pos++) {
		if (!n->children[pos]) {
			break;
		}
		n->children[pos] = 0;
	}

	if (node->count <= 3) {
		// Shrink node
		auto new_node = new Node4(n->prefix_length);
		for (unsigned i = 0; i < n->count; i++) {
			new_node->key[new_node->count] = n->key[i];
			new_node->children_ptrs[new_node->count++] = n->children[i];
			n->children[i] = 0;
		}
		CopyPrefix(n, new_node);
		delete node;
		node = new_node;
	}
}

} // namespace duckdb
