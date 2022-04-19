#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

Node48::Node48(size_t compression_length) : Node(NodeType::N48, compression_length) {
	for (idx_t i = 0; i < 256; i++) {
		child_index[i] = Node::EMPTY_MARKER;
	}
}

idx_t Node48::GetChildPos(uint8_t k) {
	if (child_index[k] == Node::EMPTY_MARKER) {
		return DConstants::INVALID_INDEX;
	} else {
		return k;
	}
}

idx_t Node48::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k, equal);
}

idx_t Node48::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

unique_ptr<Node> *Node48::GetChild(idx_t pos) {
	D_ASSERT(child_index[pos] != Node::EMPTY_MARKER);
	return &child[child_index[pos]];
}

idx_t Node48::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

void Node48::Insert(unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node48 *n = static_cast<Node48 *>(node.get());

	// Insert leaf into inner node
	if (node->count < 48) {
		// Insert element
		idx_t pos = n->count;
		if (n->child[pos]) {
			// find an empty position in the node list if the current position is occupied
			pos = 0;
			while (n->child[pos]) {
				pos++;
			}
		}
		n->child[pos] = move(child);
		n->child_index[key_byte] = pos;
		n->count++;
	} else {
		// Grow to Node256
		auto new_node = make_unique<Node256>(n->prefix_length);
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->child[i] = move(n->child[n->child_index[i]]);
			}
		}
		new_node->count = n->count;
		CopyPrefix(n, new_node.get());
		node = move(new_node);
		Node256::Insert( node, key_byte, child);
	}
}

void Node48::Erase(unique_ptr<Node> &node, int pos) {
	Node48 *n = static_cast<Node48 *>(node.get());

	n->child[n->child_index[pos]].reset();
	n->child_index[pos] = Node::EMPTY_MARKER;
	n->count--;
	if (node->count <= 12) {
		auto new_node = make_unique<Node16>(n->prefix_length);
		CopyPrefix(n, new_node.get());
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->key[new_node->count] = i;
				new_node->child[new_node->count++] = move(n->child[n->child_index[i]]);
			}
		}
		node = move(new_node);
	}
}

std::pair<idx_t, idx_t> Node48::Serialize(duckdb::MetaBlockWriter &writer) {
	// Iterate through children and annotate their offsets
	vector<std::pair<idx_t, idx_t>> child_offsets;
	auto block_id = writer.block->id;
	for (auto &child_node : child) {
		if (child_node) {
			child_offsets.push_back(child_node->Serialize(writer));
		}
	}
	auto offset = writer.offset;
	// Write Node Type
	writer.Write(48);
	// Write Key values
	for (auto &key_v : child_index) {
		writer.Write(key_v);
	}
	// Write child offsets
	for (auto &offsets : child_offsets) {
		writer.Write(offsets.first);
		writer.Write(offsets.second);
	}
	return {block_id, offset};
}

} // namespace duckdb
