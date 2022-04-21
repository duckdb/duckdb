#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"

#include <cstring>

namespace duckdb {

Node16::Node16(size_t compression_length) : Node(NodeType::N16, compression_length) {
	memset(key, 16, sizeof(key));
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

unique_ptr<Node> *Node16::GetChild(ART &art, idx_t pos) {
	D_ASSERT(pos < count);
	if (!child[pos]) {
		child[pos] = Node::Deserialize(art, block_offsets[pos].first, block_offsets[pos].second);
	}
	return &child[pos];
}

idx_t Node16::GetMin() {
	return 0;
}

void Node16::Insert(unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node16 *n = static_cast<Node16 *>(node.get());

	if (n->count < 16) {
		// Insert element
		idx_t pos = 0;
		while (pos < node->count && n->key[pos] < key_byte) {
			pos++;
		}
		if (n->child[pos] != nullptr) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->child[i] = move(n->child[i - 1]);
			}
		}
		n->key[pos] = key_byte;
		n->child[pos] = move(child);
		n->count++;
	} else {
		// Grow to Node48
		auto new_node = make_unique<Node48>(n->prefix_length);
		for (idx_t i = 0; i < node->count; i++) {
			new_node->child_index[n->key[i]] = i;
			new_node->child[i] = move(n->child[i]);
		}
		CopyPrefix(n, new_node.get());
		new_node->count = node->count;
		node = move(new_node);

		Node48::Insert(node, key_byte, child);
	}
}

std::pair<idx_t, idx_t> Node16::Serialize(duckdb::MetaBlockWriter &writer) {
	// Iterate through children and annotate their offsets
	vector<std::pair<idx_t, idx_t>> child_offsets;
	for (auto &child_node : child) {
		if (child_node) {
			child_offsets.push_back(child_node->Serialize(writer));
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

unique_ptr<Node16> Node16::Deserialize(duckdb::MetaBlockReader &reader) {
	auto count = reader.Read<uint16_t>();
	auto prefix_length = reader.Read<uint32_t>();
	auto node16 = make_unique<Node16>(prefix_length);
	node16->count = count;

	for (idx_t i = 0; i < prefix_length; i++) {
		node16->prefix[i] = reader.Read<uint8_t>();
	}

	// Get Key values
	for (idx_t i = 0; i < 16; i++) {
		node16->key[i] = reader.Read<uint8_t>();
	}

	// Get Child offsets
	for (idx_t i = 0; i < 16; i++) {
		node16->block_offsets[i] = {reader.Read<idx_t>(), reader.Read<idx_t>()};
	}
	return node16;
}

void Node16::Erase(unique_ptr<Node> &node, int pos) {
	Node16 *n = static_cast<Node16 *>(node.get());
	// erase the child and decrease the count
	n->child[pos].reset();
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->child[pos] = move(n->child[pos + 1]);
	}
	if (node->count <= 3) {
		// Shrink node
		auto new_node = make_unique<Node4>(n->prefix_length);
		for (unsigned i = 0; i < n->count; i++) {
			new_node->key[new_node->count] = n->key[i];
			new_node->child[new_node->count++] = move(n->child[i]);
		}
		CopyPrefix(n, new_node.get());
		node = move(new_node);
	}
}

} // namespace duckdb
