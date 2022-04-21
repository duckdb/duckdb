#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

Node256::Node256(size_t compression_length) : Node(NodeType::N256, compression_length) {
}

idx_t Node256::GetChildPos(uint8_t k) {
	//! FIXME: GOTTA do something here to check the keys with serialized nodes
	if (child[k]) {
		return k;
	} else {
		return DConstants::INVALID_INDEX;
	}
}

idx_t Node256::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (child[pos]) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child[pos]) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

unique_ptr<Node> *Node256::GetChild(ART &art, idx_t pos) {
	if (!child[pos]) {
		child[pos] = Node::Deserialize(art, block_offsets[pos].first, block_offsets[pos].second);
	}
	return &child[pos];
}

void Node256::Insert(unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->count++;
	n->child[key_byte] = move(child);
}

void Node256::Erase(unique_ptr<Node> &node, int pos) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->child[pos].reset();
	n->count--;
	if (node->count <= 36) {
		auto new_node = make_unique<Node48>(n->prefix_length);
		CopyPrefix(n, new_node.get());
		for (idx_t i = 0; i < 256; i++) {
			if (n->child[i]) {
				new_node->child_index[i] = new_node->count;
				new_node->child[new_node->count] = move(n->child[i]);
				new_node->count++;
			}
		}
		node = move(new_node);
	}
}

std::pair<idx_t, idx_t> Node256::Serialize(duckdb::MetaBlockWriter &writer) {
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
	// Write child offsets
	for (auto &offsets : child_offsets) {
		writer.Write(offsets.first);
		writer.Write(offsets.second);
	}
	return {block_id, offset};
}

unique_ptr<Node256> Node256::Deserialize(duckdb::MetaBlockReader &reader) {
	auto count = reader.Read<uint16_t>();
	auto prefix_length = reader.Read<uint32_t>();
	auto node256 = make_unique<Node256>(prefix_length);
	node256->count = count;

	for (idx_t i = 0; i < prefix_length; i++) {
		node256->prefix[i] = reader.Read<uint8_t>();
	}

	// Get Child offsets
	for (idx_t i = 0; i < 256; i++) {
		node256->block_offsets[i] = {reader.Read<idx_t>(), reader.Read<idx_t>()};
	}
	return node256;
}

} // namespace duckdb
