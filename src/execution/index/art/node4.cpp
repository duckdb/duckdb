#include "duckdb/execution/index/art/node4.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

void Node4::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n4 = art.n4_nodes.GetDataAtPosition<Node4>(node.GetPointer());

	// insert new child node into node
	if (n4->count < Node4::GetCapacity()) {
		// still space, just insert the child
		idx_t pos = 0;
		while ((pos < n4->count) && (n4->key[pos] < byte)) {
			pos++;
		}
		if (n4->children[pos]) {
			for (idx_t i = n4->count; i > pos; i--) {
				n4->key[i] = n4->key[i - 1];
				n4->children[i] = n4->children[i - 1];
			}
		}
		n4->key[pos] = byte;
		n4->children[pos] = child;
		n4->count++;

	} else {
		// node is full, grow to Node16
		ARTNode new_n16_node(art, ARTNodeType::N16);
		auto new_n16 = art.n16_nodes.GetDataAtPosition<Node16>(new_n16_node.GetPointer());
		art.IncreaseMemorySize(new_n16->MemorySize(art, false));

		new_n16->count = n4->count;
		new_n16->prefix = std::move(n4->prefix);

		for (idx_t i = 0; i < n4->count; i++) {
			new_n16->key[i] = n4->key[i];
			new_n16->children[i] = n4->children[i];
			n4->children[i] = ARTNode();
		}
		n4->count = 0;

		art.DecreaseMemorySize(n4->MemorySize());
		art.n4_nodes.FreePosition(node.GetPointer());
		node = new_n16_node;
		Node16::InsertChild(art, node, byte, child);
	}
}

void Node4::DeleteChild(ART &art, ARTNode &node, idx_t pos) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());
	auto n4 = art.n4_nodes.GetDataAtPosition<Node4>(node.GetPointer());

	// adjust the ART size
	if (n4->ChildIsInMemory(pos)) {
		auto child = n4->GetChild(pos);
		art.DecreaseMemorySize(child.MemorySize(art, true));
	}

	D_ASSERT(pos < n4->count);
	D_ASSERT(n4->count > 1);

	// erase the child and decrease the count
	ARTNode::Delete(art, n4->children[pos]);
	n4->count--;
	D_ASSERT(n4->count >= 1);

	// potentially move any children backwards
	for (; pos < n4->count; pos++) {
		n4->key[pos] = n4->key[pos + 1];
		n4->children[pos] = n4->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < Node4::GetCapacity(); pos++) {
		n4->children[pos] = ARTNode();
	}

	// this is a one way node, compress
	if (n4->count == 1) {

		// get only child and concatenate prefixes
		auto child = n4->GetChild(0);
		Prefix::Concatenate(art, child, n4->key[0], node);

		art.DecreaseMemorySize(n4->MemorySize());
		art.n4_nodes.FreePosition(node.GetPointer());
		node = child;
	}
}

void Node4::Delete(ART &art, ARTNode &node) {

	D_ASSERT(node);
	D_ASSERT(!node.IsSwizzled());

	auto n4 = art.n4_nodes.GetDataAtPosition<Node4>(node.GetPointer());

	// delete all children
	for (idx_t i = 0; i < n4->count; i++) {
		ARTNode::Delete(art, n4->children[i]);
	}
	art.n4_nodes.FreePosition(node.GetPointer());
}

void Node4::ReplaceChild(const idx_t &pos, ARTNode &child) {
	D_ASSERT(pos < GetCapacity());
	children[pos] = child;
}

ARTNode Node4::GetChild(const idx_t &pos) {
	D_ASSERT(pos < count);
	return children[pos];
}

idx_t Node4::GetChildPos(const uint8_t &byte) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == byte) {
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node4::GetChildPosGreaterEqual(const uint8_t &byte, bool &inclusive) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] >= byte) {
			inclusive = false;
			if (key[pos] == byte) {
				inclusive = true;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node4::GetMinPos() {
	return 0;
}

idx_t Node4::GetNextPos(idx_t pos) {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

idx_t Node4::GetNextPosAndByte(idx_t pos, uint8_t &byte) {
	if (pos == DConstants::INVALID_INDEX) {
		byte = key[0];
		return 0;
	}
	pos++;
	if (pos < count) {
		byte = key[pos];
		return pos;
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node4::MemorySize() {
#ifdef DEBUG
	return prefix.MemorySize() + sizeof(*this);
#endif
}

bool Node4::ChildIsInMemory(const idx_t &pos) {
#ifdef DEBUG
	D_ASSERT(pos < count);
	return children[pos].InMemory();
#endif
}

} // namespace duckdb
