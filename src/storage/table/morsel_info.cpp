#include "duckdb/storage/table/morsel_info.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

const idx_t MorselInfo::MORSEL_VECTOR_COUNT;
const idx_t MorselInfo::MORSEL_SIZE;
const idx_t MorselInfo::MORSEL_LAYER_COUNT;
const idx_t MorselInfo::MORSEL_LAYER_SIZE;

ChunkInfo *MorselInfo::GetChunkInfo(idx_t vector_idx) {
	if (!root) {
		return nullptr;
	}
	idx_t first_layer = vector_idx / MorselInfo::MORSEL_LAYER_COUNT;
	idx_t second_layer = vector_idx - first_layer * MorselInfo::MORSEL_LAYER_COUNT;
	idx_t layers[] { first_layer, second_layer };
	VersionNode *node = root.get();
	for(idx_t i = 0; i < 3; i++) {
		switch(node->type) {
		case VersionNodeType::VERSION_NODE_INTERNAL: {
			assert(i < 2);
			auto &internal_node = (VersionNodeInternal &) *node;
			// recurse into children
			idx_t layer_idx = layers[i];
			if (!internal_node.children[layer_idx]) {
				// no child node for this entry
				return nullptr;
			}
			node = internal_node.children[layer_idx].get();
			break;
		}
		case VersionNodeType::VERSION_NODE_LEAF: {
			auto &leaf = (VersionNodeLeaf &) *node;
			// leaf node: fetch from this chunk
			return leaf.info.get();
		}
		default:
			throw InternalException("Unrecognized version node type");
		}
	}
	throw InternalException("No leaf node found");
}

idx_t MorselInfo::GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> lock(morsel_lock);

	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetSelVector(transaction, sel_vector, max_count);
}

bool MorselInfo::Fetch(Transaction &transaction, idx_t row) {
	lock_guard<mutex> lock(morsel_lock);

	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void MorselInfo::Append(Transaction &transaction, idx_t start, idx_t count, transaction_t commit_id) {
	idx_t end = start + count;
	lock_guard<mutex> lock(morsel_lock);

	// check if the append covers the ENTIRE morsel
	if (start == 0 && count == MorselInfo::MORSEL_SIZE) {
		assert(!root);
		// if the append covers the entire morsel, we create a constant root entry
		auto leaf = make_unique<VersionNodeLeaf>();
		auto entry = make_unique<ChunkConstantInfo>(*this);
		entry->insert_id = commit_id;
		leaf->info = move(entry);
		root = move(leaf);
		return;
	}
	// the append does not cover the entire morsel: check if the root already exists
	if (!root) {
		root = make_unique<VersionNodeInternal>();
	}
	assert(root->type == VersionNodeType::VERSION_NODE_INTERNAL);
	auto &root_version = (VersionNodeInternal &) *root;
	// search the start and end nodes on both layers
	idx_t start_vector_idx = start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (start + count) / STANDARD_VECTOR_SIZE;
	idx_t start_layer[2], end_layer[2];

	start_layer[0] = start_vector_idx / MorselInfo::MORSEL_LAYER_COUNT;
	start_layer[1] = start_vector_idx - start_layer[0] * MorselInfo::MORSEL_LAYER_COUNT;

	end_layer[0] = end_vector_idx / MorselInfo::MORSEL_LAYER_COUNT;
	end_layer[1] = end_vector_idx - end_layer[0] * MorselInfo::MORSEL_LAYER_COUNT;

	for(idx_t first_layer = start_layer[0]; first_layer <= end_layer[0]; first_layer++) {
		idx_t flayer_start = first_layer * MorselInfo::MORSEL_LAYER_SIZE;
		idx_t flayer_end = (first_layer + 1) * MorselInfo::MORSEL_LAYER_SIZE;
		// first layer: check if we encompass ALL of this layer
		if (start <= flayer_start && end >= flayer_end) {
			// entire layer is encapsulated: add a constant node here
			auto leaf = make_unique<VersionNodeLeaf>();
			auto entry = make_unique<ChunkConstantInfo>(*this);
			entry->insert_id = commit_id;
			leaf->info = move(entry);
			root_version.children[first_layer] = move(leaf);
			continue;
		}
		if (!root_version.children[first_layer]) {
			root_version.children[first_layer] = make_unique<VersionNodeInternal>();
		}
		auto &first_layer_node = (VersionNodeInternal &) root_version.children[first_layer];
		idx_t start_sl = first_layer == start_layer[0] ? start_layer[1] : 0;
		idx_t end_sl = first_layer == end_layer[0] ? end_layer[1] : 0;
		for(idx_t second_layer = start_sl; second_layer <= end_sl; second_layer++) {
			// check if we encompass ALL of this layer
			idx_t slayer_start = flayer_start + second_layer * STANDARD_VECTOR_SIZE;
			idx_t slayer_end = flayer_start + (second_layer + 1) * STANDARD_VECTOR_SIZE;
			if (start <= slayer_start && end >= slayer_end) {
				// entire layer is encapsulated: add a constant node here
				auto leaf = make_unique<VersionNodeLeaf>();
				auto entry = make_unique<ChunkConstantInfo>(*this);
				entry->insert_id = commit_id;
				leaf->info = move(entry);
				first_layer_node.children[second_layer] = move(leaf);
				continue;
			}
			// we don't encompass all of the second layer: make a non-constant node if it is not there yet
			ChunkInsertInfo *insert_info;
			if (!first_layer_node.children[second_layer]) {
				auto leaf = make_unique<VersionNodeLeaf>();
				auto entry = make_unique<ChunkInsertInfo>(*this);
				insert_info = entry.get();
				leaf->info = move(entry);
				first_layer_node.children[second_layer] = move(leaf);
			} else {
				assert(first_layer_node.children[second_layer]->type == VersionNodeType::VERSION_NODE_LEAF);
				auto &leaf = (VersionNodeLeaf &) *first_layer_node.children[second_layer];
				assert(leaf.info->type == ChunkInfoType::INSERT_INFO);
				insert_info = (ChunkInsertInfo *) leaf.info.get();
			}
			idx_t start_pos = MaxValue<idx_t>(slayer_start, start) - slayer_start;
			idx_t end_pos = MinValue<idx_t>(slayer_end, end) - slayer_start;
			insert_info->Append(start_pos, end_pos, commit_id);
			if (end_pos == slayer_end && insert_info->all_same_id) {
				// finished inserting into this node, and every row has the same id
				// we can fold it into a constant node
				auto &leaf = (VersionNodeLeaf &) *first_layer_node.children[second_layer];
				auto constant_entry = make_unique<ChunkConstantInfo>(*this);
				constant_entry->insert_id = commit_id;
				leaf.info = move(constant_entry);
			}
		}
	}
}

}