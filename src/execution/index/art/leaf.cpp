#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/leaf_segment.hpp"

namespace duckdb {

Leaf *Leaf::Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t &row_id) {
	auto leaf = art.leaf_nodes.GetDataAtPosition<Leaf>(node.GetPointer());
	art.IncreaseMemorySize(sizeof(Leaf));

	// set the fields of the leaf
	leaf->count = 1;
	leaf->row_ids.inlined = row_id;

	// initialize the prefix
	D_ASSERT(key.len >= depth);
	leaf->prefix.Initialize(art, key, depth, key.len - depth);

	return leaf;
}

Leaf *Leaf::Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t *row_ids,
                       const idx_t &count) {
	auto leaf = art.leaf_nodes.GetDataAtPosition<Leaf>(node.GetPointer());
	art.IncreaseMemorySize(sizeof(Leaf));

	// inlined leaf
	D_ASSERT(count >= 1);
	if (count == 1) {
		return Leaf::Initialize(art, node, key, depth, row_ids[0]);
	}

	// set the fields of the leaf
	leaf->count = 0;

	// copy the row IDs
	leaf->row_ids.position = art.leaf_segments.GetPosition();
	auto segment = LeafSegment::Initialize(art, leaf->row_ids.position);
	for (idx_t i = 0; i < count; i++) {
		segment = segment->Append(art, leaf->count, row_ids[i]);
	}

	// set the prefix
	D_ASSERT(key.len >= depth);
	leaf->prefix.Initialize(art, key, depth, key.len - depth);
}

} // namespace duckdb
