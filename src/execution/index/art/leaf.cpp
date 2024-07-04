#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {
	// We directly inline this row ID into the node pointer.
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);
	node.Clear();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
	node.SetRowId(row_id);
}

Leaf &Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF));
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

	leaf.count = 0;
	leaf.ptr.Clear();
	return leaf;
}

} // namespace duckdb
