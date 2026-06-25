#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

void TreeRenderer::ToStream(RenderTree &root, BaseResultRenderer &ss) {
	if (!UsesRawKeyNames()) {
		root.SanitizeKeyNames();
	}
	ToStreamInternal(root, ss);
}

} // namespace duckdb
