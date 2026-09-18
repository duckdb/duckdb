#include "duckdb/common/tree_renderer.hpp"

namespace duckdb {

void TreeRenderer::ToStream(RenderTree &root, BaseTreeRenderer &ss) {
	if (!UsesRawKeyNames()) {
		root.SanitizeKeyNames();
	}
	ToStreamInternal(root, ss);
	Finish();
}

} // namespace duckdb
