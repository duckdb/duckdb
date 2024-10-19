#include "duckdb/common/tree_renderer.hpp"

namespace duckdb {

void TreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	if (!UsesRawKeyNames()) {
		root.SanitizeKeyNames();
	}
	return ToStreamInternal(root, ss);
}

} // namespace duckdb
