#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

void TreeRenderer::ToStream(RenderTree &root, BaseResultRenderer &ss) {
	if (!UsesRawKeyNames()) {
		root.SanitizeKeyNames();
	}
	ToStreamInternal(root, ss);
}

void TreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	// bridge for plain-text consumers: render into a string sink and emit the result
	StringResultRenderer renderer;
	ToStream(root, renderer);
	ss << renderer.str();
}

} // namespace duckdb
