//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/tree_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/profiling_node.hpp"
#include "duckdb/common/render_tree.hpp"

namespace duckdb {

class TreeRenderer {
public:
	explicit TreeRenderer() {
	}
	virtual ~TreeRenderer() {
	}

public:
	virtual void ToStream(RenderTree &root, std::ostream &ss) = 0;
	static unique_ptr<TreeRenderer> CreateRenderer(ExplainFormat format);
};

} // namespace duckdb
