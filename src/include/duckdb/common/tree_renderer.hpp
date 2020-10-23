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

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;

struct RenderTreeNode {
	string name;
	vector<string> extra_text;
};

struct RenderTree {
	RenderTree(idx_t width, idx_t height);

	unique_ptr<unique_ptr<RenderTreeNode>[]> nodes;
	idx_t width;
	idx_t height;
public:
	RenderTreeNode *GetNode(idx_t x, idx_t y);
	void SetNode(idx_t x, idx_t y, unique_ptr<RenderTreeNode> node);
	bool HasNode(idx_t x, idx_t y);

	idx_t GetPosition(idx_t x, idx_t y);
};

class TreeRenderer {
public:
	TreeRenderer(unique_ptr<RenderTree> tree);

	//! Renders the tree to a string
	string ToString();

	//! Creates a tree node from a logical operator tree
	static unique_ptr<RenderTree> CreateTree(const LogicalOperator &op);
	//! Creates a tree node from a physical operator tree
	static unique_ptr<RenderTree> CreateTree(const PhysicalOperator &op);

	static unique_ptr<RenderTreeNode> CreateNode(const LogicalOperator &op);
	static unique_ptr<RenderTreeNode> CreateNode(const PhysicalOperator &op);
private:
	unique_ptr<RenderTree> root;
public:
	constexpr static idx_t TREE_RENDER_WIDTH = 29;
	constexpr static idx_t MAX_EXTRA_LINES = 10;

	static constexpr const char* LTCORNER = "┌";
	static constexpr const char* RTCORNER = "┐";
	static constexpr const char* LDCORNER = "└";
	static constexpr const char* RDCORNER = "┘";

	static constexpr const char* MIDDLE = "┼";
	static constexpr const char* TMIDDLE = "┬";
	static constexpr const char* LMIDDLE = "├";
	static constexpr const char* RMIDDLE = "┤";
	static constexpr const char* DMIDDLE = "┴";

	static constexpr const char* VERTICAL = "│";
	static constexpr const char* HORIZONTAL = "─";
};

} // namespace duckdb
