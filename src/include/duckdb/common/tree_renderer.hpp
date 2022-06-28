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
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

struct RenderTreeNode {
	string name;
	string extra_text;
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

struct TreeRendererConfig {
	void enable_detailed() {
		MAX_EXTRA_LINES = 1000;
		detailed = true;
	}

	void enable_standard() {
		MAX_EXTRA_LINES = 30;
		detailed = false;
	}

	idx_t MAXIMUM_RENDER_WIDTH = 240;
	idx_t NODE_RENDER_WIDTH = 29;
	idx_t MINIMUM_RENDER_WIDTH = 15;
	idx_t MAX_EXTRA_LINES = 30;
	bool detailed = false;

#ifndef DUCKDB_ASCII_TREE_RENDERER
	const char *LTCORNER = "\342\224\214"; // "┌";
	const char *RTCORNER = "\342\224\220"; // "┐";
	const char *LDCORNER = "\342\224\224"; // "└";
	const char *RDCORNER = "\342\224\230"; // "┘";

	const char *MIDDLE = "\342\224\274";  // "┼";
	const char *TMIDDLE = "\342\224\254"; // "┬";
	const char *LMIDDLE = "\342\224\234"; // "├";
	const char *RMIDDLE = "\342\224\244"; // "┤";
	const char *DMIDDLE = "\342\224\264"; // "┴";

	const char *VERTICAL = "\342\224\202";   // "│";
	const char *HORIZONTAL = "\342\224\200"; // "─";
#else
	// ASCII version
	const char *LTCORNER = "<";
	const char *RTCORNER = ">";
	const char *LDCORNER = "<";
	const char *RDCORNER = ">";

	const char *MIDDLE = "+";
	const char *TMIDDLE = "+";
	const char *LMIDDLE = "+";
	const char *RMIDDLE = "+";
	const char *DMIDDLE = "+";

	const char *VERTICAL = "|";
	const char *HORIZONTAL = "-";
#endif
};

class TreeRenderer {
public:
	explicit TreeRenderer(TreeRendererConfig config_p = TreeRendererConfig()) : config(move(config_p)) {
	}

	string ToString(const LogicalOperator &op);
	string ToString(const PhysicalOperator &op);
	string ToString(const QueryProfiler::TreeNode &op);
	string ToString(const Pipeline &op);

	void Render(const LogicalOperator &op, std::ostream &ss);
	void Render(const PhysicalOperator &op, std::ostream &ss);
	void Render(const QueryProfiler::TreeNode &op, std::ostream &ss);
	void Render(const Pipeline &op, std::ostream &ss);

	void ToStream(RenderTree &root, std::ostream &ss);

	void EnableDetailed() {
		config.enable_detailed();
	}
	void EnableStandard() {
		config.enable_standard();
	}

private:
	unique_ptr<RenderTree> CreateTree(const LogicalOperator &op);
	unique_ptr<RenderTree> CreateTree(const PhysicalOperator &op);
	unique_ptr<RenderTree> CreateTree(const QueryProfiler::TreeNode &op);
	unique_ptr<RenderTree> CreateTree(const Pipeline &op);

	string ExtraInfoSeparator();
	unique_ptr<RenderTreeNode> CreateRenderNode(string name, string extra_info);
	unique_ptr<RenderTreeNode> CreateNode(const LogicalOperator &op);
	unique_ptr<RenderTreeNode> CreateNode(const PhysicalOperator &op);
	unique_ptr<RenderTreeNode> CreateNode(const QueryProfiler::TreeNode &op);
	unique_ptr<RenderTreeNode> CreateNode(const PipelineRenderNode &op);

private:
	//! The configuration used for rendering
	TreeRendererConfig config;

private:
	void RenderTopLayer(RenderTree &root, std::ostream &ss, idx_t y);
	void RenderBoxContent(RenderTree &root, std::ostream &ss, idx_t y);
	void RenderBottomLayer(RenderTree &root, std::ostream &ss, idx_t y);

	bool CanSplitOnThisChar(char l);
	bool IsPadding(char l);
	string RemovePadding(string l);
	void SplitUpExtraInfo(const string &extra_info, vector<string> &result);
	void SplitStringBuffer(const string &source, vector<string> &result);

	template <class T>
	idx_t CreateRenderTreeRecursive(RenderTree &result, const T &op, idx_t x, idx_t y);

	template <class T>
	unique_ptr<RenderTree> CreateRenderTree(const T &op);
	string ExtractExpressionsRecursive(ExpressionInfo &states);
};

} // namespace duckdb
