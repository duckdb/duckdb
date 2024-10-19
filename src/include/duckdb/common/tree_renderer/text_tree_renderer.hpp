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
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/render_tree.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

struct TextTreeRendererConfig {
	void EnableDetailed() {
		max_extra_lines = 1000;
		detailed = true;
	}

	void EnableStandard() {
		max_extra_lines = 30;
		detailed = false;
	}

	idx_t maximum_render_width = 240;
	idx_t node_render_width = 29;
	idx_t minimum_render_width = 15;
	idx_t max_extra_lines = 30;
	bool detailed = false;

#ifndef DUCKDB_ASCII_TREE_RENDERER
	const char *LTCORNER = "\342\224\214"; // NOLINT "┌";
	const char *RTCORNER = "\342\224\220"; // NOLINT "┐";
	const char *LDCORNER = "\342\224\224"; // NOLINT "└";
	const char *RDCORNER = "\342\224\230"; // NOLINT "┘";

	const char *MIDDLE = "\342\224\274";  // NOLINT "┼";
	const char *TMIDDLE = "\342\224\254"; // NOLINT "┬";
	const char *LMIDDLE = "\342\224\234"; // NOLINT "├";
	const char *RMIDDLE = "\342\224\244"; // NOLINT "┤";
	const char *DMIDDLE = "\342\224\264"; // NOLINT "┴";

	const char *VERTICAL = "\342\224\202";   // NOLINT "│";
	const char *HORIZONTAL = "\342\224\200"; // NOLINT "─";
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

class TextTreeRenderer : public TreeRenderer {
public:
	explicit TextTreeRenderer(TextTreeRendererConfig config_p = TextTreeRendererConfig()) : config(config_p) {
	}
	~TextTreeRenderer() override {
	}

public:
	string ToString(const LogicalOperator &op);
	string ToString(const PhysicalOperator &op);
	string ToString(const ProfilingNode &op);
	string ToString(const Pipeline &op);

	void Render(const LogicalOperator &op, std::ostream &ss);
	void Render(const PhysicalOperator &op, std::ostream &ss);
	void Render(const ProfilingNode &op, std::ostream &ss) override;
	void Render(const Pipeline &op, std::ostream &ss);

	void ToStreamInternal(RenderTree &root, std::ostream &ss) override;

	void EnableDetailed() {
		config.EnableDetailed();
	}
	void EnableStandard() {
		config.EnableStandard();
	}
	bool UsesRawKeyNames() override {
		return true;
	}

private:
	//! The configuration used for rendering
	TextTreeRendererConfig config;

private:
	string ExtraInfoSeparator();
	void RenderTopLayer(RenderTree &root, std::ostream &ss, idx_t y);
	void RenderBoxContent(RenderTree &root, std::ostream &ss, idx_t y);
	void RenderBottomLayer(RenderTree &root, std::ostream &ss, idx_t y);

	bool CanSplitOnThisChar(char l);
	bool IsPadding(char l);
	string RemovePadding(string l);
	void SplitUpExtraInfo(const InsertionOrderPreservingMap<string> &extra_info, vector<string> &result);
	void SplitStringBuffer(const string &source, vector<string> &result);
};

} // namespace duckdb
