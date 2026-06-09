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

class QueryProfiler;

//! TreeRenderer renders a plan/operator tree (for EXPLAIN) or a query profiler's output in a particular format.
//! It is the single renderer abstraction: each format (text, JSON, HTML, GraphViz, Mermaid, YAML) is one subclass
//! that handles both EXPLAIN rendering and profiler output. New formats are added in CreateRenderer.
class TreeRenderer {
public:
	explicit TreeRenderer() {
	}
	virtual ~TreeRenderer() {
	}

public:
	void ToStream(RenderTree &root, std::ostream &ss);
	virtual void ToStreamInternal(RenderTree &root, std::ostream &ss) = 0;
	//! Create a TreeRenderer for the given format name (e.g. "json", "text"). Throws if the name is not recognized.
	//! This is the primary, name-based factory; new render formats are added here.
	static unique_ptr<TreeRenderer> CreateRenderer(const string &name);
	//! Create a TreeRenderer for the given ExplainFormat (thin wrapper over the name-based factory).
	static unique_ptr<TreeRenderer> CreateRenderer(const ExplainFormat &format);

	virtual bool UsesRawKeyNames() {
		return false;
	}
	virtual void Render(const ProfilingNode &op, std::ostream &ss) {
	}

	//! Render the profiler's output in this format. Only called when profiling is enabled. The base implementation
	//! renders the profiling node tree; formats with richer output (text, JSON) override this.
	virtual string RenderProfiler(const QueryProfiler &profiler);
	//! The message shown (in this format) when profiling is disabled.
	virtual string RenderProfilerDisabled();
};

} // namespace duckdb
