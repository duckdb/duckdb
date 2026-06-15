//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/tree_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
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
	//! Create a TreeRenderer for the given format name (e.g. "json", "text"). The name is matched case-insensitively
	//! and throws if it is not recognized. Returns nullptr for formats that render no output (i.e. "no_output").
	//! This is the primary, name-based factory; new render formats are added here.
	static unique_ptr<TreeRenderer> CreateRenderer(const string &name);
	//! Create a TreeRenderer for the given ProfilerPrintFormat (thin wrapper over the name-based factory).
	static unique_ptr<TreeRenderer> CreateRenderer(const ProfilerPrintFormat &format);

	//! Generic configuration of the renderer: passes renderer settings (e.g. from the "profiling_renderer_settings"
	//! setting) to the renderer. Renderers override this to handle the settings they support, and throw on invalid
	//! setting values. Unrecognized settings are ignored - they may be intended for a different renderer.
	virtual void Configure(const unordered_map<string, Value> &settings) {
	}

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
