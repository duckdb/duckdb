//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiler_printer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/explain_format.hpp"

namespace duckdb {

class QueryProfiler;

//! ProfilerPrinter renders the output of a QueryProfiler in a specific format.
//! New profiling output formats can be supported by subclassing ProfilerPrinter and registering the format name in
//! QueryProfiler::CreateProfiler, instead of extending a hard-coded switch.
class ProfilerPrinter {
public:
	explicit ProfilerPrinter(const QueryProfiler &profiler);
	virtual ~ProfilerPrinter();

public:
	//! Render the profiling output. Only called when profiling is enabled.
	virtual string ToString() const = 0;
	//! Render the message that is shown when profiling is disabled.
	virtual string RenderDisabledMessage() const = 0;
	//! Whether or not this printer emits any output (used to suppress automatic output).
	virtual bool EmitsOutput() const {
		return true;
	}
	//! Whether or not the optimizer must be profiled in order to render this format.
	virtual bool PrintOptimizerOutput() const {
		return false;
	}

protected:
	const QueryProfiler &profiler;
};

//! Renders the profiler output as a text query tree.
class QueryTreeProfilerPrinter : public ProfilerPrinter {
public:
	explicit QueryTreeProfilerPrinter(const QueryProfiler &profiler);

public:
	string ToString() const override;
	string RenderDisabledMessage() const override;
};

//! Identical to QueryTreeProfilerPrinter, but additionally profiles the optimizer.
class QueryTreeOptimizerProfilerPrinter : public QueryTreeProfilerPrinter {
public:
	explicit QueryTreeOptimizerProfilerPrinter(const QueryProfiler &profiler);

public:
	bool PrintOptimizerOutput() const override;
};

//! Renders the profiler output as JSON.
class JSONProfilerPrinter : public ProfilerPrinter {
public:
	explicit JSONProfilerPrinter(const QueryProfiler &profiler);

public:
	string ToString() const override;
	string RenderDisabledMessage() const override;
};

//! Suppresses all profiler output.
class NoOutputProfilerPrinter : public ProfilerPrinter {
public:
	explicit NoOutputProfilerPrinter(const QueryProfiler &profiler);

public:
	string ToString() const override;
	string RenderDisabledMessage() const override;
	bool EmitsOutput() const override;
};

//! Base class for printers that render the operator tree using a TreeRenderer (HTML/GraphViz/Mermaid).
class TreeRendererProfilerPrinter : public ProfilerPrinter {
public:
	TreeRendererProfilerPrinter(const QueryProfiler &profiler, ExplainFormat format);

public:
	string ToString() const override;

protected:
	ExplainFormat format;
};

//! Renders the profiler output as HTML.
class HTMLProfilerPrinter : public TreeRendererProfilerPrinter {
public:
	explicit HTMLProfilerPrinter(const QueryProfiler &profiler);

public:
	string RenderDisabledMessage() const override;
};

//! Renders the profiler output as a GraphViz graph.
class GraphVizProfilerPrinter : public TreeRendererProfilerPrinter {
public:
	explicit GraphVizProfilerPrinter(const QueryProfiler &profiler);

public:
	string RenderDisabledMessage() const override;
};

//! Renders the profiler output as a Mermaid flowchart.
class MermaidProfilerPrinter : public TreeRendererProfilerPrinter {
public:
	explicit MermaidProfilerPrinter(const QueryProfiler &profiler);

public:
	string RenderDisabledMessage() const override;
};

} // namespace duckdb
