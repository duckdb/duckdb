//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/html_tree_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/render_tree.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

class HTMLTreeRenderer : public TreeRenderer {
public:
	explicit HTMLTreeRenderer() {
	}
	~HTMLTreeRenderer() override {
	}

public:
	string ToString(const LogicalOperator &op);
	string ToString(const PhysicalOperator &op);
	string ToString(const ProfilingNode &op);
	string ToString(const Pipeline &op);

	void Render(const LogicalOperator &op, BaseTreeRenderer &ss);
	void Render(const PhysicalOperator &op, BaseTreeRenderer &ss);
	void Render(const ProfilingNode &op, BaseTreeRenderer &ss) override;
	void Render(const Pipeline &op, BaseTreeRenderer &ss);

	void ToStreamInternal(RenderTree &root, BaseTreeRenderer &ss) override;

	//! Capture the query-level summary metrics (real/CPU time, bytes read/written) before rendering the node tree.
	void RenderProfiler(const QueryProfiler &profiler, BaseTreeRenderer &ss) override;

	//! Keep the internal "__cardinality__"/"__timing__"/"__estimated_cardinality__" keys raw so they can be surfaced
	//! as dedicated metrics in the viewer rather than as generic detail rows.
	bool UsesRawKeyNames() override {
		return true;
	}

	string RenderProfilerDisabled() override;

protected:
	//! Pretty-print the query SQL before it is highlighted. The base renderer returns it unchanged; the CLI overrides
	//! this to run duckdb_format_sql, which requires the autocomplete extension and so cannot live in core.
	virtual string FormatSQL(const string &sql) {
		return sql;
	}

private:
	//! Query-level summary, populated by RenderProfiler (EXPLAIN ANALYZE / profiling) and shown in the header.
	bool has_query_metrics = false;
	double query_real_time = 0;
	double query_cpu_time = 0;
	idx_t query_bytes_read = 0;
	idx_t query_bytes_written = 0;
	//! The query text, shown as an (expandable) preview.
	string query_sql;
	//! All string-keyed phase timings (parser/planner/optimizer/physical_planner/...), in seconds; cascaded in the UI.
	vector<std::pair<string, double>> query_timings;
};

} // namespace duckdb
