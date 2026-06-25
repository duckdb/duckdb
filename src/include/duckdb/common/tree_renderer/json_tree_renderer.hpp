//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/json_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/common/render_tree.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

class JSONTreeRenderer : public TreeRenderer {
public:
	explicit JSONTreeRenderer() {
	}
	~JSONTreeRenderer() override {
	}

public:
	string ToString(const LogicalOperator &op);
	string ToString(const PhysicalOperator &op);
	string ToString(const ProfilingNode &op);
	string ToString(const Pipeline &op);

	void Render(const LogicalOperator &op, BaseResultRenderer &ss);
	void Render(const PhysicalOperator &op, BaseResultRenderer &ss);
	void Render(const ProfilingNode &op, BaseResultRenderer &ss) override;
	void Render(const Pipeline &op, BaseResultRenderer &ss);

	void ToStreamInternal(RenderTree &root, BaseResultRenderer &ss) override;

	//! Profiler JSON output: the full query profile result tree (with query-level metrics)
	void RenderProfiler(const QueryProfiler &profiler, BaseResultRenderer &ss) override;
	string RenderProfilerDisabled() override;
};

} // namespace duckdb
