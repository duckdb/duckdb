//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/graphviz_tree_renderer.hpp
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

class GRAPHVIZTreeRenderer : public TreeRenderer {
public:
	explicit GRAPHVIZTreeRenderer() {
	}
	~GRAPHVIZTreeRenderer() override {
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

	string RenderProfilerDisabled() override;
};

} // namespace duckdb
