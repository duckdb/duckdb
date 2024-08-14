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
#include "duckdb/main/profiling_node.hpp"
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

	void Render(const LogicalOperator &op, std::ostream &ss);
	void Render(const PhysicalOperator &op, std::ostream &ss);
	void Render(const ProfilingNode &op, std::ostream &ss);
	void Render(const Pipeline &op, std::ostream &ss);

	void ToStream(RenderTree &root, std::ostream &ss) override;
};

} // namespace duckdb
