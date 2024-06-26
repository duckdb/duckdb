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
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/common/render_tree.hpp"

namespace duckdb {
class LogicalOperator;
class PhysicalOperator;
class Pipeline;
struct PipelineRenderNode;

struct JSONRendererConfig {};

class JSONRenderer {
public:
	explicit JSONRenderer(JSONRendererConfig config_p = JSONRendererConfig()) : config(config_p) {
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

private:
	//! The configuration used for rendering
	JSONRendererConfig config;

private:
	// TODO: private methods go here
};

} // namespace duckdb
