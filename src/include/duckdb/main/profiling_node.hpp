//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiling_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/profiling_info.hpp"

namespace duckdb {

//! Recursive tree mirroring the operator tree.
class ProfilingNode {
public:
	explicit ProfilingNode() {
	}
	virtual ~ProfilingNode() {};

private:
	ProfilingInfo profiling_info;

public:
	idx_t depth = 0;
	vector<unique_ptr<ProfilingNode>> children;

public:
	idx_t GetChildCount() {
		return children.size();
	}
	ProfilingInfo &GetProfilingInfo() {
		return profiling_info;
	}
	const ProfilingInfo &GetProfilingInfo() const {
		return profiling_info;
	}
	optional_ptr<ProfilingNode> GetChild(idx_t idx) {
		return children[idx].get();
	}
	optional_ptr<ProfilingNode> AddChild(unique_ptr<ProfilingNode> child) {
		children.push_back(std::move(child));
		return children.back().get();
	}
};

} // namespace duckdb
