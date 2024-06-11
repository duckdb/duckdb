//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiling_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/profiling_info.hpp"

namespace duckdb {

class QueryProfilingNode;

// Recursive tree that mirrors the operator tree
class ProfilingNode {
public:
	ProfilingInfo profiling_info;
	vector<unique_ptr<ProfilingNode>> children;
	idx_t depth = 0;
	bool is_query = false;

public:
	idx_t GetChildCount() {
		return children.size();
	}

	// TODO: Can this be simplified?
	template <class TARGET>
	TARGET &Cast() {
		if (std::is_same<TARGET, QueryProfilingNode>::value) {
			if (!is_query) {
				throw InternalException("Failed to cast ProfilingNode to QueryProfilingNode - node type mismatch");
			}
			return reinterpret_cast<TARGET &>(*this);
		}

		if (is_query) {
			throw InternalException("Failed to cast ProfilingNode to OperatorProfilingNode - node type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (std::is_same<TARGET, QueryProfilingNode>::value) {
			if (!is_query) {
				throw InternalException("Failed to cast ProfilingNode to QueryProfilingNode - node type mismatch");
			}
			return reinterpret_cast<const TARGET &>(*this);
		}

		if (is_query) {
			throw InternalException("Failed to cast ProfilingNode to OperatorProfilingNode - node type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

// Holds the top level query info
class QueryProfilingNode : public ProfilingNode {
public:
	string query;
};

class OperatorProfilingNode : public ProfilingNode {
public:
	PhysicalOperatorType type;
	string name;
};

} // namespace duckdb

