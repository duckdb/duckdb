//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/column_binding_layout.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

struct ColumnBindingLayout {
	ColumnBindingLayout() : error_context("output layout") {
	}

	explicit ColumnBindingLayout(vector<ColumnBinding> bindings_p, string error_context_p = "output layout")
	    : bindings(std::move(bindings_p)), error_context(std::move(error_context_p)) {
		for (idx_t i = 0; i < bindings.size(); i++) {
			positions[bindings[i]] = i;
		}
	}

	idx_t GetPosition(const ColumnBinding &binding) const {
		auto entry = positions.find(binding);
		if (entry == positions.end()) {
			throw InternalException("Could not find binding %s in %s", binding.ToString(), error_context);
		}
		return entry->second;
	}

	ProjectionIndex GetProjectionIndex(const ColumnBinding &binding) const {
		return ProjectionIndex(GetPosition(binding));
	}

	vector<ProjectionIndex> CreateProjectionMap(const vector<ColumnBinding> &projected_bindings) const {
		vector<ProjectionIndex> result;
		result.reserve(projected_bindings.size());
		for (auto &binding : projected_bindings) {
			result.push_back(GetProjectionIndex(binding));
		}
		return result;
	}

	bool HasSameBindings(const vector<ColumnBinding> &other) const {
		if (bindings.size() != other.size()) {
			return false;
		}
		column_binding_set_t seen;
		for (auto &binding : other) {
			if (!seen.insert(binding).second || positions.find(binding) == positions.end()) {
				return false;
			}
		}
		return true;
	}

	vector<ColumnBinding> ProjectBindings(const vector<ProjectionIndex> &projection_map) const {
		if (projection_map.empty()) {
			return bindings;
		}
		vector<ColumnBinding> result;
		result.reserve(projection_map.size());
		for (auto &projection_index : projection_map) {
			if (!projection_index.IsValid() || projection_index.GetIndex() >= bindings.size()) {
				throw InternalException("Invalid projection index in %s", error_context);
			}
			result.push_back(bindings[projection_index.GetIndex()]);
		}
		return result;
	}

	vector<ColumnBinding> bindings;
	column_binding_map_t<idx_t> positions;
	string error_context;
};

} // namespace duckdb
