//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/select_bind_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/group_by_node.hpp"

namespace duckdb {

//! Bind state during a SelectNode
struct SelectBindState {
	// Mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	case_insensitive_map_t<idx_t> alias_map;
	parsed_expression_map_t<idx_t> projection_map;
	//! The original unparsed expressions. This is exported after binding, because the binding might change the
	//! expressions (e.g. when a * clause is present)
	vector<unique_ptr<ParsedExpression>> original_expressions;
};

} // namespace duckdb
