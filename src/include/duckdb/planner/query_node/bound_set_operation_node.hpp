//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/query_node/bound_set_operation_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {
struct BoundSetOpChild;

//! Bound equivalent of SetOperationNode
class BoundSetOperationNode : public BoundQueryNode {
public:
	//! The type of set operation
	SetOperationType setop_type = SetOperationType::NONE;
	//! whether the ALL modifier was used or not
	bool setop_all = false;
	//! The bound children
	vector<BoundSetOpChild> bound_children;

	//! Index used by the set operation
	idx_t setop_index;

public:
	idx_t GetRootIndex() override {
		return setop_index;
	}
};

struct BoundSetOpChild {
	unique_ptr<BoundSetOperationNode> bound_node;
	BoundStatement node;
	shared_ptr<Binder> binder;
	//! Original select list (if this was a SELECT statement)
	vector<unique_ptr<ParsedExpression>> select_list;
	//! Exprs used by the UNION BY NAME operations to add a new projection
	vector<unique_ptr<Expression>> reorder_expressions;

	const vector<string> &GetNames();
	const vector<LogicalType> &GetTypes();
	idx_t GetRootIndex();
};

} // namespace duckdb
