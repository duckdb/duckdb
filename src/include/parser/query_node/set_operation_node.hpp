//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/query_node/set_operation_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"
#include "planner/bindcontext.hpp"

namespace duckdb {

class SetOperationNode : public QueryNode {
public:
	SetOperationNode() : QueryNode(QueryNodeType::SET_OPERATION_NODE) {
	}

	//! The type of set operation
	SetOperationType setop_type = SetOperationType::NONE;
	//! The left side of the set operation
	unique_ptr<QueryNode> left = nullptr;
	//! The right side of the set operation
	unique_ptr<QueryNode> right = nullptr;

	//! The following information is only gathered after binding the SetOperationNode
	struct {
		//! The bind context used by the left side of the set operation
		unique_ptr<BindContext> left_context;
		//! The bind context used by the right side of the set operation
		unique_ptr<BindContext> right_context;
	} binding;

	vector<unique_ptr<Expression>> &GetSelectList() override {
		return left->GetSelectList();
	}

	size_t GetSelectCount() override {
		return left->GetSelectCount();
	}

	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() override;
	//! Serializes a SelectNode to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SelectNode
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);
};

}; // namespace duckdb
