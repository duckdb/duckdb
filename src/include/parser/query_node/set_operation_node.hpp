
#pragma once

#include "parser/query_node.hpp"
#include "parser/expression.hpp"
#include "parser/sql_statement.hpp"
#include "planner/bindcontext.hpp"
#include "parser/sql_node_visitor.hpp"


namespace duckdb {

class SetOperationNode : public QueryNode {
public:
	SetOperationNode() : QueryNode(QueryNodeType::SET_OPERATION_NODE) {
	}
	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}
    
	//! The type of set operation
	SetOperationType setop_type = SetOperationType::NONE;
	//! The left side of the set operation
	std::unique_ptr<QueryNode> left = nullptr;
	//! The right side of the set operation
	std::unique_ptr<QueryNode> right = nullptr;

	//! The bind context used by the left side of the set operation
	std::unique_ptr<BindContext> setop_left_binder;
	//! The bind context used by the right side of the set operation
	std::unique_ptr<BindContext> setop_right_binder;

    vector<unique_ptr<Expression>>& GetSelectList() override { return left->GetSelectList(); }

	bool Equals(const QueryNode *other) override;
	//! Create a copy of this SelectNode
	std::unique_ptr<QueryNode> Copy() override;
	//! Serializes a SelectNode to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SelectNode
	static std::unique_ptr<QueryNode> Deserialize(Deserializer &source);
};

};