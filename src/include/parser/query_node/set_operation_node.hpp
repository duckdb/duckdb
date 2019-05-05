//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/query_node/set_operation_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/set_operation_type.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class SetOperationNode : public QueryNode {
public:
	SetOperationNode() : QueryNode(QueryNodeType::SET_OPERATION_NODE) {
	}

	//! The type of set operation
	SetOperationType setop_type = SetOperationType::NONE;
	//! The left side of the set operation
	unique_ptr<QueryNode> left;
	//! The right side of the set operation
	unique_ptr<QueryNode> right;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		return left->GetSelectList();
	}

public:
	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() override;

	//! Serializes a SelectNode to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SelectNode
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);
};

}; // namespace duckdb
