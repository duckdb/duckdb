
#pragma once

#include "common/common.hpp"
#include "common/serializer.hpp"
#include "parser/expression.hpp"

namespace duckdb {

enum QueryNodeType : uint8_t { SELECT_NODE = 1, SET_OPERATION_NODE = 2 };

//! Single node in ORDER BY statement
struct OrderByNode {
	//! Sort order, ASC or DESC
	OrderType type;
	//! Expression to order by
	std::unique_ptr<Expression> expression;

	OrderByNode() {
	}
	OrderByNode(OrderType type, std::unique_ptr<Expression> expression)
	    : type(type), expression(std::move(expression)) {
	}
};

//! ORDER BY description
struct OrderByDescription {
	//! List of order nodes
	std::vector<OrderByNode> orders;
};

//! LIMIT description
struct LimitDescription {
	//! LIMIT count
	int64_t limit = -1;
	//! OFFSET
	int64_t offset = -1;
};

class QueryNode {
public:
	QueryNode(QueryNodeType type) : type(type) {
	}
	virtual ~QueryNode() {
	}

	virtual void Accept(SQLNodeVisitor *) = 0;
    
	virtual bool Equals(const QueryNode *other) {
		if (!other) {
			return false;
		}
		return other->type == type;
	}

	//! Create a copy of this QueryNode
	virtual std::unique_ptr<QueryNode> Copy() = 0;
	//! Serializes a QueryNode to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a QueryNode, returns nullptr if
	//! deserialization is not possible
	static std::unique_ptr<QueryNode> Deserialize(Deserializer &source);

    virtual vector<unique_ptr<Expression>>& GetSelectList() = 0;

	//! The type of the query node, either SetOperation or Select
	QueryNodeType type;
	//! DISTINCT or not
	bool select_distinct = false;
	//! Order By Description
	OrderByDescription orderby;
	//! Limit Description
	LimitDescription limit;

	//! Whether or not the query has a LIMIT clause
	bool HasLimit() {
		return limit.limit >= 0 || limit.offset >= 0;
	}
	//! Whether or not the query has an ORDER BY clause
	bool HasOrder() {
		return orderby.orders.size() > 0;
	}
};

};