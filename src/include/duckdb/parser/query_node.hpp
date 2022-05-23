//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"

namespace duckdb {

enum QueryNodeType : uint8_t {
	SELECT_NODE = 1,
	SET_OPERATION_NODE = 2,
	BOUND_SUBQUERY_NODE = 3,
	RECURSIVE_CTE_NODE = 4
};

class QueryNode {
public:
	explicit QueryNode(QueryNodeType type) : type(type) {
	}
	virtual ~QueryNode() {
	}

	//! The type of the query node, either SetOperation or Select
	QueryNodeType type;
	//! The set of result modifiers associated with this query node
	vector<unique_ptr<ResultModifier>> modifiers;
	//! CTEs (used by SelectNode and SetOperationNode)
	unordered_map<string, unique_ptr<CommonTableExpressionInfo>> cte_map;

	virtual const vector<unique_ptr<ParsedExpression>> &GetSelectList() const = 0;

public:
	//! Convert the query node to a string
	virtual string ToString() const = 0;

	virtual bool Equals(const QueryNode *other) const;

	//! Create a copy of this QueryNode
	virtual unique_ptr<QueryNode> Copy() const = 0;
	//! Serializes a QueryNode to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer) const;
	//! Serializes a QueryNode to a stand-alone binary blob
	DUCKDB_API virtual void Serialize(FieldWriter &writer) const = 0;
	//! Deserializes a blob back into a QueryNode
	DUCKDB_API static unique_ptr<QueryNode> Deserialize(Deserializer &source);

	string CTEToString() const;
	string ResultModifiersToString() const;

protected:
	//! Copy base QueryNode properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(QueryNode &other) const;
};

} // namespace duckdb
