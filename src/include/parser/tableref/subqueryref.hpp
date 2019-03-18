//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/tableref/subqueryref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query_node.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/tableref.hpp"

namespace duckdb {
//! Represents a subquery
class SubqueryRef : public TableRef {
public:
	SubqueryRef(unique_ptr<QueryNode> subquery);

	unique_ptr<TableRef> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	bool Equals(const TableRef *other_) const override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (SubqueryRef *)other_;
		return subquery->Equals(other->subquery.get());
	}

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a SubqueryRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SubqueryRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! The subquery
	unique_ptr<QueryNode> subquery;
	//! Alises for the column names
	vector<string> column_name_alias;
};
} // namespace duckdb
