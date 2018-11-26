//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/tableref/subqueryref.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_node_visitor.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/tableref.hpp"
#include "planner/bindcontext.hpp"

namespace duckdb {
//! Represents a subquery
class SubqueryRef : public TableRef {
  public:
	SubqueryRef(std::unique_ptr<SelectStatement> subquery);

	std::unique_ptr<TableRef> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	bool Equals(const TableRef *other_) override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (SubqueryRef *)other_;
		return subquery->Equals(other->subquery.get());
	}

	std::unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a SubqueryRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SubqueryRef
	static std::unique_ptr<TableRef> Deserialize(Deserializer &source);

	//! The subquery
	std::unique_ptr<SelectStatement> subquery;
	//! Alises for the column names
	std::vector<std::string> column_name_alias;
	// Bindcontext, FIXME
	std::unique_ptr<BindContext> context;
};
} // namespace duckdb
