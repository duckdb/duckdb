//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/constraints/parsed_constraint.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"
#include "parser/sql_node_visitor.hpp"

#include <vector>

namespace duckdb {

//! The ParsedConstraint represents either a UNIQUE or PRIMARY KEY constraint
//! that is not fully resolved yet. When added to a table in the catalog, it
//! will be transformed to a proper PRIMARY KEY or UNIQUE constraint
class ParsedConstraint : public Constraint {
  public:
	ParsedConstraint(ConstraintType type, size_t index)
	    : Constraint(ConstraintType::DUMMY), ctype(type), index(index) {
	}
	ParsedConstraint(ConstraintType type, std::vector<std::string> columns)
	    : Constraint(ConstraintType::DUMMY), ctype(type), index((size_t)-1),
	      columns(columns) {
	}
	virtual ~ParsedConstraint() {
	}

	virtual std::unique_ptr<Constraint> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual std::string ToString() const {
		return "Dummy Constraint";
	}

	//! Serialize to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a ParsedConstraint
	static std::unique_ptr<Constraint> Deserialize(Deserializer &source);

	ConstraintType ctype;
	size_t index;
	std::vector<std::string> columns;
};

} // namespace duckdb
