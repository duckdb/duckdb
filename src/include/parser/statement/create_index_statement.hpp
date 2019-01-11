//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_index_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref/basetableref.hpp"

namespace duckdb {

class CreateIndexStatement : public SQLStatement {
public:
	CreateIndexStatement() : SQLStatement(StatementType::CREATE_INDEX), info(make_unique<CreateIndexInformation>()){};

	string ToString() const override {
		return "CREATE INDEX";
	}
	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}

	//! The table to create the index on
	unique_ptr<BaseTableRef> table;
	//! Set of expressions to index by
	vector<unique_ptr<Expression>> expressions;
	// Info for index creation
	unique_ptr<CreateIndexInformation> info;
};

} // namespace duckdb
//
