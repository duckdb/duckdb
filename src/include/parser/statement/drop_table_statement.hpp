//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/drop_table_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropTableStatement : public SQLStatement {
public:
	DropTableStatement() : SQLStatement(StatementType::DROP_TABLE), info(make_unique<DropTableInformation>()){};
	virtual ~DropTableStatement() {
	}

	virtual string ToString() const {
		return "DROP TABLE";
	}
	virtual unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) const {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<DropTableInformation> info;
};

} // namespace duckdb
