//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/drop_index_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class DropIndexStatement : public SQLStatement {
public:
	DropIndexStatement() : SQLStatement(StatementType::DROP_INDEX), info(make_unique<DropIndexInformation>()){};
	virtual ~DropIndexStatement() {
	}

	virtual string ToString() const {
		return "DROP Index";
	}
	virtual unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}
	unique_ptr<DropIndexInformation> info;
};

} // namespace duckdb
