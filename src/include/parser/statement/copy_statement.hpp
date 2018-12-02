//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/statement/copy_statement.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "parser/sql_statement.hpp"

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class CopyStatement : public SQLStatement {
  public:
	CopyStatement() : SQLStatement(StatementType::COPY){};
	virtual ~CopyStatement() {
	}
	virtual std::string ToString() const;
	virtual std::unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other_) {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	std::string table;
	std::string schema;

	// The SQL statement used instead of a table when copying data out to a file
	std::unique_ptr<SQLStatement> select_statement;

	std::string file_path;

	// List of Columns that will be copied from/to.
	std::vector<std::string> select_list;

	// File Format
	ExternalFileFormat format = ExternalFileFormat::CSV;

	// Copy: From CSV (True) To CSV (False)
	bool is_from;

	char delimiter = ',';
	char quote = '"';
	char escape = '"';
};
} // namespace duckdb
