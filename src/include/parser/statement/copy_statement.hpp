//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

class CopyStatement : public SQLStatement {
public:
	CopyStatement() : SQLStatement(StatementType::COPY){};

	string ToString() const override;
	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	string table;
	string schema;

	// The SQL statement used instead of a table when copying data out to a file
	unique_ptr<QueryNode> select_statement;

	string file_path;

	// List of Columns that will be copied from/to.
	vector<string> select_list;

	// File Format
	ExternalFileFormat format = ExternalFileFormat::CSV;

	// Copy: From CSV (True) To CSV (False)
	bool is_from;

	char delimiter = ',';
	char quote = '"';
	char escape = '"';
};
} // namespace duckdb
