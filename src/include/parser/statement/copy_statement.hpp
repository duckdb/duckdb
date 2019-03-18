//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"

#include <vector>

namespace duckdb {

class CopyStatement : public SQLStatement {
public:
	CopyStatement() : SQLStatement(StatementType::COPY), info(make_unique<CopyInformation>()){};

	string ToString() const override;

	bool Equals(const SQLStatement *other_) const override {
		if (!SQLStatement::Equals(other_)) {
			return false;
		}
		throw NotImplementedException("Equality not implemented!");
	}

	unique_ptr<CopyInformation> info;
	// The SQL statement used instead of a table when copying data out to a file
	unique_ptr<QueryNode> select_statement;
};
} // namespace duckdb
